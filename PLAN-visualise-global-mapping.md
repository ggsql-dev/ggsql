# Plan: VISUALISE Global Mapping Syntax

## Overview

Change the VISUALISE clause from a type declaration to a functional mapping declaration:

**Current syntax:**
```sql
SELECT * FROM sales
VISUALISE AS PLOT
DRAW line MAPPING date AS x, revenue AS y, region AS color
DRAW point MAPPING date AS x, revenue AS y, region AS color
```

**New syntax:**
```sql
SELECT * FROM sales
VISUALISE date AS x, revenue AS y, region AS color
DRAW line
DRAW point
```

**With FROM (CTE shorthand):**
```sql
VISUALISE date AS x, revenue AS y FROM sales
DRAW line
DRAW point
```

**With implicit mapping:**
```sql
-- Explicit: column AS aesthetic
VISUALISE date AS x, revenue AS y

-- Implicit: column name becomes aesthetic name
VISUALISE x, y
-- Equivalent to: VISUALISE x AS x, y AS y

-- Wildcard: all columns become implicit mappings
VISUALISE *
-- Equivalent to: VISUALISE col1 AS col1, col2 AS col2, ... for all columns
```

## Key Changes

1. Remove `AS PLOT` and `VizType` entirely
2. Global mapping in VISUALISE sets defaults for all layers
3. Layer MAPPING clauses add to or override global mappings
4. FROM clause moves to end of VISUALISE (after mappings)
5. **Implicit mapping**: `VISUALISE x, y` → `VISUALISE x AS x, y AS y`
6. **Wildcard mapping**: `VISUALISE *` → map all columns implicitly
7. **American spelling**: Support both `VISUALISE` and `VISUALIZE` (case-insensitive)

## Syntax Examples

### Basic Usage
```sql
-- Global mappings inherited by all layers
VISUALISE date AS x, revenue AS y
DRAW line
DRAW point SETTING size TO 5

-- Layer overrides global mapping
VISUALISE date AS x, revenue AS y
DRAW line
DRAW point MAPPING 'red' AS color  -- adds color, keeps x and y from global
```

### Implicit Mapping
```sql
-- Implicit: column name = aesthetic name
VISUALISE x, y
DRAW point
-- Equivalent to: VISUALISE x AS x, y AS y

-- Mixed implicit and explicit
VISUALISE x, y, region AS color
DRAW line
-- Equivalent to: VISUALISE x AS x, y AS y, region AS color

-- Wildcard: map all columns
SELECT date, revenue, region FROM sales
VISUALISE *
DRAW point
-- Creates: date AS date, revenue AS revenue, region AS region
```

### With FROM (CTE Shorthand)
```sql
-- Direct table
VISUALISE date AS x, revenue AS y FROM sales
DRAW line

-- With CTE
WITH monthly AS (
    SELECT month, SUM(revenue) as total FROM sales GROUP BY month
)
VISUALISE month AS x, total AS y FROM monthly
DRAW bar

-- Implicit mapping with FROM
VISUALISE x, y FROM points_table
DRAW point
```

### Layer Override Behavior
```sql
VISUALISE date AS x, revenue AS y, region AS color
DRAW line                                    -- uses: x=date, y=revenue, color=region
DRAW point MAPPING profit AS y               -- uses: x=date, y=profit, color=region (y overridden)
DRAW text MAPPING label AS label, 0.5 AS alpha  -- uses: x=date, y=revenue, color=region, label=label, alpha=0.5
```

### Empty Global Mapping
```sql
-- Empty VISUALISE - each layer must define its own mappings
VISUALISE
DRAW bar MAPPING category AS x, count AS y
DRAW line MAPPING date AS x, revenue AS y

-- Empty with FROM
VISUALISE FROM sales
DRAW bar MAPPING category AS x, count AS y
```

### American Spelling
```sql
-- VISUALIZE works identically to VISUALISE
VISUALIZE date AS x, revenue AS y FROM sales
DRAW line

-- Case-insensitive
visualize x, y
DRAW point
```

---

## Implementation Plan

### Phase 1: Grammar Changes (`tree-sitter-ggsql/grammar.js`)

**Current grammar:**
```javascript
visualise_statement: $ => seq(
  caseInsensitive('VISUALISE'),
  optional(seq(caseInsensitive('FROM'), $.identifier)),
  caseInsensitive('AS'),
  $.viz_type,
  repeat($.viz_clause)
)
```

**New grammar:**
```javascript
visualise_statement: $ => seq(
  visualiseKeyword(),                   // Accepts VISUALISE or VISUALIZE (case-insensitive)
  optional($.global_mapping),           // Optional global aesthetics
  optional(seq(caseInsensitive('FROM'), $.source_reference)),  // Optional FROM
  repeat($.viz_clause)
)

// Helper for British/American spelling
function visualiseKeyword() {
  return choice(caseInsensitive('VISUALISE'), caseInsensitive('VISUALIZE'));
}

// Global mapping supports: explicit, implicit, wildcard, or mixed
global_mapping: $ => choice(
  $.wildcard_mapping,                   // VISUALISE *
  seq(
    $.global_mapping_item,
    repeat(seq(',', $.global_mapping_item))
  )
)

wildcard_mapping: $ => '*',

// Supports both explicit (col AS aes) and implicit (col)
global_mapping_item: $ => choice(
  seq($.mapping_value, caseInsensitive('AS'), $.aesthetic_name),  // explicit: date AS x
  $.identifier                                                      // implicit: x (becomes x AS x)
)

// Reuse existing mapping_item from draw_clause (explicit only)
mapping_item: $ => seq(
  $.mapping_value,
  caseInsensitive('AS'),
  $.aesthetic_name
)
```

**Files to modify:**
- `tree-sitter-ggsql/grammar.js`

**Tasks:**
1. Remove `AS` and `viz_type` from visualise_statement
2. Add `global_mapping` rule with wildcard support
3. Add `global_mapping_item` supporting both explicit and implicit forms
4. Move FROM clause position (after mappings, before clauses)
5. Regenerate parser: `cd tree-sitter-ggsql && npx tree-sitter generate`

---

### Phase 2: AST Changes (`src/parser/ast.rs`)

**Current VizSpec:**
```rust
pub struct VizSpec {
    pub viz_type: VizType,
    pub source: Option<String>,
    pub layers: Vec<Layer>,
    // ...
}
```

**New VizSpec:**
```rust
pub struct VizSpec {
    pub source: Option<String>,
    pub global_mapping: GlobalMapping,  // NEW - replaces viz_type
    pub layers: Vec<Layer>,
    // ...
}

// Represents the global mapping specification
pub enum GlobalMapping {
    /// No global mapping specified - layers must define all aesthetics
    Empty,
    /// Wildcard (*) - resolve all columns at execution time
    Wildcard,
    /// Explicit list of mappings (may include implicit entries)
    Mappings(Vec<GlobalMappingItem>),
}

// Individual mapping item in global mapping
pub enum GlobalMappingItem {
    /// Explicit mapping: `date AS x` → column "date" maps to aesthetic "x"
    Explicit { column: String, aesthetic: String },
    /// Implicit mapping: `x` → column "x" maps to aesthetic "x"
    Implicit { name: String },
}
```

**Tasks:**
1. Remove `VizType` enum entirely
2. Add `GlobalMapping` enum with Empty, Wildcard, and Mappings variants
3. Add `GlobalMappingItem` enum for explicit vs implicit items
4. Add method `VizSpec::resolve_global_aesthetics(&self, df: &DataFrame) -> HashMap<String, AestheticValue>`
   - For `Empty`: returns empty HashMap
   - For `Wildcard`: returns all DataFrame columns as implicit mappings
   - For `Mappings`: resolves implicit items to explicit, returns HashMap
5. Add method `VizSpec::resolve_layer_aesthetics(&self, layer: &Layer, df: &DataFrame) -> HashMap<String, AestheticValue>`
6. Update `VizSpec::new()` constructor
7. Remove all `VizType` references throughout codebase

---

### Phase 3: Builder Changes (`src/parser/builder.rs`)

**Tasks:**
1. Update `build_visualise_statement()` to parse global mappings
2. Remove all viz_type parsing
3. Handle wildcard, implicit, and explicit mapping items
4. Handle FROM clause in new position
5. Store GlobalMapping in VizSpec

**Key function changes:**

```rust
fn build_visualise_statement(node: Node, source: &str) -> Result<VizSpec> {
    let mut spec = VizSpec::new();

    for child in node.children(&mut node.walk()) {
        match child.kind() {
            "global_mapping" => {
                spec.global_mapping = parse_global_mapping(child, source)?;
            }
            "wildcard_mapping" => {
                spec.global_mapping = GlobalMapping::Wildcard;
            }
            "source_reference" => {
                spec.source = Some(get_node_text(child, source));
            }
            // ... handle clauses
        }
    }

    Ok(spec)
}

fn parse_global_mapping(node: Node, source: &str) -> Result<GlobalMapping> {
    let mut items = Vec::new();

    for child in node.children(&mut node.walk()) {
        match child.kind() {
            "global_mapping_item" => {
                items.push(parse_global_mapping_item(child, source)?);
            }
            _ => continue,
        }
    }

    if items.is_empty() {
        Ok(GlobalMapping::Empty)
    } else {
        Ok(GlobalMapping::Mappings(items))
    }
}

fn parse_global_mapping_item(node: Node, source: &str) -> Result<GlobalMappingItem> {
    let children: Vec<_> = node.children(&mut node.walk()).collect();

    // Check if this is explicit (has AS) or implicit (single identifier)
    if children.iter().any(|c| c.kind().eq_ignore_ascii_case("AS")) {
        // Explicit: value AS aesthetic
        let column = get_mapping_value(children[0], source)?;
        let aesthetic = get_node_text(children.last().unwrap(), source);
        Ok(GlobalMappingItem::Explicit { column, aesthetic })
    } else {
        // Implicit: just identifier
        let name = get_node_text(&children[0], source);
        Ok(GlobalMappingItem::Implicit { name })
    }
}
```

---

### Phase 4: Splitter Changes (`src/parser/splitter.rs`)

**Tasks:**
1. Update regex/logic to find VISUALISE without `AS PLOT`
2. Handle `VISUALISE ... FROM <source>` for SELECT injection
3. Ensure byte offset splitting still works correctly

**Key changes:**
- Pattern matching needs to handle `VISUALISE <mapping> FROM` vs `VISUALISE <mapping>` without FROM
- FROM source extraction for CTE shorthand

---

### Phase 5: Writer Changes (`src/writer/vegalite.rs`)

**Tasks:**
1. Resolve global mapping to concrete aesthetics (handles wildcard + implicit)
2. Merge global + layer aesthetics
3. Global aesthetics as base, layer aesthetics override

```rust
fn build_layer_encoding(
    &self,
    layer: &Layer,
    spec: &VizSpec,
    df: &DataFrame
) -> Result<Value> {
    // Resolve global mapping (handles Wildcard, Implicit, Explicit)
    let global_aesthetics = self.resolve_global_mapping(&spec.global_mapping, df)?;

    // Start with resolved global aesthetics
    let mut aesthetics = global_aesthetics;

    // Layer aesthetics override globals
    for (key, value) in &layer.aesthetics {
        aesthetics.insert(key.clone(), value.clone());
    }

    // Build encoding from merged aesthetics
    self.build_encoding_from_aesthetics(&aesthetics, df, spec)
}

/// Resolve GlobalMapping to concrete aesthetic mappings
fn resolve_global_mapping(
    &self,
    mapping: &GlobalMapping,
    df: &DataFrame
) -> Result<HashMap<String, AestheticValue>> {
    match mapping {
        GlobalMapping::Empty => Ok(HashMap::new()),

        GlobalMapping::Wildcard => {
            // Map all DataFrame columns to aesthetics with same name
            let mut aesthetics = HashMap::new();
            for col_name in df.get_column_names() {
                aesthetics.insert(
                    col_name.to_string(),
                    AestheticValue::Column(col_name.to_string())
                );
            }
            Ok(aesthetics)
        }

        GlobalMapping::Mappings(items) => {
            let mut aesthetics = HashMap::new();
            for item in items {
                match item {
                    GlobalMappingItem::Explicit { column, aesthetic } => {
                        aesthetics.insert(
                            aesthetic.clone(),
                            AestheticValue::Column(column.clone())
                        );
                    }
                    GlobalMappingItem::Implicit { name } => {
                        // Implicit: name is both column and aesthetic
                        aesthetics.insert(
                            name.clone(),
                            AestheticValue::Column(name.clone())
                        );
                    }
                }
            }
            Ok(aesthetics)
        }
    }
}
```

**Note on Wildcard Resolution:**
- Wildcard (`*`) resolution happens at write time, not parse time
- This is because the DataFrame schema is only known after SQL execution
- The writer has access to `df.get_column_names()` to enumerate columns

---

### Phase 6: Update Tests

**Files to update:**
- `tree-sitter-ggsql/test/corpus/basic.txt` - Grammar tests
- `src/parser/builder.rs` - Builder unit tests
- `src/parser/splitter.rs` - Splitter tests
- `src/writer/vegalite.rs` - Writer tests
- `src/lib.rs` - Integration tests
- `ggsql-jupyter/tests/test_compliance.py`
- `ggsql-jupyter/tests/test_integration.py`

**New test cases needed:**
1. Global mapping only (no layer MAPPING)
2. Global mapping + layer override
3. Global mapping + layer additions
4. Empty global mapping with FROM
5. Complex: multiple layers with different overrides

---

### Phase 7: Update Documentation

**Files to update:**
- `CLAUDE.md` - Architecture docs
- `README.md` - User-facing docs
- `EXAMPLES.md` - Example queries
- `ggsql-jupyter/README.md`
- `ggsql-vscode/README.md`
- `ggsql-jupyter/tests/quarto/doc.qmd`
- `ggsql-jupyter/tests/fixtures/sample_notebook.ipynb`
- `ggsql-vscode/examples/sample.gsql`

---

## Migration Guide

### Breaking Changes

| Old Syntax | New Syntax |
|------------|------------|
| `VISUALISE AS PLOT` | `VISUALISE` |
| `VISUALISE FROM sales AS PLOT` | `VISUALISE FROM sales` |
| `VISUALISE AS PLOT DRAW line MAPPING date AS x, revenue AS y` | `VISUALISE date AS x, revenue AS y DRAW line` |
| `VISUALISE AS PLOT DRAW point MAPPING x AS x, y AS y` | `VISUALISE x, y DRAW point` (implicit) |
| Multiple layers with same mappings | Use global mapping, layers inherit |

### Common Migration Patterns

```sql
-- OLD: Repeated mappings across layers
VISUALISE AS PLOT
DRAW line MAPPING date AS x, revenue AS y, region AS color
DRAW point MAPPING date AS x, revenue AS y, region AS color

-- NEW: Global mapping, layers inherit
VISUALISE date AS x, revenue AS y, region AS color
DRAW line
DRAW point
```

```sql
-- OLD: Simple scatter plot
VISUALISE AS PLOT
DRAW point MAPPING x AS x, y AS y

-- NEW: Implicit mapping
VISUALISE x, y
DRAW point
```

```sql
-- OLD: With FROM shorthand
VISUALISE FROM sales AS PLOT
DRAW bar MAPPING category AS x, total AS y

-- NEW: With FROM shorthand
VISUALISE category AS x, total AS y FROM sales
DRAW bar
```

### Backward Compatibility

**Clean break (recommended)**
- Remove old syntax entirely
- Update all examples and tests
- Simpler grammar and codebase

---

## File Change Summary

| File | Type | Changes |
|------|------|---------|
| `tree-sitter-ggsql/grammar.js` | Modify | New visualise_statement structure |
| `src/parser/ast.rs` | Modify | Add global_aesthetics field |
| `src/parser/builder.rs` | Modify | Parse global mappings, extract reusable mapping parser |
| `src/parser/splitter.rs` | Modify | Update VISUALISE detection |
| `src/writer/vegalite.rs` | Modify | Merge global + layer aesthetics |
| `src/writer/mod.rs` | Possibly | Update Writer trait if needed |
| `tree-sitter-ggsql/test/corpus/basic.txt` | Modify | Update all test cases |
| `src/lib.rs` | Modify | Update integration tests |
| Multiple test files | Modify | Update query syntax |
| Multiple doc files | Modify | Update examples |

---

## Phased Rollout

| Phase | Scope | Estimated Complexity |
|-------|-------|---------------------|
| 1 | Grammar changes | Medium |
| 2 | AST changes | Low |
| 3 | Builder changes | Medium |
| 4 | Splitter changes | Medium |
| 5 | Writer changes | Low |
| 6 | Tests | Medium (many files) |
| 7 | Documentation | Low (tedious) |

**Recommended order:** 1 → 2 → 3 → 4 → 5 → 6 → 7

---

## Resolved Design Decisions

1. **Empty VISUALISE**: ✅ Yes, `VISUALISE` and `VISUALISE FROM sales` are valid
   - Layers must provide all mappings when global mapping is empty

2. **VizType**: ✅ Remove entirely
   - Can re-add later if MAP/TABLE support needed

3. **MAPPING keyword in VISUALISE**: ✅ No MAPPING keyword
   - `VISUALISE date AS x` not `VISUALISE MAPPING date AS x`
   - VISUALISE implies mapping

4. **Implicit mapping**: ✅ Supported
   - `VISUALISE x, y` → `VISUALISE x AS x, y AS y`

5. **Wildcard mapping**: ✅ Supported
   - `VISUALISE *` maps all columns implicitly
   - Resolution happens at write time (needs DataFrame schema)

6. **Multiple VISUALISE statements**: Keep support
   - Each VISUALISE starts a new spec

7. **American spelling**: ✅ Supported
   - Both `VISUALISE` and `VISUALIZE` accepted (case-insensitive)
   - Consistent with existing ggSQL behavior
