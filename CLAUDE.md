# vvSQL System Architecture & Implementation Summary

## Overview

**vvSQL** is a SQL extension for declarative data visualization based on Grammar of Graphics principles. It allows users to combine SQL data queries with visualization specifications in a single, composable syntax.

**Core Innovation**: vvSQL extends standard SQL with a `VISUALISE AS` clause that separates data retrieval (SQL) from visual encoding (Grammar of Graphics), enabling terminal visualization operations that produce charts instead of relational data.

```sql
SELECT date, revenue, region FROM sales WHERE year = 2024
VISUALISE AS PLOT
WITH line USING x = date, y = revenue, color = region
SCALE x USING type = 'date'
COORD cartesian USING ylim = [0, 100000]
LABEL title = 'Sales by Region', x = 'Date', y = 'Revenue'
THEME minimal
```

**Statistics**:

- ~6,500 lines of Rust code (+1,200 for COORD implementation)
- 293-line Tree-sitter grammar (simplified, no external scanner)
- ~820 lines of TypeScript/React code in test application
- 9 React components (4 main + 5 shadcn/ui components)
- Full bindings: Rust, C, Python, Node.js with tree-sitter integration
- Syntax highlighting support via Tree-sitter queries
- 148 total tests (60 for parser/builder, including 32 comprehensive edge cases)
- End-to-end working pipeline: SQL → Data → Visualization
- Coordinate transformations: Cartesian (xlim/ylim), Flip, Polar
- VISUALISE FROM shorthand syntax with automatic SELECT injection

---

## VISUALISE FROM Feature

vvSQL supports two patterns for creating visualizations:

### Traditional Pattern: SELECT ... VISUALISE AS

The original syntax where SQL and visualization are separated by `VISUALISE AS`:

```sql
SELECT * FROM sales WHERE year = 2024
VISUALISE AS PLOT
WITH line USING x = date, y = revenue
```

### Shorthand Pattern: VISUALISE FROM ... AS

A concise syntax that automatically injects `SELECT * FROM <source>`:

```sql
-- Direct table visualization
VISUALISE FROM sales AS PLOT
WITH bar USING x = region, y = total

-- CTE visualization (no trailing SELECT)
WITH monthly_totals AS (
    SELECT DATE_TRUNC('month', sale_date) as month, SUM(revenue) as total
    FROM sales
    GROUP BY month
)
VISUALISE FROM monthly_totals AS PLOT
WITH line USING x = month, y = total
```

**Behind the scenes**: The splitter automatically injects `SELECT * FROM <source>` before passing to the database.

### Validation Rules

The parser enforces that `VISUALISE FROM` cannot be combined with trailing SELECT statements:

**✅ Valid:**
- `VISUALISE FROM table AS PLOT` - Direct table (injected SELECT)
- `WITH cte AS (...) VISUALISE FROM cte AS PLOT` - CTE without trailing SELECT
- `SELECT ... VISUALISE AS PLOT` - Traditional pattern
- `WITH cte AS (...) SELECT * FROM cte VISUALISE AS PLOT` - CTE with explicit SELECT

**❌ Invalid (Parse Error):**
- `SELECT * FROM x VISUALISE FROM y AS PLOT` - Cannot mix SELECT with VISUALISE FROM
- `WITH cte AS (...) SELECT * FROM cte VISUALISE FROM cte AS PLOT` - Trailing SELECT conflicts

**Rationale**: Prevents ambiguity about which data source to visualize. The parser validates this by checking if the last SQL statement is a SELECT before allowing VISUALISE FROM.

### Implementation Details

**1. Grammar Changes** (`tree-sitter-vvsql/grammar.js`):
- Line 71-77: Updated `with_statement` to include `optional($.select_statement)`, allowing WITH to be followed by SELECT as a compound statement
- Lines 158-172: Made `subquery` rule fully recursive to support complex SQL (VALUES, nested subqueries)

**2. Query Splitter** (`src/parser/splitter.rs`):
- Lines 60-92: Checks for VISUALISE FROM and injects `SELECT * FROM <source>`
- Handles semicolons correctly: adds semicolon before injected SELECT if needed
- Special case for WITH statements: no semicolon needed between `WITH cte AS (...) SELECT * FROM cte`

**3. AST Builder Validation** (`src/parser/builder.rs`):
- Lines 43-50: Validates VISUALISE FROM usage after parsing
- Lines 1021-1065: Recursive validation to detect trailing SELECT in compound statements
- `with_statement_has_trailing_select()` helper distinguishes internal CTEs from trailing SELECT

---

## System Architecture

### High-Level Flow

```
┌─────────────────────────────────────────────────────────────┐
│                       vvSQL Query                            │
│  "SELECT ... FROM ... WHERE ... VISUALISE AS PLOT WITH ..."  │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
         ┌───────────────────────────────┐
         │      Query Splitter           │
         │  (Regex-based, tree-sitter)   │
         └───────────┬───────────────────┘
                     │
         ┌───────────┴───────────┐
         ▼                       ▼
  ┌─────────────┐        ┌──────────────┐
  │  SQL Part   │        │   VIZ Part   │
  │ "SELECT..." │        │ "VISUALISE..." │
  └──────┬──────┘        └──────┬───────┘
         │                      │
         ▼                      ▼
  ┌─────────────┐        ┌──────────────┐
  │   Reader    │        │    Parser    │
  │  (DuckDB,   │        │ (tree-sitter)│
  │   Postgres) │        │   → AST      │
  └──────┬──────┘        └──────┬───────┘
         │                      │
         ▼                      │
  ┌─────────────┐               │
  │  DataFrame  │               │
  │  (Polars)   │               │
  └──────┬──────┘               │
         │                      │
         └──────────┬───────────┘
                    ▼
         ┌─────────────────────┐
         │      Writer          │
         │  (Vega-Lite, ggplot) │
         └──────────┬───────────┘
                    ▼
         ┌─────────────────────┐
         │   Visualization      │
         │  (JSON, PNG, HTML)   │
         └─────────────────────┘
```

### Key Design Principles

1. **Separation of Concerns**: SQL handles data retrieval, VISUALISE handles visual encoding
2. **Pluggable Architecture**: Readers and Writers are trait-based, enabling multiple backends
3. **Grammar of Graphics**: Composable layers, explicit aesthetic mappings, scale transformations
4. **Terminal Operation**: Produces visualizations, not relational data (like SQL's `SELECT`)
5. **Type Safety**: Strong typing through AST with Rust's type system

---

## Component Breakdown

### 1. Parser Module (`src/parser/`)

**Responsibility**: Split queries and parse visualization specifications into typed AST.

#### Query Splitter (`splitter.rs`)

- Uses tree-sitter to parse the full query and find VISUALISE statements
- Splits query at byte offset of first VISUALISE statement
- Handles VISUALISE FROM by injecting `SELECT * FROM <source>`
- Robust to parse errors in SQL portion (complex SQL we don't fully parse)
- Properly handles semicolons between SQL statements

**Key Features:**
1. **Byte offset splitting**: Uses character positions instead of parse tree node boundaries
2. **SELECT injection**: Automatically adds `SELECT * FROM <source>` when VISUALISE FROM is used
3. **Semicolon handling**: Adds semicolons correctly between statements, with special case for WITH

```rust
pub fn split_query(query: &str) -> Result<(String, String)> {
    // Parse with tree-sitter to find VISUALISE statements
    let tree = parser.parse(query, None)?;
    let root = tree.root_node();

    // Find first VISUALISE statement by byte offset
    let first_viz_start: Option<usize> = root.children(&mut root.walk())
        .find(|n| n.kind() == "visualise_statement")
        .map(|n| n.start_byte());

    // Split at byte offset (robust to parse errors)
    let (sql_text, viz_text) = if let Some(viz_start) = first_viz_start {
        let sql_part = &query[..viz_start];
        let viz_part = &query[viz_start..];
        (sql_part.trim().to_string(), viz_part.trim().to_string())
    } else {
        (query.to_string(), String::new())
    };

    // Check for VISUALISE FROM and inject SELECT if needed
    let mut modified_sql = sql_text.clone();
    for child in root.children(&mut root.walk()) {
        if child.kind() == "visualise_statement" {
            if let Some(from_identifier) = extract_from_identifier(&child, query) {
                // Inject SELECT * FROM <source>
                if modified_sql.trim().is_empty() {
                    modified_sql = format!("SELECT * FROM {}", from_identifier);
                } else {
                    // Handle semicolons correctly
                    let trimmed = modified_sql.trim();
                    let last_is_with = trimmed.to_uppercase().starts_with("WITH");

                    if last_is_with {
                        // WITH followed by SELECT - no semicolon
                        modified_sql = format!("{} SELECT * FROM {}", trimmed, from_identifier);
                    } else if trimmed.ends_with(';') {
                        // Already has semicolon
                        modified_sql = format!("{} SELECT * FROM {}", trimmed, from_identifier);
                    } else {
                        // Add semicolon before SELECT
                        modified_sql = format!("{}; SELECT * FROM {}", trimmed, from_identifier);
                    }
                }
                break;
            }
        }
    }

    Ok((modified_sql, viz_text))
}
```

**Why byte offset splitting?**
- Complex SQL queries may have parse errors (we don't fully parse SQL)
- Byte offset splitting works even when SQL portion has ERROR nodes
- More robust than relying on clean parse tree node boundaries

#### Tree-sitter Integration (`mod.rs`)

- Uses `tree-sitter-vvsql` grammar (293 lines, simplified approach)
- Parses **full query** (SQL + VISUALISE) into concrete syntax tree (CST)
- Grammar supports: PLOT/TABLE/MAP types, WITH/SCALE/FACET/COORD/LABEL/GUIDE/THEME clauses
- British and American spellings: `VISUALISE` / `VISUALIZE`
- **SQL portion parsing**: Basic SQL structure (SELECT, WITH, CREATE, INSERT, subqueries)
- **Recursive subquery support**: Fully recursive grammar for complex SQL

**Grammar Structure** (`tree-sitter-vvsql/grammar.js`):

Key grammar rules:
- `query`: Root node containing SQL + VISUALISE portions
- `sql_portion`: Zero or more SQL statements before VISUALISE
- `with_statement`: WITH clause with optional trailing SELECT (compound statement)
- `subquery`: Fully recursive subquery rule supporting nested parentheses
- `visualise_statement`: VISUALISE AS/FROM clause with viz_type and clauses

**Critical Grammar Features**:
1. **Compound WITH statements**: `WITH cte AS (...) SELECT * FROM cte` parses as single statement
2. **Recursive subqueries**: `SELECT * FROM (SELECT * FROM (VALUES (1, 2)))` fully supported
3. **VISUALISE FROM extraction**: Grammar identifies FROM identifier for SELECT injection

```rust
pub fn parse_query(query: &str) -> Result<Vec<VizSpec>> {
    // Parse full query (SQL + VISUALISE) with tree-sitter
    let tree = parse_full_query(query)?;

    // Build AST from parse tree
    let specs = builder::build_ast(&tree, query)?;
    Ok(specs)
}
```

#### AST Builder (`builder.rs`)

- Converts tree-sitter CST → typed AST (`VizSpec` structure)
- Handles multiple visualization specifications per query
- **Validates VISUALISE FROM semantics**: Checks for SELECT+VISUALISE FROM conflicts
- Validates grammar structure during parsing

**Validation Logic**:

The builder performs semantic validation that cannot be done in the grammar:

1. **Check last SQL statement type**: Determines if query ends with SELECT
2. **Recursive WITH validation**: Distinguishes between internal CTEs and trailing SELECT
3. **VISUALISE FROM validation**: Errors if `VISUALISE FROM` used after SELECT statement

```rust
pub fn build_ast(tree: &Tree, source: &str) -> Result<Vec<VizSpec>> {
    let root = tree.root_node();

    // Extract SQL portion and check if last statement is SELECT
    let sql_portion_node = root.children(&mut root.walk())
        .find(|n| n.kind() == "sql_portion");

    let last_is_select = if let Some(sql_node) = sql_portion_node {
        check_last_statement_is_select(&sql_node)
    } else {
        false
    };

    // Build VizSpec for each VISUALISE statement
    let mut specs = Vec::new();
    for child in root.children(&mut root.walk()) {
        if child.kind() == "visualise_statement" {
            let spec = build_visualise_statement(&child, source)?;

            // Validate VISUALISE FROM usage
            if spec.source.is_some() && last_is_select {
                return Err(VvsqlError::ParseError(
                    "Cannot use VISUALISE FROM when the last SQL statement is SELECT. \
                     Use either 'SELECT ... VISUALISE AS' or remove the SELECT and use \
                     'VISUALISE FROM ... AS'.".to_string()
                ));
            }

            specs.push(spec);
        }
    }

    Ok(specs)
}

/// Check if a with_statement has a trailing SELECT (after the CTE definitions)
fn with_statement_has_trailing_select(with_node: &Node) -> bool {
    let mut cursor = with_node.walk();
    let mut seen_cte_definition = false;

    for child in with_node.children(&mut cursor) {
        if child.kind() == "cte_definition" {
            seen_cte_definition = true;
        } else if child.kind() == "select_statement" && seen_cte_definition {
            // This is a SELECT after CTE definitions (trailing SELECT)
            return true;
        }
    }

    false
}
```

**Why Recursive Validation?**

The grammar treats `WITH cte AS (SELECT 1)` as a single statement where the internal SELECT is part of the CTE definition. The validation must distinguish between:
- `WITH cte AS (SELECT 1)` - No trailing SELECT, VISUALISE FROM allowed
- `WITH cte AS (SELECT 1) SELECT * FROM cte` - Has trailing SELECT, VISUALISE FROM not allowed

#### AST Types (`ast.rs`)

Core data structures representing visualization specifications:

```rust
pub struct VizSpec {
    pub viz_type: VizType,           // PLOT, TABLE, MAP
    pub source: Option<String>,      // FROM source (for VISUALISE FROM)
    pub layers: Vec<Layer>,          // WITH clauses
    pub scales: Vec<Scale>,          // SCALE clauses
    pub facet: Option<Facet>,        // FACET clause
    pub coord: Option<Coord>,        // COORD clause
    pub labels: Option<Labels>,      // LABEL clause
    pub guides: Vec<Guide>,          // GUIDE clauses
    pub theme: Option<Theme>,        // THEME clause
}

pub struct Layer {
    pub geom: Geom,                  // point, line, bar, etc.
    pub aesthetics: HashMap<String, AestheticValue>,
    pub name: Option<String>,
}

pub enum Geom {
    Point, Line, Bar, Area, Tile, Ribbon,
    Histogram, Density, Smooth, Boxplot,
    Text, Segment, HLine, VLine,
}

pub enum AestheticValue {
    Column(String),                  // Unquoted: x = revenue
    Literal(Value),                  // Quoted: color = 'blue'
}

pub struct Scale {
    pub aesthetic: String,
    pub scale_type: Option<ScaleType>,
    pub properties: HashMap<String, Value>,
}

pub enum ScaleType {
    Linear, Log10, Sqrt, Reverse,
    Ordinal, Categorical,
    Date, DateTime, Time,
    Viridis, Plasma, Magma, // Color palettes
}
```

**Key Methods**:

- `VizSpec::find_scale()` - Look up scale specification for an aesthetic
- Type conversions for JSON serialization/deserialization

#### Error Handling (`error.rs`)

**Detailed parse error reporting** with location information:

```rust
pub struct ParseError {
    pub message: String,      // Error description
    pub line: usize,          // Line number (0-based)
    pub column: usize,        // Column number (0-based)
    pub context: String,      // Parsing context
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} at line {}, column {} (in {})",
            self.message,
            self.line + 1,    // Display as 1-based
            self.column + 1,
            self.context
        )
    }
}
```

**Benefits**:

- Precise error location reporting for user-friendly diagnostics
- Context information helps identify where parsing failed
- Converts to VvsqlError for unified error handling

---

### 2. Reader Module (`src/reader/`)

**Responsibility**: Abstract data source access, execute SQL, return DataFrames.

#### Reader Trait (`mod.rs`)

```rust
pub trait Reader {
    fn execute(&self, sql: &str) -> Result<DataFrame>;
    fn supports_query(&self, sql: &str) -> bool;
}
```

#### DuckDB Reader (`duckdb.rs`)

**Current Production Reader** - Fully implemented and tested.

**Features**:

- In-memory databases: `duckdb://memory`
- File-based databases: `duckdb://path/to/file.db`
- SQL execution → Polars DataFrame conversion
- Comprehensive type handling

**Type Conversions**:

```rust
fn value_to_string(value: &ValueRef) -> String {
    match value {
        // Basic types
        Ok(ValueRef::Int32(i)) => i.to_string(),
        Ok(ValueRef::Int64(i)) => i.to_string(),
        Ok(ValueRef::Double(f)) => f.to_string(),
        Ok(ValueRef::Text(s)) => String::from_utf8_lossy(s).to_string(),

        // Date/Time conversions (CRITICAL for proper visualization)
        Ok(ValueRef::Date32(d)) => {
            // Convert days since Unix epoch to ISO date (YYYY-MM-DD)
            let unix_epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let date = unix_epoch + chrono::Duration::days(d as i64);
            date.format("%Y-%m-%d").to_string()
        },
        Ok(ValueRef::Timestamp(_, ts)) => {
            // Convert microseconds since Unix epoch to ISO datetime
            let secs = ts / 1_000_000;
            let nsecs = ((ts % 1_000_000) * 1000) as u32;
            let unix_epoch = chrono::DateTime::<chrono::Utc>::from_timestamp(secs, nsecs)
                .unwrap();
            unix_epoch.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
        },
        _ => String::new(),
    }
}
```

**Why Date/Time Conversion Matters**:

- DuckDB stores dates as `Date32` (days since 1970-01-01)
- DuckDB stores timestamps as `Timestamp` (microseconds since epoch)
- Without conversion, dates appear as numbers (e.g., `19727.0`)
- ISO format enables proper temporal axis formatting in Vega-Lite

**Connection Parsing** (`connection.rs`):

```rust
pub fn parse_connection_string(uri: &str) -> Result<ConnectionInfo> {
    // duckdb://memory → In-memory database
    // duckdb:///path/to/file.db → File-based database
}
```

---

### 3. Writer Module (`src/writer/`)

**Responsibility**: Convert DataFrame + VizSpec → output format (JSON, PNG, R code, etc.)

#### Writer Trait (`mod.rs`)

```rust
pub trait Writer {
    fn write(&self, df: &DataFrame, spec: &VizSpec) -> Result<String>;
    fn file_extension(&self) -> &str;
}
```

#### Vega-Lite Writer (`vegalite.rs`)

**Current Production Writer** - Fully implemented and tested.

**Features**:

- Converts VizSpec → Vega-Lite JSON specification
- Multi-layer composition support
- Scale type → Vega field type mapping
- Faceting (wrap and grid layouts)
- Axis label customization
- Inline data embedding

**Architecture**:

```rust
impl Writer for VegaLiteWriter {
    fn write(&self, df: &DataFrame, spec: &VizSpec) -> Result<String> {
        // 1. Convert DataFrame to JSON values
        let data_values = self.dataframe_to_json(df)?;

        // 2. Build Vega-Lite spec
        let mut vl_spec = json!({
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "data": {"values": data_values},
            "width": 600,
            "autosize": {"type": "fit", "contains": "padding"}
        });

        // 3. Handle single vs multi-layer
        if spec.layers.len() == 1 {
            // Single layer: flat structure
            vl_spec["mark"] = self.geom_to_mark(&spec.layers[0].geom);
            vl_spec["encoding"] = self.build_encoding(&spec.layers[0], df, spec)?;
        } else {
            // Multi-layer: layered structure
            let layers: Vec<Value> = spec.layers.iter()
                .map(|layer| {
                    let mut layer_spec = json!({
                        "mark": self.geom_to_mark(&layer.geom),
                        "encoding": self.build_encoding(layer, df, spec)?
                    });
                    // Apply axis labels to each layer
                    apply_axis_labels(&mut layer_spec, &spec.labels);
                    Ok(layer_spec)
                })
                .collect::<Result<Vec<_>>>()?;
            vl_spec["layer"] = json!(layers);
        }

        // 4. Add faceting, title, etc.
        self.add_faceting(&mut vl_spec, spec)?;
        if let Some(labels) = &spec.labels {
            if let Some(title) = labels.labels.get("title") {
                vl_spec["title"] = json!(title);
            }
        }

        Ok(serde_json::to_string_pretty(&vl_spec)?)
    }
}
```

**Key Mappings**:

**Geom → Vega Mark**:

```rust
fn geom_to_mark(&self, geom: &Geom) -> Value {
    json!(match geom {
        Geom::Point => "point",
        Geom::Line => "line",
        Geom::Bar => "bar",
        Geom::Area => "area",
        Geom::Tile => "rect",
        Geom::Text => "text",
        _ => "point" // fallback
    })
}
```

**Scale Type → Vega Field Type** (CRITICAL for date formatting):

```rust
fn build_encoding_channel(&self, aesthetic: &str, value: &AestheticValue,
                          df: &DataFrame, spec: &VizSpec) -> Result<Value> {
    match value {
        AestheticValue::Column(col) => {
            // Check for explicit SCALE specification
            let field_type = if let Some(scale) = spec.find_scale(aesthetic) {
                if let Some(scale_type) = &scale.scale_type {
                    match scale_type {
                        ScaleType::Linear | ScaleType::Log10 | ScaleType::Sqrt => "quantitative",
                        ScaleType::Ordinal | ScaleType::Categorical => "nominal",
                        ScaleType::Date | ScaleType::DateTime | ScaleType::Time => "temporal",
                        _ => "quantitative"
                    }
                } else {
                    self.infer_field_type(df, col) // Auto-detect from DataFrame
                }
            } else {
                self.infer_field_type(df, col)
            };

            Ok(json!({
                "field": col,
                "type": field_type
            }))
        }
        AestheticValue::Literal(val) => {
            // Direct value (e.g., color = 'blue')
            Ok(json!({"value": val}))
        }
    }
}
```

**Multi-Layer Axis Labels Fix** (Critical bug fix):

```rust
// In multi-layer code path, apply axis labels to EACH layer
for layer in &spec.layers {
    let mut encoding = self.build_encoding(layer, df, spec)?;

    // Override axis titles from LABEL clause
    if let Some(labels) = &spec.labels {
        if let Some(x_label) = labels.labels.get("x") {
            if let Some(x_enc) = encoding.get_mut("x") {
                x_enc["title"] = json!(x_label);
            }
        }
        if let Some(y_label) = labels.labels.get("y") {
            if let Some(y_enc) = encoding.get_mut("y") {
                y_enc["title"] = json!(y_label);
            }
        }
    }
}
```

**Why This Matters**:

- Without scale type mapping, dates display as numbers
- Without multi-layer axis labels, custom axis titles disappear when adding layers
- These fixes enable proper temporal visualization and consistent labeling

---

### 4. REST API (`src/rest.rs`)

**Responsibility**: HTTP interface for executing vvSQL queries.

**Technology**: Axum web framework with CORS support

**Endpoints**:

```rust
// POST /api/v1/query - Execute vvSQL query
// Request:
{
  "query": "SELECT ... VISUALISE AS PLOT ...",
  "reader": "duckdb://memory",  // optional, default
  "writer": "vegalite"            // optional, default
}

// Response (success):
{
  "status": "success",
  "data": {
    "spec": { /* Vega-Lite JSON */ },
    "metadata": {
      "rows": 100,
      "columns": ["date", "revenue", "region", "..."],
      "viz_type": "PLOT",
      "layers": 2
    }
  }
}

// Response (error):
{
  "status": "error",
  "error": {
    "type": "ParseError",
    "message": "..."
  }
}
```

**Additional Endpoints**:

- `POST /api/v1/parse` - Parse query and return AST (debugging)
- `GET /api/v1/health` - Health check
- `GET /api/v1/version` - Version info

**CORS Configuration**: Allows cross-origin requests for web frontends

**CLI Options**:

```bash
# Basic usage
vvsql-rest --host 127.0.0.1 --port 3334

# With sample data (pre-loaded products, sales, employees tables)
vvsql-rest --load-sample-data

# Load custom data files (CSV, Parquet, JSON)
vvsql-rest --load-data data.csv --load-data other.parquet

# Configure CORS origins
vvsql-rest --cors-origin "http://localhost:5173,http://localhost:3000"
```

**Sample Data Loading**:

- `--load-sample-data`: Loads built-in sample data (products, sales, employees)
- `--load-data <file>`: Loads data from CSV, Parquet, or JSON files into in-memory database
- Multiple `--load-data` flags can be used to load multiple files
- Pre-loaded data persists for the lifetime of the server session

---

### 5. CLI (`src/cli.rs`)

**Responsibility**: Command-line interface for local query execution.

**Commands**:

```bash
# Parse query and show AST
vvsql parse "SELECT ... VISUALISE AS PLOT ..."

# Execute query and generate output
vvsql exec "SELECT ... VISUALISE AS PLOT ..." \
  --reader duckdb://memory \
  --writer vegalite \
  --output viz.vl.json

# Execute query from file
vvsql run query.sql --writer vegalite

# Validate query syntax
vvsql validate query.sql
```

**Features**:

- JSON or human-readable output formats
- File or stdin input
- Pluggable reader/writer selection
- Error reporting with context

---

### 6. Test Application (`test-app/`)

**Responsibility**: Interactive web UI for testing vvSQL queries.

**Technology Stack**:

- React + TypeScript
- Vega-Lite for rendering
- shadcn/ui components
- esbuild for bundling
- Tailwind CSS for styling

**Architecture**:

```
App.tsx
├── QueryEditor.tsx          # SQL + VISUALISE editor
├── ExampleQueries.tsx       # Pre-built example gallery
├── VegaRenderer.tsx         # Vega-Lite chart rendering
├── MetadataPanel.tsx        # Execution stats display
└── services/api.ts          # REST API client
```

**Features**:

- Live query editing with syntax highlighting
- One-click example query loading
- Real-time visualization rendering
- Error display with type information
- Execution metadata (rows, columns, timing)

**Example Queries Included**:

1. **Regional Trends** - Multi-line chart with date scale and colored regions
2. **Faceted Categories** - Category trends faceted by product category with colored regions
3. **Product Revenue** - Bar chart showing total revenue by product with JOIN operations

**Sample Data**: DuckDB in-memory database with:

- `products` table (5 products)
- `sales` table (1000+ transactions)
- `employees` table (10 sales staff)

**Usage**:

```bash
cd test-app
npm install
npm run dev  # Starts on http://localhost:5173
```

---

## Grammar Deep Dive

### vvSQL Grammar Structure

```sql
[SELECT ...] VISUALISE AS <type> [clauses]...
```

### Clause Types

| Clause         | Repeatable | Purpose            | Example                              |
| -------------- | ---------- | ------------------ | ------------------------------------ |
| `VISUALISE AS` | ✅ Yes     | Entry point        | `VISUALISE AS PLOT`                  |
| `WITH`         | ✅ Yes     | Define layers      | `WITH line USING x=date, y=value`    |
| `SCALE`        | ✅ Yes     | Configure scales   | `SCALE x USING type='date'`          |
| `FACET`        | ❌ No      | Small multiples    | `FACET WRAP region`                  |
| `COORD`        | ❌ No      | Coordinate system  | `COORD cartesian USING xlim=[0,100]` |
| `LABEL`        | ❌ No      | Text labels        | `LABEL title='My Chart', x='Date'`   |
| `GUIDE`        | ✅ Yes     | Legend/axis config | `GUIDE color USING position='right'` |
| `THEME`        | ❌ No      | Visual styling     | `THEME minimal`                      |

### WITH Clause (Layers)

**Syntax**:

```sql
WITH <geom> USING <aesthetic> = <value>, ... [AS <name>]
```

**Geom Types**:

- **Basic**: `point`, `line`, `bar`, `area`, `tile`, `ribbon`
- **Statistical**: `histogram`, `density`, `smooth`, `boxplot`
- **Annotation**: `text`, `segment`, `hline`, `vline`

**Common Aesthetics**:

- **Position**: `x`, `y`, `xmin`, `xmax`, `ymin`, `ymax`
- **Color**: `color`, `fill`, `alpha`
- **Size/Shape**: `size`, `shape`, `linetype`, `linewidth`
- **Text**: `label`, `family`, `fontface`

**Literal vs Column**:

- Unquoted → column reference: `color = region`
- Quoted → literal value: `color = 'blue'`, `size = 3`

**Example**:

```sql
WITH line USING x = date, y = revenue, color = region, size = 2
WITH point USING x = date, y = revenue, color = region AS "data_points"
```

### SCALE Clause

**Syntax**:

```sql
SCALE <aesthetic> USING
  [type = <scale_type>]
  [limits = [min, max]]
  [breaks = <array | interval>]
  [palette = <name>]
  [domain = [values...]]
```

**Scale Types**:

- **Continuous**: `linear`, `log10`, `log2`, `sqrt`, `reverse`
- **Discrete**: `categorical`, `ordinal`
- **Temporal**: `date`, `datetime`, `time`
- **Color Palettes**: `viridis`, `plasma`, `magma`, `inferno`, `diverging`

**Critical for Date Formatting**:

```sql
SCALE x USING type = 'date'
-- Maps to Vega-Lite field type = "temporal"
-- Enables proper date axis formatting
```

**Domain Property**:

The `domain` property explicitly sets the input domain for a scale:

```sql
-- Set domain for discrete scale
SCALE color USING domain = ['red', 'green', 'blue']

-- Set domain for continuous scale
SCALE x USING domain = [0, 100]
```

**Note**: Cannot specify domain in both SCALE and COORD for the same aesthetic (will error).

**Example**:

```sql
SCALE x USING type = 'date', breaks = '2 months'
SCALE y USING type = 'log10', limits = [1, 1000]
SCALE color USING palette = 'viridis', domain = ['A', 'B', 'C']
```

### FACET Clause

**Syntax**:

```sql
-- Grid layout
FACET <row_vars> BY <col_vars> [USING scales = <sharing>]

-- Wrapped layout
FACET WRAP <vars> [USING scales = <sharing>]
```

**Scale Sharing**:

- `'fixed'` (default) - Same scales across all facets
- `'free'` - Independent scales for each facet
- `'free_x'` - Independent x-axis, shared y-axis
- `'free_y'` - Independent y-axis, shared x-axis

**Example**:

```sql
FACET WRAP region USING scales = 'free_y'
FACET region BY category USING scales = 'fixed'
```

### COORD Clause

**Syntax**:

```sql
-- With coordinate type
COORD <type> [USING <properties>]

-- With properties only (defaults to cartesian)
COORD USING <properties>
```

**Coordinate Types**:

- **`cartesian`** - Standard x/y Cartesian coordinates (default)
- **`flip`** - Flipped Cartesian (swaps x and y axes)
- **`polar`** - Polar coordinates (for pie charts, rose plots)
- **`fixed`** - Fixed aspect ratio
- **`trans`** - Transformed coordinates
- **`map`** - Map projections
- **`quickmap`** - Quick approximation for maps

**Properties by Type**:

**Cartesian**:
- `xlim = [min, max]` - Set x-axis limits
- `ylim = [min, max]` - Set y-axis limits
- `<aesthetic> = [values...]` - Set domain for any aesthetic (color, fill, size, etc.)

**Flip**:
- `<aesthetic> = [values...]` - Set domain for any aesthetic

**Polar**:
- `theta = <aesthetic>` - Which aesthetic maps to angle (defaults to `y`)
- `<aesthetic> = [values...]` - Set domain for any aesthetic

**Important Notes**:

1. **Axis limits auto-swap**: `xlim = [100, 0]` automatically becomes `[0, 100]`
2. **ggplot2 compatibility**: `coord_flip` preserves axis label names (labels stay with aesthetic names, not visual position)
3. **Domain conflicts**: Error if same aesthetic has domain in both SCALE and COORD
4. **Multi-layer support**: All coordinate transforms apply to all layers

**Status**:
- ✅ **Cartesian**: Fully implemented and tested
- ✅ **Flip**: Fully implemented and tested
- ✅ **Polar**: Fully implemented and tested
- ❌ **Other types**: Not yet implemented

**Examples**:

```sql
-- Cartesian with axis limits
COORD cartesian USING xlim = [0, 100], ylim = [0, 50]

-- Cartesian with aesthetic domain
COORD cartesian USING color = ['red', 'green', 'blue']

-- Cartesian shorthand (type optional when using USING)
COORD USING xlim = [0, 100]

-- Flip coordinates for horizontal bar chart
COORD flip

-- Flip with aesthetic domain
COORD flip USING color = ['A', 'B', 'C']

-- Polar for pie chart (theta defaults to y)
COORD polar

-- Polar for rose plot (x maps to radius)
COORD polar USING theta = y

-- Combined with other clauses
WITH bar USING x = category, y = value
COORD cartesian USING xlim = [0, 100], ylim = [0, 200]
LABEL x = 'Category', y = 'Count'
```

**Breaking Change**: The COORD syntax changed from `COORD USING type = 'cartesian'` to `COORD cartesian`. Queries using the old syntax will need to be updated.

### LABEL Clause

**Syntax**:

```sql
LABEL
  [title = <string>]
  [subtitle = <string>]
  [x = <string>]
  [y = <string>]
  [<aesthetic> = <string>]
  [caption = <string>]
  [tag = <string>]
```

**Example**:

```sql
LABEL
  title = 'Sales by Region',
  x = 'Date',
  y = 'Revenue (USD)',
  caption = 'Data from Q4 2024'
```

### THEME Clause

**Syntax**:

```sql
THEME <name> [USING <overrides>]
```

**Base Themes**: `minimal`, `classic`, `gray`, `bw`, `dark`, `void`

**Example**:

```sql
THEME minimal
THEME dark USING background = '#1a1a1a'
```

---

## Complete Example Walkthrough

### Query

```sql
SELECT sale_date, region, SUM(quantity) as total
FROM sales
WHERE sale_date >= '2024-01-01'
GROUP BY sale_date, region
ORDER BY sale_date
VISUALISE AS PLOT
WITH line USING x = sale_date, y = total, color = region
WITH point USING x = sale_date, y = total, color = region
SCALE x USING type = 'date'
FACET WRAP region
LABEL title = 'Sales Trends by Region', x = 'Date', y = 'Total Quantity'
THEME minimal
```

### Execution Flow

**1. Query Splitting**

```rust
// splitter.rs
SQL:  "SELECT sale_date, region, SUM(quantity) as total FROM sales ..."
VIZ:  "VISUALISE AS PLOT WITH line USING x = sale_date, ..."
```

**2. SQL Execution** (DuckDB Reader)

```rust
// duckdb.rs
connection.execute(sql) → ResultSet
ResultSet → DataFrame (Polars)

// DataFrame columns: sale_date (Date32), region (String), total (Int64)
// Date32 values converted to ISO format: "2024-01-01"
```

**3. VIZ Parsing** (tree-sitter)

```rust
// parser/mod.rs
Tree-sitter CST → AST

VizSpec {
  viz_type: VizType::Plot,
  layers: [
    Layer { geom: Geom::Line, aesthetics: {"x": "sale_date", "y": "total", "color": "region"} },
    Layer { geom: Geom::Point, aesthetics: {"x": "sale_date", "y": "total", "color": "region"} }
  ],
  scales: [
    Scale { aesthetic: "x", scale_type: Some(ScaleType::Date) }
  ],
  facet: Some(Facet::Wrap { variables: ["region"], scales: "fixed" }),
  labels: Some(Labels { labels: {"title": "...", "x": "Date", "y": "Total Quantity"} }),
  theme: Some(Theme::Minimal)
}
```

**4. Vega-Lite Generation** (VegaLite Writer)

```json
{
  "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
  "data": {
    "values": [
      {"sale_date": "2024-01-01", "region": "North", "total": 150},
      {"sale_date": "2024-01-01", "region": "South", "total": 120},
      ...
    ]
  },
  "title": "Sales Trends by Region",
  "width": 600,
  "autosize": {"type": "fit", "contains": "padding"},
  "facet": {
    "field": "region",
    "type": "nominal"
  },
  "spec": {
    "layer": [
      {
        "mark": "line",
        "encoding": {
          "x": {"field": "sale_date", "type": "temporal", "title": "Date"},
          "y": {"field": "total", "type": "quantitative", "title": "Total Quantity"},
          "color": {"field": "region", "type": "nominal"}
        }
      },
      {
        "mark": "point",
        "encoding": {
          "x": {"field": "sale_date", "type": "temporal", "title": "Date"},
          "y": {"field": "total", "type": "quantitative", "title": "Total Quantity"},
          "color": {"field": "region", "type": "nominal"}
        }
      }
    ]
  }
}
```

**5. Rendering** (Browser/Vega-Lite)

- Vega-Lite JSON → SVG/Canvas visualization
- Faceted multi-line chart with points
- Temporal x-axis with proper date formatting
- Color-coded regions
- Interactive tooltips

---

## Data Flow Example

### Input Query

```sql
SELECT '2024-01-01'::DATE + INTERVAL (n) DAY as date,
       n * 10 as revenue
FROM generate_series(0, 30) as t(n)
VISUALISE AS PLOT
WITH line USING x = date, y = revenue
SCALE x USING type = 'date'
LABEL title = 'Revenue Growth', x = 'Date', y = 'Revenue ($)'
```

### Step-by-Step Transformation

**1. Query Split**

```
SQL: "SELECT '2024-01-01'::DATE + INTERVAL (n) DAY as date, n * 10 as revenue FROM generate_series(0, 30) as t(n)"
VIZ: "VISUALISE AS PLOT WITH line USING x = date, y = revenue SCALE x USING type = 'date' LABEL title = 'Revenue Growth', x = 'Date', y = 'Revenue ($)'"
```

**2. DuckDB Execution**

```
Query Result (DuckDB internal):
┌────────────┬─────────┐
│ date       │ revenue │
│ Timestamp  │ Int64   │
├────────────┼─────────┤
│ 1704067200 │ 0       │  (microseconds)
│ 1704153600 │ 10      │
│ 1704240000 │ 20      │
└────────────┴─────────┘
```

**3. Type Conversion** (DuckDB Reader)

```
DataFrame (Polars):
┌────────────┬─────────┐
│ date       │ revenue │
│ String     │ Int64   │
├────────────┼─────────┤
│ "2024-01-01T00:00:00.000Z" │ 0       │
│ "2024-01-02T00:00:00.000Z" │ 10      │
│ "2024-01-03T00:00:00.000Z" │ 20      │
└────────────────────────────┴─────────┘
```

**4. AST Parsing**

```rust
VizSpec {
    viz_type: Plot,
    layers: [
        Layer {
            geom: Line,
            aesthetics: {
                "x": Column("date"),
                "y": Column("revenue")
            }
        }
    ],
    scales: [
        Scale { aesthetic: "x", scale_type: Some(Date) }
    ],
    labels: Labels {
        labels: {
            "title": "Revenue Growth",
            "x": "Date",
            "y": "Revenue ($)"
        }
    }
}
```

**5. Vega-Lite Output**

```json
{
  "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
  "title": "Revenue Growth",
  "data": {
    "values": [
      { "date": "2024-01-01T00:00:00.000Z", "revenue": 0 },
      { "date": "2024-01-02T00:00:00.000Z", "revenue": 10 },
      { "date": "2024-01-03T00:00:00.000Z", "revenue": 20 }
    ]
  },
  "mark": "line",
  "encoding": {
    "x": {
      "field": "date",
      "type": "temporal",
      "title": "Date"
    },
    "y": {
      "field": "revenue",
      "type": "quantitative",
      "title": "Revenue ($)"
    }
  },
  "width": 600,
  "autosize": { "type": "fit", "contains": "padding" }
}
```

**6. Visual Output**

- Line chart with temporal x-axis
- Dates formatted as "Jan 1", "Jan 2", "Jan 3"
- Y-axis shows revenue values
- Chart title: "Revenue Growth"
- Axis labels: "Date" and "Revenue ($)"
