# Plan: Implicit Stats for Layer-Specific Database Preprocessing

This plan outlines the changes needed to implement implicit statistical transformations (like ggplot2's `stat_count`, `stat_bin`, etc.) where certain geoms automatically trigger database-side aggregation.

## Goal

Allow different layers to have different data preprocessing done at the database level without exposing this to the user. For example:
- `DRAW bar MAPPING category AS x` automatically counts occurrences (no y needed)
- `DRAW histogram MAPPING value AS x` automatically bins and counts
- `DRAW point MAPPING x AS x, y AS y` uses raw data

## Vega-Lite Output Strategy

Use named datasets so different layers can reference different data:

```json
{
  "datasets": {
    "base_data": [...],
    "layer_0": [...]
  },
  "layer": [
    {"data": {"name": "layer_0"}, "mark": "bar", ...},
    {"data": {"name": "base_data"}, "mark": "point", ...}
  ]
}
```

---

## 1. Define Stat Requirements (`src/parser/ast.rs`)

Add stat types and map geoms to their default stats:

```rust
pub enum Stat {
    Identity,    // No transformation (point, line, path, etc.)
    Count,       // COUNT(*) GROUP BY x (bar with only x)
    Bin,         // Histogram binning + count
    Boxplot,     // min, q1, median, q3, max
    Density,     // KDE (may need client-side fallback)
    Smooth,      // Regression (client-side fallback)
}

impl Geom {
    pub fn default_stat(&self) -> Stat {
        match self {
            Geom::Bar | Geom::Col => Stat::Count,
            Geom::Histogram => Stat::Bin,
            Geom::Boxplot => Stat::Boxplot,
            Geom::Density | Geom::Violin => Stat::Density,
            Geom::Smooth => Stat::Smooth,
            _ => Stat::Identity,
        }
    }
}
```

---

## 2. Stat Resolution Logic (`src/parser/ast.rs` or new `src/stats.rs`)

Determine actual stat based on geom + provided aesthetics:

```rust
impl Layer {
    pub fn resolve_stat(&self) -> Stat {
        let default = self.geom.default_stat();

        // If y is explicitly provided for bar, use identity (no aggregation)
        if matches!(self.geom, Geom::Bar | Geom::Col)
           && self.aesthetics.contains_key("y") {
            return Stat::Identity;
        }

        default
    }
}
```

---

## 3. SQL Generation for Stats (new `src/stats/sql.rs`)

Generate SQL transformations for each stat type:

```rust
pub fn generate_stat_sql(
    base_query: &str,
    layer: &Layer,
    layer_index: usize,
) -> Option<String> {
    match layer.resolve_stat() {
        Stat::Identity => None,  // Use base query as-is

        Stat::Count => {
            let x_col = layer.get_column("x")?;
            Some(format!(
                "layer_{idx} AS (
                    SELECT {x} AS x, COUNT(*) AS y
                    FROM base_data
                    GROUP BY {x}
                )",
                idx = layer_index,
                x = x_col
            ))
        },

        Stat::Bin => {
            let x_col = layer.get_column("x")?;
            let bins = 30; // or from parameters
            Some(format!(
                "layer_{idx} AS (
                    SELECT
                        FLOOR({x} / bin_width) * bin_width AS x,
                        COUNT(*) AS y
                    FROM base_data,
                         (SELECT (MAX({x}) - MIN({x})) / {bins} AS bin_width FROM base_data)
                    GROUP BY 1
                )",
                idx = layer_index, x = x_col, bins = bins
            ))
        },

        Stat::Boxplot => {
            let x_col = layer.get_column("x")?;
            let y_col = layer.get_column("y")?;
            Some(format!(
                "layer_{idx} AS (
                    SELECT
                        {x} AS x,
                        MIN({y}) AS ymin,
                        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {y}) AS lower,
                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {y}) AS middle,
                        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {y}) AS upper,
                        MAX({y}) AS ymax
                    FROM base_data
                    GROUP BY {x}
                )",
                idx = layer_index, x = x_col, y = y_col
            ))
        },

        // Density/Smooth may need Vega-Lite transforms instead
        _ => None,
    }
}
```

---

## 4. Query Builder (new `src/stats/query_builder.rs`)

Combine base query with all layer CTEs:

```rust
pub fn build_multi_layer_query(
    base_sql: &str,
    spec: &VizSpec,
) -> (String, Vec<DatasetMapping>) {
    let mut ctes = vec![format!("base_data AS ({})", base_sql)];
    let mut mappings = vec![];

    for (idx, layer) in spec.layers.iter().enumerate() {
        if let Some(cte) = generate_stat_sql(base_sql, layer, idx) {
            ctes.push(cte);
            mappings.push(DatasetMapping {
                layer_index: idx,
                dataset_name: format!("layer_{}", idx),
            });
        } else {
            mappings.push(DatasetMapping {
                layer_index: idx,
                dataset_name: "base_data".to_string(),
            });
        }
    }

    // Final query selects from all CTEs
    let dataset_names: Vec<_> = mappings.iter()
        .map(|m| &m.dataset_name)
        .collect::<HashSet<_>>();

    let selects: Vec<_> = dataset_names.iter()
        .map(|name| format!(
            "SELECT '{name}' AS __dataset__, * FROM {name}"
        ))
        .collect();

    let query = format!(
        "WITH {} {}",
        ctes.join(",\n"),
        selects.join(" UNION ALL ")
    );

    (query, mappings)
}
```

---

## 5. Executor Changes (`src/executor.rs` or entry points)

Update the pipeline to use the query builder:

```rust
// Before:
let df = reader.execute(&sql_part)?;
let spec = parser::parse_query(&query)?;
let output = writer.write(&spec, &df)?;

// After:
let spec = parser::parse_query(&query)?;
let (full_query, mappings) = build_multi_layer_query(&sql_part, &spec);
let df = reader.execute(&full_query)?;
let datasets = split_by_dataset(df, &mappings);
let output = writer.write(&spec, &datasets)?;
```

---

## 6. Writer Changes (`src/writer/vegalite.rs`)

Accept multiple datasets and output named datasets:

```rust
// Change signature
fn write(&self, spec: &VizSpec, datasets: &HashMap<String, DataFrame>) -> Result<String>;

// In implementation
fn build_spec(&self, spec: &VizSpec, datasets: &HashMap<String, DataFrame>) -> Value {
    let mut vl_spec = json!({
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "datasets": {},
    });

    // Add each dataset
    for (name, df) in datasets {
        vl_spec["datasets"][name] = self.dataframe_to_json(df)?;
    }

    // Set default data
    vl_spec["data"] = json!({"name": "base_data"});

    // Build layers, each referencing its dataset
    let layers: Vec<Value> = spec.layers.iter().enumerate()
        .map(|(idx, layer)| {
            let dataset_name = mappings[idx].dataset_name;
            let mut layer_spec = json!({
                "mark": self.geom_to_mark(&layer.geom),
                "encoding": self.build_encoding(layer, datasets[&dataset_name], spec)?
            });

            // Only add data reference if not using default
            if dataset_name != "base_data" {
                layer_spec["data"] = json!({"name": dataset_name});
            }

            layer_spec
        })
        .collect();

    vl_spec["layer"] = json!(layers);
    vl_spec
}
```

---

## 7. Files to Modify/Create

| File | Change |
|------|--------|
| `src/parser/ast.rs` | Add `Stat` enum, `default_stat()`, `resolve_stat()` |
| `src/stats/mod.rs` | New module |
| `src/stats/sql.rs` | SQL generation for each stat type |
| `src/stats/query_builder.rs` | Combine base query + layer CTEs |
| `src/writer/mod.rs` | Update `Writer` trait signature |
| `src/writer/vegalite.rs` | Handle multiple datasets |
| `src/cli.rs` | Update execution flow |
| `src/rest.rs` | Update execution flow |
| `ggsql-jupyter/src/executor.rs` | Update execution flow |

---

## 8. Phased Implementation

| Phase | Stat | Complexity | Notes |
|-------|------|------------|-------|
| 1 | `Stat::Count` | Low | Bar charts - simplest case |
| 2 | `Stat::Bin` | Medium | Histograms - requires bin calculation |
| 3 | `Stat::Boxplot` | Medium | Boxplots - multiple aggregates |
| 4 | `Stat::Density` | High | May need client-side fallback (Vega-Lite transforms) |
| 5 | `Stat::Smooth` | High | Regression - likely client-side only |

---

## Example Usage (User Perspective)

```sql
-- Bar chart with implicit count (no y specified)
SELECT category FROM products
VISUALISE AS PLOT
DRAW bar MAPPING category AS x

-- Histogram with implicit binning
SELECT price FROM products
VISUALISE AS PLOT
DRAW histogram MAPPING price AS x

-- Mixed: bar with count + points with raw data
SELECT category, value FROM sales
VISUALISE AS PLOT
DRAW bar MAPPING category AS x           -- uses COUNT(*)
DRAW point MAPPING category AS x, value AS y  -- uses raw data
```

The user never sees the stat transformation - it's determined by the geom type and which aesthetics are provided.
