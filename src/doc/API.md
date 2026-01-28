# ggsql API Reference

This document provides a comprehensive reference for the ggsql public API.

## Overview

- **Stage 1: `prepare()`** - Parse query, execute SQL, resolve mappings, prepare data
- **Stage 2: `render()`** - Generate output (Vega-Lite JSON, etc.)

### API Functions

| Function     | Use Case                                             |
| ------------ | ---------------------------------------------------- |
| `prepare()`  | Main entry point - full visualization pipeline       |
| `render()`   | Generate output from prepared data                   |
| `validate()` | Validate syntax + semantics, inspect query structure |

---

## Core Functions

### `prepare`

```rust
pub fn prepare(query: &str, reader: &dyn Reader) -> Result<Prepared>
```

Prepare a ggsql query for visualization. This is the main entry point for the two-stage API.

**What happens during preparation:**

1. Parses the query (SQL + VISUALISE portions)
2. Executes the main SQL query using the provided reader
3. Resolves wildcards (`VISUALISE *`) against actual columns
4. Merges global mappings into each layer
5. Executes layer-specific queries (filters, stats)
6. Injects constant values as synthetic columns
7. Computes aesthetic labels from column names

**Arguments:**

- `query` - The full ggsql query string
- `reader` - A reader implementing the `Reader` trait

**Returns:**

- `Ok(Prepared)` - Ready for rendering
- `Err(GgsqlError)` - Parse, validation, or execution error

**Example:**

```rust
use ggsql::{prepare, reader::DuckDBReader, writer::VegaLiteWriter};

let reader = DuckDBReader::from_connection_string("duckdb://memory")?;
let prepared = prepare(
    "SELECT x, y FROM data VISUALISE x, y DRAW point",
    &reader
)?;

// Access metadata
println!("Rows: {}", prepared.metadata().rows);
println!("Columns: {:?}", prepared.metadata().columns);

// Render to Vega-Lite
let writer = VegaLiteWriter::new();
let result = prepared.render(&writer)?;
```

**Error Conditions:**

- Parse error in SQL or VISUALISE portion
- SQL execution failure
- Missing required aesthetics
- Invalid geom type
- Multiple VISUALISE statements (not yet supported)

---

### `validate`

```rust
pub fn validate(query: &str) -> Result<Validated>
```

Validate query syntax and semantics without executing SQL. This function combines query parsing and validation into a single operation.

**What is validated:**

- Syntax (parsing)
- Required aesthetics for each geom type
- Valid scale types (linear, log10, date, etc.)
- Valid coord types and properties
- Valid geom types
- Valid aesthetic names
- Valid SETTING parameters

**Arguments:**

- `query` - The full ggsql query string (SQL + VISUALISE)

**Returns:**

- `Ok(Validated)` - Validation results with query inspection methods
- `Err(GgsqlError)` - Internal error

**Example:**

```rust
use ggsql::validate;

let validated = validate("SELECT x, y FROM data VISUALISE x, y DRAW point")?;

// Check validity
if !validated.valid() {
    for error in validated.errors() {
        eprintln!("Error: {}", error.message);
    }
}

// Inspect query structure
if validated.has_visual() {
    println!("SQL: {}", validated.sql());
    println!("Visual: {}", validated.visual());
}
```

**Notes:**

- Does not execute SQL
- Does not resolve wildcards or global mappings
- Cannot validate column existence (requires data)
- Returns all errors, not just the first one
- CST available via `tree()` for advanced inspection

---

## Type Reference

### `Validated`

Result of validating a query (syntax + semantics, no SQL execution).

```rust
pub struct Validated {
    // All fields private
}
```

**Methods:**

| Method       | Signature                                    | Description                        |
| ------------ | -------------------------------------------- | ---------------------------------- |
| `has_visual` | `fn has_visual(&self) -> bool`               | Whether query contains VISUALISE   |
| `sql`        | `fn sql(&self) -> &str`                      | The SQL portion (before VISUALISE) |
| `visual`     | `fn visual(&self) -> &str`                   | The VISUALISE portion (raw text)   |
| `tree`       | `fn tree(&self) -> Option<&Tree>`            | CST for advanced inspection        |
| `valid`      | `fn valid(&self) -> bool`                    | Whether query is valid             |
| `errors`     | `fn errors(&self) -> &[ValidationError]`     | Validation errors                  |
| `warnings`   | `fn warnings(&self) -> &[ValidationWarning]` | Validation warnings                |

**Example:**

```rust
let validated = ggsql::validate("SELECT 1 as x VISUALISE DRAW point MAPPING x AS x, y AS y")?;

// Check validity
if !validated.valid() {
    for error in validated.errors() {
        eprintln!("Error: {}", error.message);
    }
}

// Inspect query structure
assert!(validated.has_visual());
assert_eq!(validated.sql(), "SELECT 1 as x");
assert!(validated.visual().starts_with("VISUALISE"));

// CST access for advanced use cases
if let Some(tree) = validated.tree() {
    println!("Root node: {}", tree.root_node().kind());
}
```

---

### `Prepared`

Result of preparing a visualization, ready for rendering.

#### Rendering Methods

| Method   | Signature                                                 | Description             |
| -------- | --------------------------------------------------------- | ----------------------- |
| `render` | `fn render(&self, writer: &dyn Writer) -> Result<String>` | Render to output format |

**Example:**

```rust
let writer = VegaLiteWriter::new();
let json = prepared.render(&writer)?;
println!("{}", json);
```

#### Plot Access Methods

| Method        | Signature                        | Description                     |
| ------------- | -------------------------------- | ------------------------------- |
| `plot`        | `fn plot(&self) -> &Plot`        | Get resolved plot specification |
| `layer_count` | `fn layer_count(&self) -> usize` | Number of layers                |

**Example:**

```rust
println!("Layers: {}", prepared.layer_count());

let plot = prepared.plot();
for (i, layer) in plot.layers.iter().enumerate() {
    println!("Layer {}: {:?}", i, layer.geom);
}
```

#### Metadata Methods

| Method     | Signature                         | Description                |
| ---------- | --------------------------------- | -------------------------- |
| `metadata` | `fn metadata(&self) -> &Metadata` | Get visualization metadata |

**Example:**

```rust
let meta = prepared.metadata();
println!("Rows: {}", meta.rows);
println!("Columns: {:?}", meta.columns);
println!("Layer count: {}", meta.layer_count);
```

#### Data Access Methods

| Method       | Signature                                              | Description                     |
| ------------ | ------------------------------------------------------ | ------------------------------- |
| `data`       | `fn data(&self) -> Option<&DataFrame>`                 | Global data (main query result) |
| `layer_data` | `fn layer_data(&self, i: usize) -> Option<&DataFrame>` | Layer-specific data             |
| `stat_data`  | `fn stat_data(&self, i: usize) -> Option<&DataFrame>`  | Stat transform results          |
| `data_map`   | `fn data_map(&self) -> &HashMap<String, DataFrame>`    | Raw data map access             |

**Example:**

```rust
// Global data
if let Some(df) = prepared.data() {
    println!("Global data: {} rows", df.height());
}

// Layer-specific data (from FILTER or FROM clause)
if let Some(df) = prepared.layer_data(0) {
    println!("Layer 0 has filtered data: {} rows", df.height());
}

// Stat data (histogram bins, density estimates, etc.)
if let Some(df) = prepared.stat_data(1) {
    println!("Layer 1 stat data: {} rows", df.height());
}
```

#### Query Introspection Methods

| Method      | Signature                                       | Description                      |
| ----------- | ----------------------------------------------- | -------------------------------- |
| `sql`       | `fn sql(&self) -> &str`                         | Main SQL query that was executed |
| `visual`    | `fn visual(&self) -> &str`                      | Raw VISUALISE text               |
| `layer_sql` | `fn layer_sql(&self, i: usize) -> Option<&str>` | Layer filter/source query        |
| `stat_sql`  | `fn stat_sql(&self, i: usize) -> Option<&str>`  | Stat transform query             |

**Example:**

```rust
// Main query
println!("SQL: {}", prepared.sql());
println!("Visual: {}", prepared.visual());

// Per-layer queries
for i in 0..prepared.layer_count() {
    if let Some(sql) = prepared.layer_sql(i) {
        println!("Layer {} filter: {}", i, sql);
    }
    if let Some(sql) = prepared.stat_sql(i) {
        println!("Layer {} stat: {}", i, sql);
    }
}
```

#### Warnings Method

| Method     | Signature                                    | Description                          |
| ---------- | -------------------------------------------- | ------------------------------------ |
| `warnings` | `fn warnings(&self) -> &[ValidationWarning]` | Validation warnings from preparation |

**Example:**

```rust
let prepared = ggsql::prepare(query, &reader)?;

// Check for warnings
if !prepared.warnings().is_empty() {
    for warning in prepared.warnings() {
        eprintln!("Warning: {}", warning.message);
    }
}

// Continue with rendering
let json = prepared.render(&writer)?;
```

---

### `Metadata`

Information about the prepared visualization.

```rust
pub struct Metadata {
    pub rows: usize,           // Rows in primary data source
    pub columns: Vec<String>,  // Column names
    pub layer_count: usize,    // Number of layers in the plot
}
```

---

### `ValidationError`

A validation error (fatal issue).

```rust
pub struct ValidationError {
    pub message: String,
    pub location: Option<Location>,
}
```

---

### `ValidationWarning`

A validation warning (non-fatal issue).

```rust
pub struct ValidationWarning {
    pub message: String,
    pub location: Option<Location>,
}
```

---

### `Location`

Location within a query string.

```rust
pub struct Location {
    pub line: usize,    // 0-based line number
    pub column: usize,  // 0-based column number
}
```

---

## Reader Trait & Implementations

### `Reader` Trait

```rust
pub trait Reader {
    /// Execute a SQL query and return a DataFrame
    fn execute(&self, sql: &str) -> Result<DataFrame>;

    /// Register a DataFrame as a queryable table
    fn register(&mut self, name: &str, df: DataFrame) -> Result<()>;

    /// Check if this reader supports DataFrame registration
    fn supports_register(&self) -> bool;
}
```

---

## Writer Trait & Implementations

### `Writer` Trait

```rust
pub trait Writer {
    /// Render a plot specification to output format
    fn write(&self, spec: &Plot, data: &HashMap<String, DataFrame>) -> Result<String>;

    /// Get the file extension for this writer's output
    fn file_extension(&self) -> &str;
}
```

## Python Bindings

The Python bindings provide the same two-stage API with Pythonic conventions.

### Classes

#### `DuckDBReader`

```python
class DuckDBReader:
    def __init__(self, connection: str) -> None:
        """Create a DuckDB reader.

        Args:
            connection: Connection string (e.g., "duckdb://memory")
        """

    def register(self, name: str, df: Any) -> None:
        """Register a DataFrame as a queryable table.

        Args:
            name: Table name
            df: Polars DataFrame or narwhals-compatible DataFrame
        """

    def execute(self, sql: str) -> polars.DataFrame:
        """Execute SQL and return a Polars DataFrame."""

    def supports_register(self) -> bool:
        """Check if registration is supported."""
```

#### `VegaLiteWriter`

```python
class VegaLiteWriter:
    def __init__(self) -> None:
        """Create a Vega-Lite writer."""
```

#### `Validated`

```python
class Validated:
    def has_visual(self) -> bool:
        """Check if query has VISUALISE clause."""

    def sql(self) -> str:
        """Get the SQL portion."""

    def visual(self) -> str:
        """Get the VISUALISE portion."""

    def valid(self) -> bool:
        """Check if query is valid."""

    def errors(self) -> list[dict]:
        """Get validation errors as list of dicts with 'message', 'location'."""

    def warnings(self) -> list[dict]:
        """Get validation warnings as list of dicts with 'message', 'location'."""

    # Note: tree() not exposed (tree-sitter nodes are Rust-only)
```

#### `Prepared`

```python
class Prepared:
    def render(self, writer: VegaLiteWriter) -> str:
        """Render to output format."""

    def metadata(self) -> dict:
        """Get metadata as dict with keys: rows, columns, layer_count."""

    def sql(self) -> str:
        """Get the main SQL query."""

    def visual(self) -> str:
        """Get the VISUALISE text."""

    def layer_count(self) -> int:
        """Get number of layers."""

    def warnings(self) -> list[dict]:
        """Get validation warnings as list of dicts with 'message', 'location'."""

    def data(self) -> polars.DataFrame | None:
        """Get global data."""

    def layer_data(self, index: int) -> polars.DataFrame | None:
        """Get layer-specific data."""

    def stat_data(self, index: int) -> polars.DataFrame | None:
        """Get stat transform data."""

    def layer_sql(self, index: int) -> str | None:
        """Get layer filter query."""

    def stat_sql(self, index: int) -> str | None:
        """Get stat transform query."""
```

### Functions

```python
def validate(query: str) -> Validated:
    """Validate query syntax and semantics.

    Returns Validated object with query inspection and validation methods.
    """

def prepare(query: str, reader: DuckDBReader) -> Prepared:
    """Prepare a query for visualization."""

def split_query(query: str) -> tuple[str, str]:
    """Split query into (sql, visualise) portions."""
```
