# ggsql

Python bindings for [ggsql](https://github.com/georgestagg/ggsql), a SQL extension for declarative data visualization.

This package provides a thin wrapper around the Rust `ggsql` crate, enabling Python users to render Vega-Lite visualizations from polars DataFrames using ggsql's VISUALISE syntax.

## Installation

### From PyPI (when published)

```bash
pip install ggsql
```

### From source

Building from source requires:
- Rust toolchain (install via [rustup](https://rustup.rs/))
- Python 3.10+
- [maturin](https://github.com/PyO3/maturin)

```bash
# Clone the monorepo
git clone https://github.com/georgestagg/ggsql.git
cd ggsql/ggsql-python

# Create a virtual environment
python -m venv .venv
source .venv/bin/activate  # or `.venv\Scripts\activate` on Windows

# Install build dependencies
pip install maturin

# Build and install in development mode
maturin develop

# Or build a wheel
maturin build --release
pip install target/wheels/ggsql-*.whl
```

## Usage

```python
import ggsql
import polars as pl

# Split a ggSQL query into SQL and VISUALISE portions
sql, viz = ggsql.split_query("""
    SELECT date, revenue, region FROM sales
    WHERE year = 2024
    VISUALISE date AS x, revenue AS y, region AS color
    DRAW line
    LABEL title => 'Sales Trends'
""")

# Execute SQL with your preferred tool
import duckdb
df = duckdb.sql(sql).pl()

# Render DataFrame + VISUALISE spec to Vega-Lite JSON
vegalite_json = ggsql.render(df, viz)
```

### Mapping styles

The `render()` function supports various mapping styles:

```python
df = pl.DataFrame({"x": [1, 2, 3], "y": [10, 20, 30], "category": ["A", "B", "A"]})

# Explicit mapping
ggsql.render(df, "VISUALISE x AS x, y AS y DRAW point")

# Implicit mapping (column name = aesthetic name)
ggsql.render(df, "VISUALISE x, y DRAW point")

# Wildcard mapping (map all matching columns)
ggsql.render(df, "VISUALISE * DRAW point")

# With color encoding
ggsql.render(df, "VISUALISE x, y, category AS color DRAW point")
```

## API

### `split_query(query: str) -> tuple[str, str]`

Split a ggSQL query into SQL and VISUALISE portions.

**Parameters:**
- `query`: The full ggSQL query string

**Returns:**
- Tuple of `(sql_portion, visualise_portion)`

**Raises:**
- `ValueError`: If the query cannot be parsed

### `render(df, viz, *, writer="vegalite") -> str`

Render a DataFrame with a VISUALISE specification.

**Parameters:**
- `df`: A `polars.DataFrame` or `polars.LazyFrame` (LazyFrames are collected automatically)
- `viz`: The VISUALISE specification string
- `writer`: Output format, currently only `"vegalite"` is supported

**Returns:**
- JSON string containing the Vega-Lite specification

**Raises:**
- `ValueError`: If the spec cannot be parsed or rendered

## Development

### Keeping in sync with the monorepo

The `ggsql-python` package is part of the [ggsql monorepo](https://github.com/georgestagg/ggsql) and depends on the Rust `ggsql` crate via a path dependency. When the Rust crate is updated, you may need to rebuild:

```bash
cd ggsql-python

# Rebuild after Rust changes
maturin develop

# If tree-sitter grammar changed, clean and rebuild
cd .. && cargo clean -p tree-sitter-ggsql && cd ggsql-python
maturin develop
```

### Running tests

```bash
# Install test dependencies
pip install pytest altair

# Run unit tests
pytest tests/test_ggsql.py -v

# Run E2E tests with altair
pytest tests/test_altair_e2e.py -v

# Run all tests
pytest tests/ -v
```

## Requirements

- Python >= 3.10
- polars >= 1.0
- pyarrow >= 12 (required for Arrow FFI)

## License

MIT
