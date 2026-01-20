import json
import pytest
import polars as pl
import ggsql


def test_split_query_basic():
    sql, viz = ggsql.split_query("""
        SELECT date, value FROM sales
        VISUALISE date AS x, value AS y
        DRAW line
    """)
    assert "SELECT" in sql
    assert "VISUALISE" in viz
    assert "DRAW line" in viz


def test_split_query_no_visualise():
    sql, viz = ggsql.split_query("SELECT * FROM data WHERE x > 5")
    assert sql == "SELECT * FROM data WHERE x > 5"
    assert viz == ""


def test_split_query_invalid_raises():
    # Test that malformed VISUALISE FROM without semicolon raises
    with pytest.raises(ValueError):
        ggsql.split_query("CREATE TABLE x VISUALISE FROM x")


def test_render_simple():
    df = pl.DataFrame({"x": [1, 2, 3], "y": [10, 20, 30]})
    output = ggsql.render(df, "VISUALISE x, y DRAW point")
    spec = json.loads(output)
    assert spec["$schema"].startswith("https://vega.github.io/schema/vega-lite")
    assert "datasets" in spec


def test_render_lazyframe():
    lf = pl.LazyFrame({"x": [1, 2, 3], "y": [10, 20, 30]})
    output = ggsql.render(lf, "VISUALISE x, y DRAW point")
    spec = json.loads(output)
    assert "layer" in spec or "mark" in spec


def test_render_explicit_writer():
    df = pl.DataFrame({"x": [1, 2, 3], "y": [10, 20, 30]})
    output = ggsql.render(df, "VISUALISE x, y DRAW point", writer="vegalite")
    spec = json.loads(output)
    assert "$schema" in spec


def test_render_invalid_viz_raises():
    df = pl.DataFrame({"x": [1]})
    with pytest.raises(ValueError):
        ggsql.render(df, "NOT VALID SYNTAX")


def test_render_unknown_writer_raises():
    df = pl.DataFrame({"x": [1], "y": [2]})
    with pytest.raises(ValueError, match="Unknown writer"):
        ggsql.render(df, "VISUALISE x, y DRAW point", writer="unknown")


def test_render_wildcard_mapping():
    """Test that VISUALISE * resolves column names."""
    df = pl.DataFrame({"x": [1, 2], "y": [10, 20]})
    output = ggsql.render(df, "VISUALISE * DRAW point")
    spec = json.loads(output)
    # Should have resolved x and y from DataFrame columns
    assert "data" in spec or "datasets" in spec


def test_render_implicit_mapping():
    """Test that VISUALISE x, y resolves to x AS x, y AS y."""
    df = pl.DataFrame({"x": [1, 2], "y": [10, 20]})
    output = ggsql.render(df, "VISUALISE x, y DRAW point")
    spec = json.loads(output)
    encoding = spec.get("encoding", {})
    assert "x" in encoding or "layer" in spec


def test_render_with_labels():
    """Test that LABEL clause produces axis titles."""
    df = pl.DataFrame({"date": [1, 2], "revenue": [100, 200]})
    output = ggsql.render(
        df,
        "VISUALISE date AS x, revenue AS y DRAW line LABEL title => 'Sales', x => 'Date'"
    )
    spec = json.loads(output)
    assert spec.get("title") == "Sales"


def test_full_workflow():
    """Test the complete workflow: split, execute (mock), render."""
    # Full ggSQL query
    query = """
        SELECT 1 as x, 10 as y
        UNION ALL SELECT 2, 20
        UNION ALL SELECT 3, 30
        VISUALISE x, y
        DRAW line
        DRAW point
        LABEL title => 'Test Chart', x => 'X Axis', y => 'Y Axis'
    """

    # Split into SQL and viz
    sql, viz = ggsql.split_query(query)
    assert "SELECT" in sql
    assert "UNION ALL" in sql
    assert "VISUALISE" in viz

    # Simulate SQL execution (in real usage, user would use DuckDB/etc)
    df = pl.DataFrame({"x": [1, 2, 3], "y": [10, 20, 30]})

    # Render to Vega-Lite
    output = ggsql.render(df, viz)
    spec = json.loads(output)

    # Verify structure
    assert spec["$schema"].startswith("https://vega.github.io/schema/vega-lite")
    assert spec["title"] == "Test Chart"
    assert "data" in spec or "datasets" in spec
    assert len(spec.get("data", {}).get("values", [])) == 3 or "datasets" in spec

    # Should have 2 layers (line + point)
    assert "layer" in spec
    assert len(spec["layer"]) == 2
