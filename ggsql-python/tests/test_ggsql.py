"""Tests for ggsql Python bindings.

These tests focus on Python-specific logic:
- DataFrame conversion
- New API: reader.execute() -> writer.render_json()
- NoVisualiseError handling
- Two-stage API (execute -> render)

Rust logic (parsing, Vega-Lite generation) is tested in the Rust test suite.
"""

import json

import pytest
import polars as pl
import altair

import ggsql


class TestValidate:
    """Tests for validate() function."""

    def test_valid_query_with_visualise(self):
        validated = ggsql.validate(
            "SELECT 1 AS x, 2 AS y VISUALISE DRAW point MAPPING x AS x, y AS y"
        )
        assert validated.has_visual()
        assert validated.valid()
        assert "SELECT" in validated.sql()
        assert "VISUALISE" in validated.visual()
        assert len(validated.errors()) == 0

    def test_valid_query_without_visualise(self):
        validated = ggsql.validate("SELECT 1 AS x, 2 AS y")
        assert not validated.has_visual()
        assert validated.valid()
        assert validated.sql() == "SELECT 1 AS x, 2 AS y"
        assert validated.visual() == ""

    def test_invalid_query_has_errors(self):
        validated = ggsql.validate("SELECT 1 VISUALISE DRAW invalid_geom")
        assert not validated.valid()
        assert len(validated.errors()) > 0

    def test_missing_required_aesthetic(self):
        # Point requires x and y, only providing x
        validated = ggsql.validate(
            "SELECT 1 AS x, 2 AS y VISUALISE DRAW point MAPPING x AS x"
        )
        assert not validated.valid()
        errors = validated.errors()
        assert len(errors) > 0
        assert any("y" in e["message"] for e in errors)


class TestDuckDB:
    """Tests for DuckDB class."""

    def test_create_in_memory(self):
        reader = ggsql.readers.DuckDB("duckdb://memory")
        assert reader is not None

    def test_execute_sql_simple_query(self):
        reader = ggsql.readers.DuckDB("duckdb://memory")
        df = reader.execute_sql("SELECT 1 AS x, 2 AS y")
        assert isinstance(df, pl.DataFrame)
        assert df.shape == (1, 2)
        assert list(df.columns) == ["x", "y"]

    def test_register_and_query(self):
        reader = ggsql.readers.DuckDB("duckdb://memory")
        df = pl.DataFrame({"x": [1, 2, 3], "y": [10, 20, 30]})
        reader.register("my_data", df)

        result = reader.execute_sql("SELECT * FROM my_data WHERE x > 1")
        assert isinstance(result, pl.DataFrame)
        assert result.shape == (2, 2)

    def test_unregister(self):
        reader = ggsql.readers.DuckDB("duckdb://memory")
        df = pl.DataFrame({"x": [1, 2, 3]})
        reader.register("test_table", df)

        # Table should exist
        result = reader.execute_sql("SELECT * FROM test_table")
        assert result.shape[0] == 3

        # Unregister
        reader.unregister("test_table")

        # Table should no longer exist
        with pytest.raises(ggsql.types.ReaderError):
            reader.execute_sql("SELECT * FROM test_table")

    def test_unregister_nonexistent_silent(self):
        """Unregistering a non-existent table should not raise."""
        reader = ggsql.readers.DuckDB("duckdb://memory")
        # Should not raise
        reader.unregister("nonexistent_table")

    def test_invalid_connection_string(self):
        with pytest.raises(ggsql.types.ReaderError):
            ggsql.readers.DuckDB("invalid://connection")


class TestVegaLite:
    """Tests for VegaLite class."""

    def test_create_writer(self):
        writer = ggsql.writers.VegaLite()
        assert writer is not None


class TestExecute:
    """Tests for reader.execute() method."""

    def test_execute_simple_query(self):
        reader = ggsql.readers.DuckDB("duckdb://memory")
        prepared = reader.execute("SELECT 1 AS x, 2 AS y VISUALISE x, y DRAW point")
        assert prepared is not None
        assert prepared.layer_count() == 1

    def test_execute_with_data_dict(self):
        reader = ggsql.readers.DuckDB("duckdb://memory")
        df = pl.DataFrame({"x": [1, 2, 3], "y": [10, 20, 30]})

        prepared = reader.execute(
            "SELECT * FROM data VISUALISE x, y DRAW point", {"data": df}
        )
        assert prepared.metadata()["rows"] == 3

    def test_execute_with_multiple_tables(self):
        reader = ggsql.readers.DuckDB("duckdb://memory")
        sales = pl.DataFrame({"id": [1, 2], "product_id": [1, 1]})
        products = pl.DataFrame({"id": [1], "name": ["Widget"]})

        prepared = reader.execute(
            """
            SELECT s.id, p.name FROM sales s
            JOIN products p ON s.product_id = p.id
            VISUALISE id AS x, name AS color DRAW bar
            """,
            {"sales": sales, "products": products},
        )
        assert prepared.metadata()["rows"] == 2

    def test_execute_tables_unregistered_after(self):
        """Tables should be unregistered after execute()."""
        reader = ggsql.readers.DuckDB("duckdb://memory")
        df = pl.DataFrame({"x": [1, 2, 3], "y": [10, 20, 30]})

        # Execute with data dict
        reader.execute("SELECT * FROM data VISUALISE x, y DRAW point", {"data": df})

        # Table should no longer exist
        with pytest.raises(ggsql.types.ReaderError):
            reader.execute_sql("SELECT * FROM data")

    def test_execute_tables_unregistered_on_error(self):
        """Tables should be unregistered even if execute() fails."""
        reader = ggsql.readers.DuckDB("duckdb://memory")
        df = pl.DataFrame({"x": [1, 2, 3]})  # Missing 'y' column

        # This should fail because we reference 'y' which doesn't exist
        with pytest.raises(ggsql.types.ValidationError):
            reader.execute(
                "SELECT * FROM data VISUALISE x, y DRAW point", {"data": df}
            )

        # Table should still be unregistered
        with pytest.raises(ggsql.types.ReaderError):
            reader.execute_sql("SELECT * FROM data")

    def test_execute_metadata(self):
        reader = ggsql.readers.DuckDB("duckdb://memory")
        prepared = reader.execute(
            "SELECT * FROM (VALUES (1, 10), (2, 20), (3, 30)) AS t(x, y) "
            "VISUALISE x, y DRAW point"
        )

        metadata = prepared.metadata()
        assert metadata["rows"] == 3
        assert "x" in metadata["columns"]
        assert "y" in metadata["columns"]
        assert metadata["layer_count"] == 1

    def test_execute_sql_accessor(self):
        reader = ggsql.readers.DuckDB("duckdb://memory")
        prepared = reader.execute("SELECT 1 AS x, 2 AS y VISUALISE x, y DRAW point")
        assert "SELECT" in prepared.sql()

    def test_execute_visual_accessor(self):
        reader = ggsql.readers.DuckDB("duckdb://memory")
        prepared = reader.execute("SELECT 1 AS x, 2 AS y VISUALISE x, y DRAW point")
        assert "VISUALISE" in prepared.visual()

    def test_execute_data_accessor(self):
        reader = ggsql.readers.DuckDB("duckdb://memory")
        prepared = reader.execute("SELECT 1 AS x, 2 AS y VISUALISE x, y DRAW point")
        data = prepared.data()
        assert isinstance(data, pl.DataFrame)
        assert data.shape == (1, 2)


class TestNoVisualiseError:
    """Tests for NoVisualiseError exception."""

    def test_execute_without_visualise_raises(self):
        reader = ggsql.readers.DuckDB("duckdb://memory")
        with pytest.raises(ggsql.types.NoVisualiseError):
            reader.execute("SELECT 1 AS x, 2 AS y")

    def test_novisualise_error_message(self):
        reader = ggsql.readers.DuckDB("duckdb://memory")
        with pytest.raises(ggsql.types.NoVisualiseError) as exc_info:
            reader.execute("SELECT 1 AS x, 2 AS y")
        assert "VISUALISE" in str(exc_info.value)
        assert "execute_sql" in str(exc_info.value)

    def test_novisualise_error_is_exception(self):
        """NoVisualiseError should be a proper exception type."""
        assert issubclass(ggsql.types.NoVisualiseError, Exception)


class TestWriterRender:
    """Tests for VegaLite.render_json() method."""

    def test_render_to_vegalite(self):
        reader = ggsql.readers.DuckDB("duckdb://memory")
        prepared = reader.execute("SELECT 1 AS x, 2 AS y VISUALISE x, y DRAW point")
        writer = ggsql.writers.VegaLite()

        result = writer.render_json(prepared)
        assert isinstance(result, str)

        spec = json.loads(result)
        assert "$schema" in spec
        assert "vega-lite" in spec["$schema"]

    def test_render_contains_data(self):
        reader = ggsql.readers.DuckDB("duckdb://memory")
        df = pl.DataFrame({"x": [1, 2, 3], "y": [10, 20, 30]})

        prepared = reader.execute(
            "SELECT * FROM data VISUALISE x, y DRAW point", {"data": df}
        )
        writer = ggsql.writers.VegaLite()

        result = writer.render_json(prepared)
        spec = json.loads(result)
        # Data should be in the spec (either inline or in datasets)
        assert "data" in spec or "datasets" in spec

    def test_render_multi_layer(self):
        reader = ggsql.readers.DuckDB("duckdb://memory")
        prepared = reader.execute(
            "SELECT * FROM (VALUES (1, 10), (2, 20)) AS t(x, y) "
            "VISUALISE "
            "DRAW point MAPPING x AS x, y AS y "
            "DRAW line MAPPING x AS x, y AS y"
        )
        writer = ggsql.writers.VegaLite()

        result = writer.render_json(prepared)
        spec = json.loads(result)
        assert "layer" in spec


class TestWriterRenderChart:
    """Tests for VegaLite.render_chart() method."""

    def test_render_chart_returns_altair(self):
        reader = ggsql.readers.DuckDB("duckdb://memory")
        prepared = reader.execute("SELECT 1 AS x, 2 AS y VISUALISE x, y DRAW point")
        writer = ggsql.writers.VegaLite()

        chart = writer.render_chart(prepared)
        assert isinstance(chart, altair.TopLevelMixin)

    def test_render_chart_layer_chart(self):
        """Simple DRAW specs produce LayerChart (ggsql always wraps in layer)."""
        reader = ggsql.readers.DuckDB("duckdb://memory")
        prepared = reader.execute("SELECT 1 AS x, 2 AS y VISUALISE x, y DRAW point")
        writer = ggsql.writers.VegaLite()

        chart = writer.render_chart(prepared)
        # ggsql wraps all charts in a layer
        assert isinstance(chart, altair.LayerChart)

    def test_render_chart_can_serialize(self):
        reader = ggsql.readers.DuckDB("duckdb://memory")
        df = pl.DataFrame({"x": [1, 2, 3], "y": [10, 20, 30]})
        prepared = reader.execute(
            "SELECT * FROM data VISUALISE x, y DRAW point", {"data": df}
        )
        writer = ggsql.writers.VegaLite()

        chart = writer.render_chart(prepared)
        # Should not raise
        json_str = chart.to_json()
        assert len(json_str) > 0

    def test_render_chart_faceted(self):
        """FACET WRAP specs produce FacetChart."""
        reader = ggsql.readers.DuckDB("duckdb://memory")
        df = pl.DataFrame(
            {
                "x": [1, 2, 3, 4, 5, 6],
                "y": [10, 20, 30, 40, 50, 60],
                "group": ["A", "A", "A", "B", "B", "B"],
            }
        )
        # Need validate=False because ggsql produces v6 specs
        prepared = reader.execute(
            "SELECT * FROM data VISUALISE x, y FACET WRAP group DRAW point",
            {"data": df},
        )
        writer = ggsql.writers.VegaLite()

        chart = writer.render_chart(prepared)
        assert isinstance(chart, altair.FacetChart)


class TestTwoStageAPIIntegration:
    """Integration tests for the two-stage execute -> render API."""

    def test_end_to_end_workflow(self):
        """Complete workflow: create reader, execute with data, render."""
        # Create reader
        reader = ggsql.readers.DuckDB("duckdb://memory")

        # Create data
        df = pl.DataFrame(
            {
                "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
                "value": [10, 20, 30],
                "region": ["North", "South", "North"],
            }
        )

        # Execute visualization
        prepared = reader.execute(
            "SELECT * FROM sales VISUALISE date AS x, value AS y, region AS color DRAW line",
            {"sales": df},
        )

        # Verify metadata
        assert prepared.metadata()["rows"] == 3
        assert prepared.layer_count() == 1

        # Render to Vega-Lite
        writer = ggsql.writers.VegaLite()
        result = writer.render_json(prepared)

        # Verify output
        spec = json.loads(result)
        assert "$schema" in spec
        assert "line" in json.dumps(spec)

    def test_can_introspect_prepared(self):
        """Test all introspection methods on Prepared."""
        reader = ggsql.readers.DuckDB("duckdb://memory")
        prepared = reader.execute("SELECT 1 AS x, 2 AS y VISUALISE x, y DRAW point")

        # All these should work without error
        assert prepared.sql() is not None
        assert prepared.visual() is not None
        assert prepared.layer_count() >= 1
        assert prepared.metadata() is not None
        assert prepared.data() is not None
        assert prepared.warnings() is not None

        # Layer-specific accessors (may return None)
        _ = prepared.layer_data(0)
        _ = prepared.stat_data(0)
        _ = prepared.layer_sql(0)
        _ = prepared.stat_sql(0)

    def test_visualise_from_shorthand(self):
        """Test VISUALISE FROM syntax."""
        reader = ggsql.readers.DuckDB("duckdb://memory")
        df = pl.DataFrame({"x": [1, 2, 3], "y": [10, 20, 30]})

        prepared = reader.execute(
            "VISUALISE FROM data DRAW point MAPPING x AS x, y AS y", {"data": df}
        )
        assert prepared.metadata()["rows"] == 3

    def test_render_chart_workflow(self):
        """Test workflow using render_chart()."""
        reader = ggsql.readers.DuckDB("duckdb://memory")
        df = pl.DataFrame({"x": [1, 2, 3], "y": [10, 20, 30]})

        spec = reader.execute(
            "SELECT * FROM data VISUALISE x, y DRAW point", {"data": df}
        )
        writer = ggsql.writers.VegaLite()
        chart = writer.render_chart(spec)

        # Should be able to convert to dict
        spec_dict = chart.to_dict()
        assert "layer" in spec_dict


class TestVersionInfo:
    """Tests for version information."""

    def test_version_string(self):
        """__version__ should be a string."""
        assert isinstance(ggsql.__version__, str)
        assert ggsql.__version__ == "0.1.0"

    def test_version_info_tuple(self):
        """version_info should be a tuple."""
        assert hasattr(ggsql, "version_info")
        assert isinstance(ggsql.version_info, tuple)
        assert ggsql.version_info == (0, 1, 0)


class TestReprMethods:
    """Tests for __repr__ methods."""

    def test_duckdb_repr(self):
        """DuckDB should have a useful repr."""
        reader = ggsql.readers.DuckDB("duckdb://memory")
        repr_str = repr(reader)
        assert "DuckDB" in repr_str
        assert "duckdb://memory" in repr_str

    def test_vegalite_repr(self):
        """VegaLite should have a useful repr."""
        writer = ggsql.writers.VegaLite()
        repr_str = repr(writer)
        assert "VegaLite" in repr_str

    def test_validated_repr(self):
        """Validated should have a useful repr."""
        validated = ggsql.validate("SELECT 1 AS x VISUALISE x DRAW point")
        repr_str = repr(validated)
        assert "Validated" in repr_str
        assert "valid=" in repr_str

    def test_prepared_repr(self):
        """Prepared should have a useful repr."""
        reader = ggsql.readers.DuckDB("duckdb://memory")
        prepared = reader.execute("SELECT 1 AS x, 2 AS y VISUALISE x, y DRAW point")
        repr_str = repr(prepared)
        assert "Prepared" in repr_str
        assert "rows=" in repr_str
        assert "layers=" in repr_str


class TestNarwhalsSupport:
    """Tests for narwhals DataFrame support."""

    def test_execute_with_pandas_dataframe(self):
        """execute() should accept pandas DataFrames."""
        import pandas as pd

        reader = ggsql.readers.DuckDB("duckdb://memory")
        df = pd.DataFrame({"x": [1, 2, 3], "y": [10, 20, 30]})

        prepared = reader.execute(
            "SELECT * FROM data VISUALISE x, y DRAW point", {"data": df}
        )
        assert prepared.metadata()["rows"] == 3

    def test_register_with_pandas_dataframe(self):
        """register() should accept pandas DataFrames."""
        import pandas as pd

        reader = ggsql.readers.DuckDB("duckdb://memory")
        df = pd.DataFrame({"x": [1, 2, 3], "y": [10, 20, 30]})

        reader.register("my_data", df)
        result = reader.execute_sql("SELECT * FROM my_data")
        assert result.shape == (3, 2)

    def test_execute_with_polars_dataframe(self):
        """execute() should still work with polars DataFrames."""
        reader = ggsql.readers.DuckDB("duckdb://memory")
        df = pl.DataFrame({"x": [1, 2, 3], "y": [10, 20, 30]})

        prepared = reader.execute(
            "SELECT * FROM data VISUALISE x, y DRAW point", {"data": df}
        )
        assert prepared.metadata()["rows"] == 3


class TestRenderJsonMethod:
    """Tests for render_json() method."""

    def test_render_json_returns_json(self):
        """render_json() should return a valid JSON string."""
        reader = ggsql.readers.DuckDB("duckdb://memory")
        prepared = reader.execute("SELECT 1 AS x, 2 AS y VISUALISE x, y DRAW point")
        writer = ggsql.writers.VegaLite()

        result = writer.render_json(prepared)
        assert isinstance(result, str)

        spec = json.loads(result)
        assert "$schema" in spec


class TestContextManager:
    """Tests for context manager protocol."""

    def test_context_manager_basic(self):
        """DuckDB should work as context manager."""
        with ggsql.readers.DuckDB("duckdb://memory") as reader:
            df = reader.execute_sql("SELECT 1 AS x, 2 AS y")
            assert df.shape == (1, 2)

    def test_context_manager_with_execute(self):
        """execute() should work inside context manager."""
        df = pl.DataFrame({"x": [1, 2, 3], "y": [10, 20, 30]})

        with ggsql.readers.DuckDB("duckdb://memory") as reader:
            prepared = reader.execute(
                "SELECT * FROM data VISUALISE x, y DRAW point", {"data": df}
            )
            assert prepared.metadata()["rows"] == 3


class TestExceptionHierarchy:
    """Tests for exception type hierarchy."""

    def test_ggsql_error_is_base(self):
        """All exceptions should inherit from GgsqlError."""
        assert issubclass(ggsql.types.ParseError, ggsql.types.GgsqlError)
        assert issubclass(ggsql.types.ValidationError, ggsql.types.GgsqlError)
        assert issubclass(ggsql.types.ReaderError, ggsql.types.GgsqlError)
        assert issubclass(ggsql.types.WriterError, ggsql.types.GgsqlError)
        assert issubclass(ggsql.types.NoVisualiseError, ggsql.types.GgsqlError)

    def test_ggsql_error_is_exception(self):
        """GgsqlError should be a proper exception type."""
        assert issubclass(ggsql.types.GgsqlError, Exception)

    def test_catch_all_ggsql_errors(self):
        """Should be able to catch all errors with GgsqlError."""
        reader = ggsql.readers.DuckDB("duckdb://memory")

        # This should raise ReaderError (missing table)
        with pytest.raises(ggsql.types.GgsqlError):
            reader.execute_sql("SELECT * FROM nonexistent_table")

    def test_reader_error_for_sql_failure(self):
        """ReaderError should be raised for SQL execution failures."""
        reader = ggsql.readers.DuckDB("duckdb://memory")

        with pytest.raises(ggsql.types.ReaderError):
            reader.execute_sql("SELECT * FROM nonexistent_table")

    def test_validation_error_for_missing_column(self):
        """ValidationError should be raised for missing column references."""
        reader = ggsql.readers.DuckDB("duckdb://memory")
        df = pl.DataFrame({"x": [1, 2, 3]})  # Missing 'y' column

        with pytest.raises(ggsql.types.ValidationError):
            reader.execute(
                "SELECT * FROM data VISUALISE x, y DRAW point", {"data": df}
            )
