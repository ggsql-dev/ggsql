"""End-to-end tests for altair integration.

These tests verify that ggsql output can be loaded into altair
and produces valid, renderable charts.
"""

import pytest
import polars as pl

altair = pytest.importorskip("altair", minversion="5.0")

import ggsql


class TestAltairChartFromGgsql:
    """Test that ggsql output can be loaded into altair charts."""

    def test_point_chart_loads(self):
        """Test that a simple point chart can be loaded into altair."""
        df = pl.DataFrame({"x": [1, 2, 3], "y": [10, 20, 30]})
        vl_json = ggsql.render(df, "VISUALISE x, y DRAW point")

        chart = altair.Chart.from_json(vl_json)

        assert chart is not None
        assert isinstance(chart, altair.TopLevelMixin)

    def test_line_chart_loads(self):
        """Test that a line chart can be loaded into altair."""
        df = pl.DataFrame({
            "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "value": [100, 120, 110]
        })
        vl_json = ggsql.render(df, "VISUALISE date AS x, value AS y DRAW line")

        chart = altair.Chart.from_json(vl_json)

        assert chart is not None

    def test_bar_chart_loads(self):
        """Test that a bar chart can be loaded into altair."""
        df = pl.DataFrame({
            "category": ["A", "B", "C"],
            "count": [25, 40, 15]
        })
        vl_json = ggsql.render(df, "VISUALISE category AS x, count AS y DRAW bar")

        chart = altair.Chart.from_json(vl_json)

        assert chart is not None

    def test_chart_with_color_encoding(self):
        """Test that color encoding works correctly."""
        df = pl.DataFrame({
            "x": [1, 2, 3, 1, 2, 3],
            "y": [10, 20, 30, 15, 25, 35],
            "group": ["A", "A", "A", "B", "B", "B"]
        })
        vl_json = ggsql.render(df, "VISUALISE x, y, group AS color DRAW point")

        chart = altair.Chart.from_json(vl_json)
        spec = chart.to_dict()

        # ggsql uses layer structure - encoding is inside layer
        assert "layer" in spec
        layer = spec["layer"][0]
        assert "encoding" in layer
        assert "color" in layer["encoding"]

    def test_multi_layer_chart_loads(self):
        """Test that multi-layer charts load correctly."""
        df = pl.DataFrame({"x": [1, 2, 3], "y": [10, 20, 30]})
        vl_json = ggsql.render(df, "VISUALISE x, y DRAW line DRAW point")

        chart = altair.Chart.from_json(vl_json)
        spec = chart.to_dict()

        # Multi-layer charts should have a layer array
        assert "layer" in spec
        assert len(spec["layer"]) == 2


class TestAltairChartValidation:
    """Test that ggsql output passes altair validation."""

    def test_schema_validation_passes(self):
        """Test that the output validates against Vega-Lite schema."""
        df = pl.DataFrame({"x": [1, 2, 3], "y": [10, 20, 30]})
        vl_json = ggsql.render(df, "VISUALISE x, y DRAW point")

        # from_json with validate=True (default) will raise if invalid
        chart = altair.Chart.from_json(vl_json, validate=True)

        assert chart is not None

    def test_chart_with_title_validates(self):
        """Test that charts with titles validate correctly."""
        df = pl.DataFrame({"x": [1, 2, 3], "y": [10, 20, 30]})
        vl_json = ggsql.render(
            df,
            "VISUALISE x, y DRAW point LABEL title => 'My Chart'"
        )

        chart = altair.Chart.from_json(vl_json, validate=True)
        spec = chart.to_dict()

        assert spec.get("title") == "My Chart"

    def test_chart_with_axis_labels_validates(self):
        """Test that charts with axis labels validate correctly."""
        df = pl.DataFrame({"x": [1, 2, 3], "y": [10, 20, 30]})
        vl_json = ggsql.render(
            df,
            "VISUALISE x, y DRAW point LABEL x => 'X Axis', y => 'Y Axis'"
        )

        chart = altair.Chart.from_json(vl_json, validate=True)

        assert chart is not None


class TestAltairChartStructure:
    """Test the structure of charts produced by ggsql.

    Note: ggsql always uses a layer structure, so mark and encoding
    are found inside spec["layer"][0] rather than at the top level.
    """

    def _get_mark(self, spec):
        """Helper to extract mark from ggsql's layer structure."""
        if "layer" in spec and spec["layer"]:
            layer = spec["layer"][0]
            mark = layer.get("mark")
            if isinstance(mark, dict):
                return mark.get("type")
            return mark
        return spec.get("mark")

    def test_point_mark_type(self):
        """Test that point charts have correct mark type."""
        df = pl.DataFrame({"x": [1, 2, 3], "y": [10, 20, 30]})
        vl_json = ggsql.render(df, "VISUALISE x, y DRAW point")

        chart = altair.Chart.from_json(vl_json)
        spec = chart.to_dict()

        assert self._get_mark(spec) == "point"

    def test_line_mark_type(self):
        """Test that line charts have correct mark type."""
        df = pl.DataFrame({"x": [1, 2, 3], "y": [10, 20, 30]})
        vl_json = ggsql.render(df, "VISUALISE x, y DRAW line")

        chart = altair.Chart.from_json(vl_json)
        spec = chart.to_dict()

        assert self._get_mark(spec) == "line"

    def test_bar_mark_type(self):
        """Test that bar charts have correct mark type."""
        df = pl.DataFrame({"x": ["A", "B", "C"], "y": [10, 20, 30]})
        vl_json = ggsql.render(df, "VISUALISE x, y DRAW bar")

        chart = altair.Chart.from_json(vl_json)
        spec = chart.to_dict()

        assert self._get_mark(spec) == "bar"

    def test_encoding_fields_present(self):
        """Test that encoding fields are correctly set."""
        df = pl.DataFrame({"x_col": [1, 2, 3], "y_col": [10, 20, 30]})
        vl_json = ggsql.render(df, "VISUALISE x_col AS x, y_col AS y DRAW point")

        chart = altair.Chart.from_json(vl_json)
        spec = chart.to_dict()

        # ggsql uses layer structure - encoding is inside layer
        assert "layer" in spec
        layer = spec["layer"][0]
        assert "encoding" in layer
        assert "x" in layer["encoding"]
        assert "y" in layer["encoding"]
        assert layer["encoding"]["x"]["field"] == "x_col"
        assert layer["encoding"]["y"]["field"] == "y_col"

    def test_data_embedded_in_spec(self):
        """Test that data is embedded in the spec."""
        df = pl.DataFrame({"x": [1, 2, 3], "y": [10, 20, 30]})
        vl_json = ggsql.render(df, "VISUALISE x, y DRAW point")

        chart = altair.Chart.from_json(vl_json)
        spec = chart.to_dict()

        # Data should be in 'datasets' (ggsql style) or 'data'
        has_data = "datasets" in spec or ("data" in spec and "values" in spec.get("data", {}))
        assert has_data


class TestAltairRoundTrip:
    """Test round-trip conversion through altair."""

    def test_to_dict_and_back(self):
        """Test that chart can be converted to dict and back."""
        df = pl.DataFrame({"x": [1, 2, 3], "y": [10, 20, 30]})
        vl_json = ggsql.render(df, "VISUALISE x, y DRAW point")

        chart1 = altair.Chart.from_json(vl_json)
        spec_dict = chart1.to_dict()
        chart2 = altair.Chart.from_dict(spec_dict)

        assert chart2 is not None
        assert chart1.to_dict() == chart2.to_dict()

    def test_to_json_and_back(self):
        """Test that chart can be converted to JSON and back."""
        df = pl.DataFrame({"x": [1, 2, 3], "y": [10, 20, 30]})
        vl_json = ggsql.render(df, "VISUALISE x, y DRAW point")

        chart1 = altair.Chart.from_json(vl_json)
        json_str = chart1.to_json()
        chart2 = altair.Chart.from_json(json_str)

        assert chart2 is not None
