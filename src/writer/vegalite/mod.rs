//! Vega-Lite JSON writer implementation
//!
//! Converts ggsql specifications and DataFrames into Vega-Lite JSON format
//! for web-based interactive visualizations.
//!
//! # Mapping Strategy
//!
//! - ggsql Geom -> Vega-Lite mark type
//! - ggsql aesthetics -> Vega-Lite encoding channels
//! - ggsql layers -> Vega-Lite layer composition
//! - Polars DataFrame -> Vega-Lite inline data
//!
//! # Example
//!
//! ```rust,ignore
//! use ggsql::writer::{Writer, VegaLiteWriter};
//!
//! let writer = VegaLiteWriter::new();
//! let vega_json = writer.write(&spec, &dataframe)?;
//! // Can be rendered in browser with vega-embed
//! ```

mod coord;
mod data;
mod encoding;
mod renderer;

use crate::plot::layer::geom::GeomAesthetics;
// ArrayElement is used in tests and for pattern matching; suppress unused import warning
#[allow(unused_imports)]
use crate::plot::ArrayElement;
use crate::plot::ParameterValue;
use crate::writer::Writer;
use crate::{naming, AestheticValue, DataFrame, GgsqlError, Plot, Result};
use serde_json::{json, Map, Value};
use std::collections::HashMap;

// Re-export submodule functions for use in write()
use coord::apply_coord_transforms;
use data::{collect_binned_columns, is_binned_aesthetic, unify_datasets};
use encoding::{
    build_detail_encoding, build_encoding_channel, infer_field_type, map_aesthetic_name,
};
use renderer::{geom_to_mark, get_renderer, validate_layer_columns, PreparedData};

/// Conversion factor from points to pixels (CSS standard: 96 DPI, 72 points/inch)
/// 1 point = 96/72 pixels = 1.333
const POINTS_TO_PIXELS: f64 = 96.0 / 72.0;

/// Conversion factor from radius (in points) to area (in square pixels)
/// Used for size aesthetic: area = pi * r^2 where r is in pixels
/// So: area_px^2 = pi * (r_pt * POINTS_TO_PIXELS)^2 = pi * r_pt^2 * (96/72)^2
const POINTS_TO_AREA: f64 = std::f64::consts::PI * POINTS_TO_PIXELS * POINTS_TO_PIXELS;

/// Vega-Lite JSON writer
///
/// Generates Vega-Lite v6 specifications from ggsql specs and data.
pub struct VegaLiteWriter {
    /// Vega-Lite schema version
    schema: String,
}

impl VegaLiteWriter {
    /// Create a new Vega-Lite writer with default settings
    pub fn new() -> Self {
        Self {
            schema: "https://vega.github.io/schema/vega-lite/v6.json".to_string(),
        }
    }
}

impl Default for VegaLiteWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl Writer for VegaLiteWriter {
    type Output = String;

    fn write(&self, spec: &Plot, data: &HashMap<String, DataFrame>) -> Result<String> {
        // Validate spec before processing
        self.validate(spec)?;

        // Determine which dataset key each layer should use
        // Use layer.data_key if set (from execute.rs), otherwise use standard layer key
        let layer_data_keys: Vec<String> = spec
            .layers
            .iter()
            .enumerate()
            .map(|(idx, layer)| {
                layer
                    .data_key
                    .clone()
                    .unwrap_or_else(|| naming::layer_key(idx))
            })
            .collect();

        // Validate all required datasets exist and validate column references
        for (layer_idx, (layer, key)) in spec.layers.iter().zip(layer_data_keys.iter()).enumerate()
        {
            let df = data.get(key).ok_or_else(|| {
                GgsqlError::WriterError(format!(
                    "Missing data source '{}' for layer {}",
                    key,
                    layer_idx + 1
                ))
            })?;
            validate_layer_columns(layer, df, layer_idx)?;
        }

        // Build the base Vega-Lite spec
        let mut vl_spec = json!({
            "$schema": self.schema
        });

        // Responsive plot sizing
        vl_spec["width"] = json!("container");
        vl_spec["height"] = json!("container");

        // Add title if present
        if let Some(labels) = &spec.labels {
            if let Some(title) = labels.labels.get("title") {
                vl_spec["title"] = json!(title);
            }
        }

        // Collect binned column information from spec
        let binned_columns = collect_binned_columns(spec);

        // Build individual datasets using renderers
        // Each renderer handles its own data preparation (standard or composite)
        let mut individual_datasets = Map::new();
        let mut layer_renderers: Vec<Box<dyn renderer::GeomRenderer>> = Vec::new();
        let mut prepared_data: Vec<PreparedData> = Vec::new();

        for (layer_idx, layer) in spec.layers.iter().enumerate() {
            let data_key = &layer_data_keys[layer_idx];
            let df = data.get(data_key).ok_or_else(|| {
                GgsqlError::WriterError(format!(
                    "Missing data source '{}' for layer {}",
                    data_key,
                    layer_idx + 1
                ))
            })?;

            // Get the appropriate renderer for this geom type
            let renderer = get_renderer(&layer.geom);

            // Prepare data using the renderer (handles both standard and composite cases)
            let prepared = renderer.prepare_data(df, data_key, &binned_columns)?;

            // Add data to individual datasets based on prepared type
            match &prepared {
                PreparedData::Single { values } => {
                    individual_datasets.insert(data_key.clone(), json!(values));
                }
                PreparedData::Composite { components, .. } => {
                    // For composite geoms (boxplot, etc.), add each component dataset
                    // with type-specific keys (e.g., "__ggsql_layer_0__lower_whisker")
                    for (component_name, values) in components {
                        let type_key = format!("{}{}", data_key, component_name);
                        individual_datasets.insert(type_key, json!(values));
                    }
                }
            }

            layer_renderers.push(renderer);
            prepared_data.push(prepared);
        }

        // Unify all datasets into a single dataset with source identification
        // Each row gets a __ggsql_source__ field identifying which layer it belongs to
        let unified_data = unify_datasets(&individual_datasets)?;

        // Set data directly on the spec (single unified dataset)
        vl_spec["data"] = json!({"values": unified_data});

        // Build layers array
        // Each layer gets a filter transform to select its data from the unified dataset
        let mut layers = Vec::new();
        for (layer_idx, layer) in spec.layers.iter().enumerate() {
            let data_key = &layer_data_keys[layer_idx];
            let df = data.get(data_key).unwrap();
            let renderer = &layer_renderers[layer_idx];
            let prepared = &prepared_data[layer_idx];

            // Layer spec
            let mut layer_spec = json!({
                "mark": geom_to_mark(&layer.geom)
            });

            // Build transform array for this layer
            // Always starts with a filter to select this layer's data from unified dataset
            let mut transforms: Vec<Value> = Vec::new();

            // Add source filter transform (if the renderer needs it)
            // Composite geoms like boxplot add their own type-specific filters
            if renderer.needs_source_filter() {
                transforms.push(json!({
                    "filter": {
                        "field": naming::SOURCE_COLUMN,
                        "equal": data_key
                    }
                }));
            }

            // Set transform array on layer spec
            layer_spec["transform"] = json!(transforms);

            // Build encoding for this layer
            // Track which aesthetic families have been titled to ensure only one title per family
            let mut encoding = Map::new();
            let mut titled_families: std::collections::HashSet<String> =
                std::collections::HashSet::new();

            // Collect primary aesthetics that exist in the layer (for title handling)
            // e.g., if layer has "y", then "ymin" and "ymax" should suppress their titles
            let primary_aesthetics: std::collections::HashSet<String> = layer
                .mappings
                .aesthetics
                .keys()
                .filter(|a| GeomAesthetics::primary_aesthetic(a) == a.as_str())
                .cloned()
                .collect();

            for (aesthetic, value) in &layer.mappings.aesthetics {
                let channel_name = map_aesthetic_name(aesthetic);
                let channel_encoding = build_encoding_channel(
                    aesthetic,
                    value,
                    df,
                    spec,
                    &mut titled_families,
                    &primary_aesthetics,
                )?;
                encoding.insert(channel_name, channel_encoding);

                // For binned positional aesthetics (x, y), add x2/y2 channel with bin_end column
                // This enables proper bin width rendering in Vega-Lite
                if matches!(aesthetic.as_str(), "x" | "y") && is_binned_aesthetic(aesthetic, spec) {
                    if let AestheticValue::Column { name: col, .. } = value {
                        let end_col = naming::bin_end_column(col);
                        let end_channel = format!("{}2", aesthetic); // "x2" or "y2"
                        encoding.insert(end_channel, json!({"field": end_col}));
                    }
                }
            }

            // Also add aesthetic parameters from SETTING as literal encodings
            // (e.g., SETTING color => 'red' becomes {"color": {"value": "red"}})
            // Only parameters that are supported aesthetics for this geom type are included
            let supported_aesthetics = layer.geom.aesthetics().supported;
            for (param_name, param_value) in &layer.parameters {
                if supported_aesthetics.contains(&param_name.as_str()) {
                    let channel_name = map_aesthetic_name(param_name);
                    // Only add if not already set by MAPPING (MAPPING takes precedence)
                    if !encoding.contains_key(&channel_name) {
                        // Convert size and linewidth from points to Vega-Lite units
                        let converted_value = match (param_name.as_str(), param_value) {
                            // Size: interpret as radius in points, convert to area in pixels^2
                            ("size", ParameterValue::Number(n)) => json!(n * n * POINTS_TO_AREA),
                            // Linewidth: interpret as width in points, convert to pixels
                            ("linewidth", ParameterValue::Number(n)) => json!(n * POINTS_TO_PIXELS),
                            // Other aesthetics: pass through unchanged
                            _ => param_value.to_json(),
                        };
                        encoding.insert(channel_name, json!({"value": converted_value}));
                    }
                }
            }

            // Add detail encoding for partition_by columns (grouping)
            if let Some(detail) = build_detail_encoding(&layer.partition_by) {
                encoding.insert("detail".to_string(), detail);
            }

            // Apply geom-specific encoding modifications via renderer
            renderer.modify_encoding(&mut encoding, layer)?;

            layer_spec["encoding"] = Value::Object(encoding);

            // Apply geom-specific spec modifications via renderer
            renderer.modify_spec(&mut layer_spec, layer)?;

            // Finalize the layer (may expand into multiple layers for composite geoms)
            let final_layers = renderer.finalize(layer_spec, layer, data_key, prepared)?;
            layers.extend(final_layers);
        }

        vl_spec["layer"] = json!(layers);

        // Apply coordinate transforms (flip, polar, cartesian limits)
        // This must happen AFTER layers are built since transforms modify layer encodings
        let first_df = data.get(&layer_data_keys[0]).unwrap();
        apply_coord_transforms(spec, first_df, &mut vl_spec)?;

        // Handle faceting if present
        if let Some(facet) = &spec.facet {
            // Use the unified global dataset for faceting
            let facet_data = data.get(&layer_data_keys[0]).unwrap();

            use crate::plot::Facet;
            match facet {
                Facet::Wrap { variables, .. } => {
                    if !variables.is_empty() {
                        let field_type = infer_field_type(facet_data, &variables[0]);
                        vl_spec["facet"] = json!({
                            "field": variables[0],
                            "type": field_type,
                        });

                        // Move layer into spec (data reference stays at top level)
                        let mut spec_inner = json!({});
                        if let Some(layer) = vl_spec.get("layer") {
                            spec_inner["layer"] = layer.clone();
                        }

                        vl_spec["spec"] = spec_inner;
                        vl_spec.as_object_mut().unwrap().remove("layer");
                    }
                }
                Facet::Grid { rows, cols, .. } => {
                    let mut facet_spec = Map::new();
                    if !rows.is_empty() {
                        let field_type = infer_field_type(facet_data, &rows[0]);
                        facet_spec.insert(
                            "row".to_string(),
                            json!({"field": rows[0], "type": field_type}),
                        );
                    }
                    if !cols.is_empty() {
                        let field_type = infer_field_type(facet_data, &cols[0]);
                        facet_spec.insert(
                            "column".to_string(),
                            json!({"field": cols[0], "type": field_type}),
                        );
                    }
                    vl_spec["facet"] = Value::Object(facet_spec);

                    // Move layer into spec (data reference stays at top level)
                    let mut spec_inner = json!({});
                    if let Some(layer) = vl_spec.get("layer") {
                        spec_inner["layer"] = layer.clone();
                    }

                    vl_spec["spec"] = spec_inner;
                    vl_spec.as_object_mut().unwrap().remove("layer");
                }
            }
        }

        serde_json::to_string_pretty(&vl_spec).map_err(|e| {
            GgsqlError::WriterError(format!("Failed to serialize Vega-Lite JSON: {}", e))
        })
    }

    fn validate(&self, spec: &Plot) -> Result<()> {
        // Check that we have at least one layer
        if spec.layers.is_empty() {
            return Err(GgsqlError::ValidationError(
                "VegaLiteWriter requires at least one layer".to_string(),
            ));
        }

        // Validate each layer
        for layer in &spec.layers {
            // Check required aesthetics
            layer.validate_required_aesthetics().map_err(|e| {
                GgsqlError::ValidationError(format!("Layer validation failed: {}", e))
            })?;

            // Check SETTING parameters are valid for this geom
            layer.validate_settings().map_err(|e| {
                GgsqlError::ValidationError(format!("Layer validation failed: {}", e))
            })?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plot::{Labels, Layer, ParameterValue};
    use crate::Geom;
    use polars::prelude::*;
    use std::collections::HashMap;

    // Re-export test functions from submodules
    use super::data::find_bin_for_value;
    use super::encoding::infer_field_type;
    use super::renderer::geom_to_mark;

    /// Helper to wrap a DataFrame in a data map for testing (uses layer 0 key)
    fn wrap_data(df: DataFrame) -> HashMap<String, DataFrame> {
        wrap_data_for_layers(df, 1)
    }

    /// Helper to wrap a DataFrame for multiple layers (clones for each layer)
    fn wrap_data_for_layers(df: DataFrame, num_layers: usize) -> HashMap<String, DataFrame> {
        let mut data_map = HashMap::new();
        for i in 0..num_layers {
            data_map.insert(naming::layer_key(i), df.clone());
        }
        data_map
    }

    #[test]
    fn test_geom_to_mark_mapping() {
        // All marks should be objects with type and clip: true
        assert_eq!(
            geom_to_mark(&Geom::point()),
            json!({"type": "point", "clip": true})
        );
        assert_eq!(
            geom_to_mark(&Geom::line()),
            json!({"type": "line", "clip": true})
        );
        assert_eq!(
            geom_to_mark(&Geom::bar()),
            json!({"type": "bar", "clip": true})
        );
        assert_eq!(
            geom_to_mark(&Geom::area()),
            json!({"type": "area", "clip": true})
        );
        assert_eq!(
            geom_to_mark(&Geom::tile()),
            json!({"type": "rect", "clip": true})
        );
    }

    #[test]
    fn test_aesthetic_name_mapping() {
        // Pass-through aesthetics (including fill and stroke for separate color control)
        assert_eq!(map_aesthetic_name("x"), "x");
        assert_eq!(map_aesthetic_name("y"), "y");
        assert_eq!(map_aesthetic_name("color"), "color");
        assert_eq!(map_aesthetic_name("fill"), "fill");
        assert_eq!(map_aesthetic_name("stroke"), "stroke");
        assert_eq!(map_aesthetic_name("opacity"), "opacity");
        assert_eq!(map_aesthetic_name("size"), "size");
        assert_eq!(map_aesthetic_name("shape"), "shape");
        // Mapped aesthetics
        assert_eq!(map_aesthetic_name("linetype"), "strokeDash");
        assert_eq!(map_aesthetic_name("linewidth"), "strokeWidth");
        assert_eq!(map_aesthetic_name("label"), "text");
    }

    #[test]
    fn test_validation_requires_layers() {
        let writer = VegaLiteWriter::new();
        let spec = Plot::new();
        assert!(writer.validate(&spec).is_err());
    }

    #[test]
    fn test_simple_point_spec() {
        let writer = VegaLiteWriter::new();

        // Create a simple spec
        let mut spec = Plot::new();
        let layer = Layer::new(Geom::point())
            .with_aesthetic(
                "x".to_string(),
                AestheticValue::standard_column("x".to_string()),
            )
            .with_aesthetic(
                "y".to_string(),
                AestheticValue::standard_column("y".to_string()),
            );
        spec.layers.push(layer);

        // Create simple DataFrame
        let df = df! {
            "x" => &[1, 2, 3],
            "y" => &[4, 5, 6],
        }
        .unwrap();

        // Generate Vega-Lite JSON
        let json_str = writer.write(&spec, &wrap_data(df)).unwrap();
        let vl_spec: Value = serde_json::from_str(&json_str).unwrap();

        // Verify structure (uses layer array with inline data)
        assert_eq!(vl_spec["$schema"], writer.schema);
        assert!(vl_spec["layer"].is_array());
        assert_eq!(vl_spec["layer"][0]["mark"]["type"], "point");
        assert_eq!(vl_spec["layer"][0]["mark"]["clip"], true);
        assert!(vl_spec["data"]["values"].is_array());
        assert_eq!(vl_spec["data"]["values"].as_array().unwrap().len(), 3);
        assert!(vl_spec["layer"][0]["encoding"]["x"].is_object());
        assert!(vl_spec["layer"][0]["encoding"]["y"].is_object());
    }

    #[test]
    fn test_with_title() {
        let writer = VegaLiteWriter::new();

        let mut spec = Plot::new();
        let layer = Layer::new(Geom::line())
            .with_aesthetic(
                "x".to_string(),
                AestheticValue::standard_column("date".to_string()),
            )
            .with_aesthetic(
                "y".to_string(),
                AestheticValue::standard_column("value".to_string()),
            );
        spec.layers.push(layer);

        let mut labels = Labels {
            labels: HashMap::new(),
        };
        labels
            .labels
            .insert("title".to_string(), "My Chart".to_string());
        spec.labels = Some(labels);

        let df = df! {
            "date" => &["2024-01-01", "2024-01-02"],
            "value" => &[10, 20],
        }
        .unwrap();

        let json_str = writer.write(&spec, &wrap_data(df)).unwrap();
        let vl_spec: Value = serde_json::from_str(&json_str).unwrap();

        assert_eq!(vl_spec["title"], "My Chart");
        assert_eq!(vl_spec["layer"][0]["mark"]["type"], "line");
        assert_eq!(vl_spec["layer"][0]["mark"]["clip"], true);
    }

    #[test]
    fn test_literal_color() {
        let writer = VegaLiteWriter::new();

        let mut spec = Plot::new();
        let layer = Layer::new(Geom::point())
            .with_aesthetic(
                "x".to_string(),
                AestheticValue::standard_column("x".to_string()),
            )
            .with_aesthetic(
                "y".to_string(),
                AestheticValue::standard_column("y".to_string()),
            )
            .with_aesthetic(
                "color".to_string(),
                AestheticValue::Literal(ParameterValue::String("red".to_string())),
            );
        spec.layers.push(layer);

        let df = df! {
            "x" => &[1, 2, 3],
            "y" => &[4, 5, 6],
        }
        .unwrap();

        let json_str = writer.write(&spec, &wrap_data(df)).unwrap();
        let vl_spec: Value = serde_json::from_str(&json_str).unwrap();

        assert_eq!(vl_spec["layer"][0]["encoding"]["color"]["value"], "red");
    }

    #[test]
    fn test_missing_column_error() {
        let writer = VegaLiteWriter::new();

        let mut spec = Plot::new();
        let layer = Layer::new(Geom::point())
            .with_aesthetic(
                "x".to_string(),
                AestheticValue::standard_column("x".to_string()),
            )
            .with_aesthetic(
                "y".to_string(),
                AestheticValue::standard_column("nonexistent".to_string()),
            );
        spec.layers.push(layer);

        let df = df! {
            "x" => &[1, 2, 3],
            "y" => &[4, 5, 6],
        }
        .unwrap();

        let result = writer.write(&spec, &wrap_data(df));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("nonexistent"));
        assert!(err.to_string().contains("does not exist"));
    }

    #[test]
    fn test_numeric_type_inference_integers() {
        let df = df! {
            "x" => &[1i64, 2, 3],
        }
        .unwrap();

        assert_eq!(infer_field_type(&df, "x"), "quantitative");
    }

    #[test]
    fn test_nominal_type_inference_strings() {
        let df = df! {
            "category" => &["A", "B", "C"],
        }
        .unwrap();

        assert_eq!(infer_field_type(&df, "category"), "nominal");
    }

    #[test]
    fn test_numeric_string_type_inference() {
        let df = df! {
            "numbers_as_strings" => &["1.5", "2.5", "3.5"],
        }
        .unwrap();

        assert_eq!(infer_field_type(&df, "numbers_as_strings"), "quantitative");
    }

    #[test]
    fn test_find_bin_for_value() {
        let breaks = vec![0.0, 10.0, 20.0, 30.0];

        // Test value in first bin [0, 10)
        assert_eq!(find_bin_for_value(5.0, &breaks), Some((0.0, 10.0)));

        // Test value at bin boundary (belongs to next bin due to half-open interval)
        assert_eq!(find_bin_for_value(10.0, &breaks), Some((10.0, 20.0)));

        // Test value in middle bin [10, 20)
        assert_eq!(find_bin_for_value(15.0, &breaks), Some((10.0, 20.0)));

        // Test value in last bin [20, 30] (closed interval)
        assert_eq!(find_bin_for_value(25.0, &breaks), Some((20.0, 30.0)));

        // Test value at last edge (should be included in last bin)
        assert_eq!(find_bin_for_value(30.0, &breaks), Some((20.0, 30.0)));

        // Test value outside range
        assert_eq!(find_bin_for_value(-5.0, &breaks), None);
        assert_eq!(find_bin_for_value(35.0, &breaks), None);

        // Test with too few breaks
        assert_eq!(find_bin_for_value(5.0, &[10.0]), None);
    }

    #[test]
    fn test_multi_layer_composition() {
        let writer = VegaLiteWriter::new();

        let mut spec = Plot::new();

        // Add line layer
        let line_layer = Layer::new(Geom::line())
            .with_aesthetic(
                "x".to_string(),
                AestheticValue::standard_column("x".to_string()),
            )
            .with_aesthetic(
                "y".to_string(),
                AestheticValue::standard_column("y".to_string()),
            );
        spec.layers.push(line_layer);

        // Add point layer
        let point_layer = Layer::new(Geom::point())
            .with_aesthetic(
                "x".to_string(),
                AestheticValue::standard_column("x".to_string()),
            )
            .with_aesthetic(
                "y".to_string(),
                AestheticValue::standard_column("y".to_string()),
            );
        spec.layers.push(point_layer);

        let df = df! {
            "x" => &[1, 2, 3],
            "y" => &[4, 5, 6],
        }
        .unwrap();

        let json_str = writer.write(&spec, &wrap_data_for_layers(df, 2)).unwrap();
        let vl_spec: Value = serde_json::from_str(&json_str).unwrap();

        // Should have layer array with 2 layers
        assert!(vl_spec["layer"].is_array());
        assert_eq!(vl_spec["layer"].as_array().unwrap().len(), 2);
        assert_eq!(vl_spec["layer"][0]["mark"]["type"], "line");
        assert_eq!(vl_spec["layer"][1]["mark"]["type"], "point");
    }

    #[test]
    fn test_build_symbol_legend_label_mapping_basic() {
        // Test the build_symbol_legend_label_mapping function directly
        use super::encoding::build_symbol_legend_label_mapping;

        let breaks = vec![
            ArrayElement::Number(0.0),
            ArrayElement::Number(25.0),
            ArrayElement::Number(50.0),
            ArrayElement::Number(75.0),
            ArrayElement::Number(100.0),
        ];

        let mut label_mapping = HashMap::new();
        label_mapping.insert("0".to_string(), Some("Low".to_string()));
        label_mapping.insert("25".to_string(), Some("Medium".to_string()));
        label_mapping.insert("50".to_string(), Some("High".to_string()));
        label_mapping.insert("75".to_string(), Some("Very High".to_string()));
        label_mapping.insert("100".to_string(), Some("Max".to_string())); // Will be excluded

        let result = build_symbol_legend_label_mapping(&breaks, &label_mapping, "left");

        // VL generates: "0 – 25", "25 – 50", "50 – 75", "≥ 75"
        // We map to range format using custom labels: "lower_label – upper_label"
        assert_eq!(
            result.get("0 – 25"),
            Some(&Some("Low – Medium".to_string()))
        );
        assert_eq!(
            result.get("25 – 50"),
            Some(&Some("Medium – High".to_string()))
        );
        assert_eq!(
            result.get("50 – 75"),
            Some(&Some("High – Very High".to_string()))
        );
        assert_eq!(
            result.get("≥ 75"),
            Some(&Some("Very High – Max".to_string()))
        );

        // Should not include a mapping for the last terminal value directly
        assert!(result.get("100").is_none());
    }

    #[test]
    fn test_symbol_legend_label_expr_uses_range_format() {
        // Test that symbol legend labelExpr maps VL's range labels to our labels
        use crate::plot::scale::Scale;
        use crate::plot::{ArrayElement, ParameterValue, ScaleType};

        let writer = VegaLiteWriter::new();

        let mut spec = Plot::new();
        let layer = Layer::new(Geom::point())
            .with_aesthetic(
                "x".to_string(),
                AestheticValue::standard_column("x".to_string()),
            )
            .with_aesthetic(
                "y".to_string(),
                AestheticValue::standard_column("y".to_string()),
            )
            .with_aesthetic(
                "color".to_string(),
                AestheticValue::standard_column("value".to_string()),
            );
        spec.layers.push(layer);

        // Add binned color scale (symbol legend case)
        let mut scale = Scale::new("color");
        scale.scale_type = Some(ScaleType::binned());
        scale.properties.insert(
            "breaks".to_string(),
            ParameterValue::Array(vec![
                ArrayElement::Number(0.0),
                ArrayElement::Number(25.0),
                ArrayElement::Number(50.0),
                ArrayElement::Number(75.0),
                ArrayElement::Number(100.0),
            ]),
        );
        // Add label renaming
        let mut labels = HashMap::new();
        labels.insert("0".to_string(), Some("Low".to_string()));
        labels.insert("25".to_string(), Some("Medium".to_string()));
        labels.insert("50".to_string(), Some("High".to_string()));
        labels.insert("75".to_string(), Some("Very High".to_string()));
        scale.label_mapping = Some(labels);
        spec.scales.push(scale);

        let df = df! {
            "x" => &[1, 2, 3],
            "y" => &[10, 45, 80],
            "value" => &[10.0, 45.0, 80.0],
        }
        .unwrap();

        let json_str = writer.write(&spec, &wrap_data(df)).unwrap();
        let vl_spec: Value = serde_json::from_str(&json_str).unwrap();

        // Check that labelExpr contains VL's range-style format
        let label_expr = &vl_spec["layer"][0]["encoding"]["color"]["legend"]["labelExpr"];
        assert!(label_expr.is_string());
        let expr = label_expr.as_str().unwrap();

        // Should contain mappings for VL's range format labels to our range format
        assert!(
            expr.contains("0 – 25"),
            "labelExpr should contain VL's range format '0 – 25', got: {}",
            expr
        );
        assert!(
            expr.contains("'Low – Medium'"),
            "labelExpr should map to 'Low – Medium', got: {}",
            expr
        );
        assert!(
            expr.contains("≥ 75"),
            "labelExpr should contain VL's last bin format '≥ 75', got: {}",
            expr
        );
        // Note: last bin maps "≥ 75" to "Very High – 100" (no custom label for 100 in this test)
        assert!(
            expr.contains("'Very High"),
            "labelExpr should contain 'Very High', got: {}",
            expr
        );
    }

    #[test]
    fn test_symbol_legend_open_format_with_oob_squish() {
        // Test that oob='squish' produces open format labels for symbol legends
        use super::encoding::build_symbol_legend_label_mapping;

        let breaks = vec![
            ArrayElement::Number(0.0),
            ArrayElement::Number(25.0),
            ArrayElement::Number(50.0),
            ArrayElement::Number(75.0),
            ArrayElement::Number(100.0),
        ];

        // Suppress first and last terminals (oob='squish' behavior)
        let mut label_mapping = HashMap::new();
        label_mapping.insert("0".to_string(), None); // Suppressed
        label_mapping.insert("25".to_string(), Some("Medium".to_string()));
        label_mapping.insert("50".to_string(), Some("High".to_string()));
        label_mapping.insert("75".to_string(), Some("Very High".to_string()));
        label_mapping.insert("100".to_string(), None); // Suppressed

        // Test with closed='left' (default)
        let result_left = build_symbol_legend_label_mapping(&breaks, &label_mapping, "left");

        // First bin: suppressed lower terminal → "< 25" (open format)
        assert_eq!(
            result_left.get("0 – 25"),
            Some(&Some("< Medium".to_string())),
            "First bin with suppressed lower should use '< upper' format"
        );
        // Last bin: suppressed upper terminal → "≥ 75" (open format, same as normal)
        assert_eq!(
            result_left.get("≥ 75"),
            Some(&Some("≥ Very High".to_string())),
            "Last bin with suppressed upper should use '≥ lower' format"
        );

        // Test with closed='right'
        let result_right = build_symbol_legend_label_mapping(&breaks, &label_mapping, "right");

        // First bin: suppressed lower terminal → "≤ 25" (right-closed means upper included)
        assert_eq!(
            result_right.get("0 – 25"),
            Some(&Some("≤ Medium".to_string())),
            "First bin with closed='right' should use '≤ upper' format"
        );
        // Last bin: suppressed upper terminal → "> 75" (right-closed means lower not included)
        assert_eq!(
            result_right.get("≥ 75"),
            Some(&Some("> Very High".to_string())),
            "Last bin with closed='right' should use '> lower' format"
        );
    }
}
