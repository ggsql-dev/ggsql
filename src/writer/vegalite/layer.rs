//! Geom rendering for Vega-Lite writer
//!
//! This module provides:
//! - Basic geom-to-mark mapping and column validation
//! - A trait-based approach to rendering different ggsql geom types to Vega-Lite specs
//!
//! Each geom type can override specific phases of the rendering pipeline while using
//! sensible defaults for standard behavior.

use crate::plot::layer::geom::GeomType;
use crate::plot::ParameterValue;
use crate::{naming, AestheticValue, DataFrame, Geom, GgsqlError, Layer, Result};
use polars::prelude::ChunkCompareEq;
use serde_json::{json, Map, Value};
use std::any::Any;
use std::collections::HashMap;

use super::data::{dataframe_to_values, dataframe_to_values_with_bins};

// =============================================================================
// Basic Geom Utilities
// =============================================================================

/// Map ggsql Geom to Vega-Lite mark type
/// Always includes `clip: true` to ensure marks don't render outside plot bounds
pub fn geom_to_mark(geom: &Geom) -> Value {
    let mark_type = match geom.geom_type() {
        GeomType::Point => "point",
        GeomType::Line => "line",
        GeomType::Path => "line",
        GeomType::Bar => "bar",
        GeomType::Area => "area",
        GeomType::Tile => "rect",
        GeomType::Ribbon => "area",
        GeomType::Polygon => "line",
        GeomType::Histogram => "bar",
        GeomType::Density => "area",
        GeomType::Violin => "line",
        GeomType::Boxplot => "boxplot",
        GeomType::Text => "text",
        GeomType::Label => "text",
        _ => "point", // Default fallback
    };
    json!({
        "type": mark_type,
        "clip": true
    })
}

/// Validate column references for a single layer against its specific DataFrame
pub fn validate_layer_columns(layer: &Layer, data: &DataFrame, layer_idx: usize) -> Result<()> {
    let available_columns: Vec<String> = data
        .get_column_names()
        .iter()
        .map(|s| s.to_string())
        .collect();

    for (aesthetic, value) in &layer.mappings.aesthetics {
        if let AestheticValue::Column { name: col, .. } = value {
            if !available_columns.contains(col) {
                let source_desc = if let Some(src) = &layer.source {
                    format!(" (source: {})", src.as_str())
                } else {
                    " (global data)".to_string()
                };
                let display_col = naming::extract_aesthetic_name(col).unwrap_or(col.as_str());
                return Err(GgsqlError::ValidationError(format!(
                    "Column '{}' referenced in aesthetic '{}' (layer {}{}) does not exist.\nAvailable columns: {}",
                    display_col,
                    aesthetic,
                    layer_idx + 1,
                    source_desc,
                    available_columns.join(", ")
                )));
            }
        }
    }

    // Check partition_by columns
    for col in &layer.partition_by {
        if !available_columns.contains(col) {
            let source_desc = if let Some(src) = &layer.source {
                format!(" (source: {})", src.as_str())
            } else {
                " (global data)".to_string()
            };
            return Err(GgsqlError::ValidationError(format!(
                "Column '{}' referenced in PARTITION BY (layer {}{}) does not exist.\nAvailable columns: {}",
                col,
                layer_idx + 1,
                source_desc,
                available_columns.join(", ")
            )));
        }
    }

    Ok(())
}

// =============================================================================
// GeomRenderer Trait System
// =============================================================================

/// Data prepared for a layer - either single dataset or multiple components
pub enum PreparedData {
    /// Standard single dataset (most geoms)
    Single { values: Vec<Value> },
    /// Multiple component datasets (boxplot, violin, errorbar)
    Composite {
        components: HashMap<String, Vec<Value>>,
        metadata: Box<dyn Any + Send + Sync>,
    },
}

/// Trait for rendering ggsql geoms to Vega-Lite layers
///
/// Provides a three-phase rendering pipeline:
/// 1. **Data Preparation**: Convert DataFrame to JSON values
/// 2. **Encoding Modifications**: Apply geom-specific encoding transformations
/// 3. **Layer Output**: Finalize and potentially expand layers
///
/// Most geoms use the default implementations. Only geoms with special requirements
/// (bar width, path ordering, boxplot decomposition) need to override specific methods.
pub trait GeomRenderer: Send + Sync {
    // === Phase 1: Data Preparation ===

    /// Prepare data for this layer.
    /// Default: convert DataFrame to JSON values (single dataset)
    fn prepare_data(
        &self,
        df: &DataFrame,
        _data_key: &str,
        binned_columns: &HashMap<String, Vec<f64>>,
    ) -> Result<PreparedData> {
        let values = if binned_columns.is_empty() {
            dataframe_to_values(df)?
        } else {
            dataframe_to_values_with_bins(df, binned_columns)?
        };
        Ok(PreparedData::Single { values })
    }

    // === Phase 2: Encoding Modifications ===

    /// Modify the encoding map for this geom.
    /// Default: no modifications
    fn modify_encoding(&self, _encoding: &mut Map<String, Value>, _layer: &Layer) -> Result<()> {
        Ok(())
    }

    /// Modify the mark/layer spec for this geom.
    /// Default: no modifications
    fn modify_spec(&self, _layer_spec: &mut Value, _layer: &Layer) -> Result<()> {
        Ok(())
    }

    // === Phase 3: Layer Output ===

    /// Whether to add the standard source filter transform.
    /// Default: true (composite geoms override to return false)
    fn needs_source_filter(&self) -> bool {
        true
    }

    /// Finalize the layer(s) for output.
    /// Default: return single layer unchanged
    /// Composite geoms override to expand into multiple layers
    fn finalize(
        &self,
        layer_spec: Value,
        _layer: &Layer,
        _data_key: &str,
        _prepared: &PreparedData,
    ) -> Result<Vec<Value>> {
        Ok(vec![layer_spec])
    }
}

// =============================================================================
// Default Renderer (for geoms with no special handling)
// =============================================================================

/// Default renderer used for geoms with standard behavior
pub struct DefaultRenderer;

impl GeomRenderer for DefaultRenderer {}

// =============================================================================
// Bar Renderer
// =============================================================================

/// Renderer for bar geom - overrides mark spec for band width
pub struct BarRenderer;

impl GeomRenderer for BarRenderer {
    fn modify_spec(&self, layer_spec: &mut Value, layer: &Layer) -> Result<()> {
        let width = match layer.parameters.get("width") {
            Some(ParameterValue::Number(w)) => *w,
            _ => 0.9,
        };

        // For dodged bars, use expression-based width with the adjusted width
        // For non-dodged bars, use band-relative width
        let width_value = if let Some(adjusted) = layer.adjusted_width {
            // Use bandwidth expression for dodged bars
            json!({"expr": format!("bandwidth('x') * {}", adjusted)})
        } else {
            json!({"band": width})
        };

        layer_spec["mark"] = json!({
            "type": "bar",
            "width": width_value,
            "align": "center",
            "clip": true
        });
        Ok(())
    }
}

// =============================================================================
// Path Renderer
// =============================================================================

/// Renderer for path geom - adds order channel for natural data order
pub struct PathRenderer;

impl GeomRenderer for PathRenderer {
    fn modify_encoding(&self, encoding: &mut Map<String, Value>, _layer: &Layer) -> Result<()> {
        // Use the natural data order
        encoding.insert("order".to_string(), json!({"value": Value::Null}));
        Ok(())
    }
}

// =============================================================================
// Ribbon Renderer
// =============================================================================

/// Renderer for ribbon geom - remaps ymin/ymax to y/y2
pub struct RibbonRenderer;

impl GeomRenderer for RibbonRenderer {
    fn modify_encoding(&self, encoding: &mut Map<String, Value>, _layer: &Layer) -> Result<()> {
        if let Some(ymax) = encoding.remove("ymax") {
            encoding.insert("y".to_string(), ymax);
        }
        if let Some(ymin) = encoding.remove("ymin") {
            encoding.insert("y2".to_string(), ymin);
        }
        Ok(())
    }
}

// =============================================================================
// Area Renderer
// =============================================================================

/// Renderer for area geom - uses DefaultRenderer behavior
/// Stacking is handled by the position infrastructure via `position => 'stack'`
pub struct AreaRenderer;

impl GeomRenderer for AreaRenderer {}

// =============================================================================
// Polygon Renderer
// =============================================================================

/// Renderer for polygon geom - uses closed line with fill
pub struct PolygonRenderer;

impl GeomRenderer for PolygonRenderer {
    fn modify_encoding(&self, encoding: &mut Map<String, Value>, _layer: &Layer) -> Result<()> {
        // Polygon needs both `fill` and `stroke` independently, but map_aesthetic_name()
        // converts fill -> color (which works for most geoms). For closed line marks,
        // we need actual `fill` and `stroke` channels, so we undo the mapping here.
        if let Some(color) = encoding.remove("color") {
            encoding.insert("fill".to_string(), color);
        }
        // Use the natural data order
        encoding.insert("order".to_string(), json!({"value": Value::Null}));
        Ok(())
    }

    fn modify_spec(&self, layer_spec: &mut Value, _layer: &Layer) -> Result<()> {
        layer_spec["mark"] = json!({
            "type": "line",
            "interpolate": "linear-closed"
        });
        Ok(())
    }
}

// =============================================================================
// Violin Renderer
// =============================================================================

/// Renderer for violin geom - uses line
pub struct ViolinRenderer;

impl GeomRenderer for ViolinRenderer {
    fn modify_spec(&self, layer_spec: &mut Value, _layer: &Layer) -> Result<()> {
        layer_spec["mark"] = json!({
            "type": "line",
            "filled": true
        });
        let offset_col = naming::aesthetic_column("offset");

        // We use an order calculation to create a proper closed shape.
        // Right side (+ offset), sort by -y (top -> bottom)
        // Left side (- offset), sort by +y (bottom -> top)
        let calc_order = format!(
            "datum.__violin_offset > 0 ? -datum.{y} : datum.{y}",
            y = naming::aesthetic_column("pos2")
        );

        // Filter threshold to trim very low density regions (removes thin tails)
        // The offset is pre-scaled to [0, 0.5 * width] by geom post_process,
        // but this filter still catches extremely low values.
        let filter_expr = format!("datum.{} > 0.001", offset_col);

        // Preserve existing transforms (e.g., source filter) and extend with violin-specific transforms
        let existing_transforms = layer_spec
            .get("transform")
            .and_then(|t| t.as_array())
            .cloned()
            .unwrap_or_default();

        // Mirror the offset on both sides (offset is already scaled by post_process)
        let violin_offset = format!("[datum.{offset}, -datum.{offset}]", offset = offset_col);

        // Check if pos1offset exists (from dodging) - we'll combine it with violin offset
        let pos1offset_col = naming::aesthetic_column("pos1offset");

        let mut transforms = existing_transforms;
        transforms.extend(vec![
            json!({
                // Remove points with very low density to clean up thin tails
                "filter": filter_expr
            }),
            json!({
                // Mirror offset on both sides (offset is pre-scaled to [0, 0.5 * width])
                "calculate": violin_offset,
                "as": "violin_offsets"
            }),
            json!({
                "flatten": ["violin_offsets"],
                "as": ["__violin_offset"]
            }),
            json!({
                // Add pos1offset (dodge displacement) if it exists, otherwise use violin offset directly
                // This positions the violin correctly when dodging
                "calculate": format!(
                    "datum.{pos1offset} != null ? datum.__violin_offset + datum.{pos1offset} : datum.__violin_offset",
                    pos1offset = pos1offset_col
                ),
                "as": "__final_offset"
            }),
            json!({
                "calculate": calc_order,
                "as": "__order"
            }),
        ]);

        layer_spec["transform"] = json!(transforms);
        Ok(())
    }

    fn modify_encoding(&self, encoding: &mut Map<String, Value>, _layer: &Layer) -> Result<()> {
        // Ensure x is in detail encoding to create separate violins per x category
        // This is needed because line marks with filled:true require detail to create separate paths
        let x_field = encoding
            .get("x")
            .and_then(|x| x.get("field"))
            .and_then(|f| f.as_str())
            .map(|s| s.to_string());

        if let Some(x_field) = x_field {
            match encoding.get_mut("detail") {
                Some(detail) if detail.is_object() => {
                    // Single field object - check if it's already x, otherwise convert to array
                    if detail.get("field").and_then(|f| f.as_str()) != Some(&x_field) {
                        let existing = detail.clone();
                        *detail = json!([existing, {"field": x_field, "type": "nominal"}]);
                    }
                }
                Some(detail) if detail.is_array() => {
                    // Array - check if x already present, add if not
                    let arr = detail.as_array_mut().unwrap();
                    let has_x = arr
                        .iter()
                        .any(|d| d.get("field").and_then(|f| f.as_str()) == Some(&x_field));
                    if !has_x {
                        arr.push(json!({"field": x_field, "type": "nominal"}));
                    }
                }
                None => {
                    // No detail encoding - add it with x field
                    encoding.insert(
                        "detail".to_string(),
                        json!({"field": x_field, "type": "nominal"}),
                    );
                }
                _ => {}
            }
        }

        // Violins use filled line marks, which don't show a fill in the legend.
        // We intercept the encoding to pupulate a different symbol to display
        for aesthetic in ["fill", "stroke"] {
            if let Some(channel) = encoding.get_mut(aesthetic) {
                // Skip if legend is explicitly null or if it's a literal value
                if channel.get("legend").is_some_and(|v| v.is_null()) {
                    continue;
                }
                if channel.get("value").is_some() {
                    continue;
                }

                // Add/update legend properties
                let legend = channel.get_mut("legend").and_then(|v| v.as_object_mut());
                if let Some(legend_map) = legend {
                    legend_map.insert("symbolType".to_string(), json!("circle"));
                } else {
                    channel["legend"] = json!({
                        "symbolType": "circle"
                    });
                }
            }
        }

        encoding.insert(
            "xOffset".to_string(),
            json!({
                "field": "__final_offset",
                "type": "quantitative",
                "scale": {
                    "domain": [-0.5, 0.5]
                }
            }),
        );
        encoding.insert(
            "order".to_string(),
            json!({
                "field": "__order",
                "type": "quantitative"
            }),
        );
        Ok(())
    }
}

// =============================================================================
// Boxplot Renderer
// =============================================================================

/// Metadata for boxplot rendering
struct BoxplotMetadata {
    /// Whether there are any outliers
    has_outliers: bool,
}

/// Renderer for boxplot geom - splits into multiple component layers
pub struct BoxplotRenderer;

impl BoxplotRenderer {
    /// Prepare boxplot data by splitting into type-specific datasets.
    ///
    /// Returns a HashMap of type_suffix -> data_values, plus has_outliers flag.
    /// Type suffixes are: "lower_whisker", "upper_whisker", "box", "median", "outlier"
    fn prepare_components(
        &self,
        data: &DataFrame,
        binned_columns: &HashMap<String, Vec<f64>>,
    ) -> Result<(HashMap<String, Vec<Value>>, bool)> {
        let type_col = naming::aesthetic_column("type");
        let type_col = type_col.as_str();

        // Get the type column for filtering
        let type_series = data
            .column(type_col)
            .and_then(|s| s.str())
            .map_err(|e| GgsqlError::WriterError(e.to_string()))?;

        // Check for outliers
        let has_outliers = type_series.equal("outlier").any();

        // Split data by type into separate datasets
        let mut type_datasets: HashMap<String, Vec<Value>> = HashMap::new();

        for type_name in &["lower_whisker", "upper_whisker", "box", "median", "outlier"] {
            let mask = type_series.equal(*type_name);
            let filtered = data
                .filter(&mask)
                .map_err(|e| GgsqlError::WriterError(e.to_string()))?;

            // Skip empty datasets (e.g., no outliers)
            if filtered.height() == 0 {
                continue;
            }

            // Drop the type column since type is now encoded in the source key
            let filtered = filtered
                .drop(type_col)
                .map_err(|e| GgsqlError::WriterError(e.to_string()))?;

            let values = if binned_columns.is_empty() {
                dataframe_to_values(&filtered)?
            } else {
                dataframe_to_values_with_bins(&filtered, binned_columns)?
            };

            type_datasets.insert(type_name.to_string(), values);
        }

        Ok((type_datasets, has_outliers))
    }

    /// Render boxplot layers using filter transforms on the unified dataset.
    ///
    /// Creates 5 layers: outliers (optional), lower whiskers, upper whiskers, box, median line.
    fn render_layers(
        &self,
        prototype: Value,
        layer: &Layer,
        base_key: &str,
        has_outliers: bool,
    ) -> Result<Vec<Value>> {
        let mut layers: Vec<Value> = Vec::new();

        let value_col = naming::aesthetic_column("pos2");
        let value2_col = naming::aesthetic_column("pos2end");

        let x_col = layer
            .mappings
            .get("pos1")
            .and_then(|x| x.column_name())
            .ok_or_else(|| {
                GgsqlError::WriterError("Boxplot requires 'x' aesthetic mapping".to_string())
            })?;
        // Validate y aesthetic exists (not used directly but required for boxplot)
        layer
            .mappings
            .get("pos2")
            .and_then(|y| y.column_name())
            .ok_or_else(|| {
                GgsqlError::WriterError("Boxplot requires 'y' aesthetic mapping".to_string())
            })?;

        // Set orientation
        let is_horizontal = x_col == value_col;
        let value_var1 = if is_horizontal { "x" } else { "y" };
        let value_var2 = if is_horizontal { "x2" } else { "y2" };

        // Get width parameter
        let base_width = layer
            .parameters
            .get("width")
            .and_then(|v| match v {
                ParameterValue::Number(n) => Some(*n),
                _ => None,
            })
            .unwrap_or(0.9);

        // For dodged boxplots, use expression-based width with adjusted_width
        // For non-dodged boxplots, use band-relative width
        let width_value = if let Some(adjusted) = layer.adjusted_width {
            json!({"expr": format!("bandwidth('x') * {}", adjusted)})
        } else {
            json!({"band": base_width})
        };

        // Helper to create filter transform for source selection
        let make_source_filter = |type_suffix: &str| -> Value {
            let source_key = format!("{}{}", base_key, type_suffix);
            json!({
                "filter": {
                    "field": naming::SOURCE_COLUMN,
                    "equal": source_key
                }
            })
        };

        // Helper to create a layer with source filter and mark
        let create_layer = |proto: &Value, type_suffix: &str, mark: Value| -> Value {
            let mut layer_spec = proto.clone();
            let existing_transforms = layer_spec
                .get("transform")
                .and_then(|t| t.as_array())
                .cloned()
                .unwrap_or_default();
            let mut new_transforms = vec![make_source_filter(type_suffix)];
            new_transforms.extend(existing_transforms);
            layer_spec["transform"] = json!(new_transforms);
            layer_spec["mark"] = mark;
            layer_spec
        };

        // Create outlier points layer (if there are outliers)
        if has_outliers {
            let mut points = create_layer(
                &prototype,
                "outlier",
                json!({
                    "type": "point"
                }),
            );
            if points["encoding"].get("color").is_some() {
                points["mark"]["filled"] = json!(true);
            }

            layers.push(points);
        }

        // Clone prototype without size/shape (these apply only to points)
        let mut summary_prototype = prototype.clone();
        if let Some(Value::Object(ref mut encoding)) = summary_prototype.get_mut("encoding") {
            encoding.remove("size");
            encoding.remove("shape");
        }

        // Build encoding templates for y and y2 fields
        let mut y_encoding = summary_prototype["encoding"][value_var1].clone();
        y_encoding["field"] = json!(value_col);
        let mut y2_encoding = summary_prototype["encoding"][value_var1].clone();
        y2_encoding["field"] = json!(value2_col);
        y2_encoding["title"] = Value::Null; // Suppress y2 title to prevent "y, y2" axis label

        // Lower whiskers (rule from y to y2, where y=q1 and y2=lower)
        let mut lower_whiskers = create_layer(
            &summary_prototype,
            "lower_whisker",
            json!({
                "type": "rule"
            }),
        );

        // Handle strokeWidth -> size for rule marks
        if let Some(linewidth) = lower_whiskers["encoding"].get("strokeWidth").cloned() {
            lower_whiskers["encoding"]["size"] = linewidth;
            if let Some(Value::Object(ref mut encoding)) = lower_whiskers.get_mut("encoding") {
                encoding.remove("strokeWidth");
            }
        }

        lower_whiskers["encoding"][value_var1] = y_encoding.clone();
        lower_whiskers["encoding"][value_var2] = y2_encoding.clone();

        // Upper whiskers (rule from y to y2, where y=q3 and y2=upper)
        let mut upper_whiskers = create_layer(
            &summary_prototype,
            "upper_whisker",
            json!({
                "type": "rule"
            }),
        );

        // Handle strokeWidth -> size for rule marks
        if let Some(linewidth) = upper_whiskers["encoding"].get("strokeWidth").cloned() {
            upper_whiskers["encoding"]["size"] = linewidth;
            if let Some(Value::Object(ref mut encoding)) = upper_whiskers.get_mut("encoding") {
                encoding.remove("strokeWidth");
            }
        }

        upper_whiskers["encoding"][value_var1] = y_encoding.clone();
        upper_whiskers["encoding"][value_var2] = y2_encoding.clone();

        // Box (bar from y to y2, where y=q1 and y2=q3)
        let mut box_part = create_layer(
            &summary_prototype,
            "box",
            json!({
                "type": "bar",
                "width": width_value,
                "align": "center"
            }),
        );
        box_part["encoding"][value_var1] = y_encoding.clone();
        box_part["encoding"][value_var2] = y2_encoding.clone();

        // Median line (tick at y, where y=median)
        let mut median_line = create_layer(
            &summary_prototype,
            "median",
            json!({
                "type": "tick",
                "width": width_value,
                "align": "center"
            }),
        );
        median_line["encoding"][value_var1] = y_encoding;

        layers.push(lower_whiskers);
        layers.push(upper_whiskers);
        layers.push(box_part);
        layers.push(median_line);

        Ok(layers)
    }
}

impl GeomRenderer for BoxplotRenderer {
    fn prepare_data(
        &self,
        df: &DataFrame,
        _data_key: &str,
        binned_columns: &HashMap<String, Vec<f64>>,
    ) -> Result<PreparedData> {
        let (components, has_outliers) = self.prepare_components(df, binned_columns)?;

        Ok(PreparedData::Composite {
            components,
            metadata: Box::new(BoxplotMetadata { has_outliers }),
        })
    }

    fn needs_source_filter(&self) -> bool {
        // Boxplot uses component-specific filters instead
        false
    }

    fn finalize(
        &self,
        prototype: Value,
        layer: &Layer,
        data_key: &str,
        prepared: &PreparedData,
    ) -> Result<Vec<Value>> {
        let PreparedData::Composite { metadata, .. } = prepared else {
            return Err(GgsqlError::InternalError(
                "BoxplotRenderer::finalize called with non-composite data".to_string(),
            ));
        };

        let info = metadata.downcast_ref::<BoxplotMetadata>().ok_or_else(|| {
            GgsqlError::InternalError("Failed to downcast boxplot metadata".to_string())
        })?;

        self.render_layers(prototype, layer, data_key, info.has_outliers)
    }
}

// =============================================================================
// Dispatcher
// =============================================================================

/// Get the appropriate renderer for a geom type
pub fn get_renderer(geom: &Geom) -> Box<dyn GeomRenderer> {
    match geom.geom_type() {
        GeomType::Path => Box::new(PathRenderer),
        GeomType::Bar => Box::new(BarRenderer),
        GeomType::Area => Box::new(AreaRenderer),
        GeomType::Ribbon => Box::new(RibbonRenderer),
        GeomType::Polygon => Box::new(PolygonRenderer),
        GeomType::Boxplot => Box::new(BoxplotRenderer),
        GeomType::Density => Box::new(AreaRenderer),
        GeomType::Violin => Box::new(ViolinRenderer),
        // All other geoms (Point, Line, Tile, etc.) use the default renderer
        _ => Box::new(DefaultRenderer),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_violin_detail_encoding() {
        let renderer = ViolinRenderer;
        let layer = Layer::new(crate::plot::Geom::violin());

        // Case 1: No detail encoding - should add x
        let mut encoding = serde_json::Map::new();
        encoding.insert(
            "x".to_string(),
            json!({"field": "species", "type": "nominal"}),
        );
        renderer.modify_encoding(&mut encoding, &layer).unwrap();
        assert_eq!(
            encoding.get("detail"),
            Some(&json!({"field": "species", "type": "nominal"}))
        );

        // Case 2: Detail is single object (not x) - should convert to array
        let mut encoding = serde_json::Map::new();
        encoding.insert(
            "x".to_string(),
            json!({"field": "species", "type": "nominal"}),
        );
        encoding.insert(
            "detail".to_string(),
            json!({"field": "island", "type": "nominal"}),
        );
        renderer.modify_encoding(&mut encoding, &layer).unwrap();
        assert_eq!(
            encoding.get("detail"),
            Some(&json!([
                {"field": "island", "type": "nominal"},
                {"field": "species", "type": "nominal"}
            ]))
        );

        // Case 3: Detail is single object (already x) - should not change
        let mut encoding = serde_json::Map::new();
        encoding.insert(
            "x".to_string(),
            json!({"field": "species", "type": "nominal"}),
        );
        encoding.insert(
            "detail".to_string(),
            json!({"field": "species", "type": "nominal"}),
        );
        renderer.modify_encoding(&mut encoding, &layer).unwrap();
        assert_eq!(
            encoding.get("detail"),
            Some(&json!({"field": "species", "type": "nominal"}))
        );

        // Case 4: Detail is array without x - should add x
        let mut encoding = serde_json::Map::new();
        encoding.insert(
            "x".to_string(),
            json!({"field": "species", "type": "nominal"}),
        );
        encoding.insert(
            "detail".to_string(),
            json!([{"field": "island", "type": "nominal"}]),
        );
        renderer.modify_encoding(&mut encoding, &layer).unwrap();
        assert_eq!(
            encoding.get("detail"),
            Some(&json!([
                {"field": "island", "type": "nominal"},
                {"field": "species", "type": "nominal"}
            ]))
        );

        // Case 5: Detail is array with x already - should not change
        let mut encoding = serde_json::Map::new();
        encoding.insert(
            "x".to_string(),
            json!({"field": "species", "type": "nominal"}),
        );
        encoding.insert(
            "detail".to_string(),
            json!([
                {"field": "island", "type": "nominal"},
                {"field": "species", "type": "nominal"}
            ]),
        );
        renderer.modify_encoding(&mut encoding, &layer).unwrap();
        assert_eq!(
            encoding.get("detail"),
            Some(&json!([
                {"field": "island", "type": "nominal"},
                {"field": "species", "type": "nominal"}
            ]))
        );
    }

    #[test]
    fn test_violin_mirroring() {
        use crate::naming;

        let renderer = ViolinRenderer;

        let layer = Layer::new(crate::plot::Geom::violin());
        let mut layer_spec = json!({
            "mark": {"type": "line"},
            "encoding": {
                "x": {"field": "species", "type": "nominal"},
                "y": {"field": naming::aesthetic_column("pos2"), "type": "quantitative"}
            }
        });

        renderer.modify_spec(&mut layer_spec, &layer).unwrap();

        // Verify transforms include mirroring (violin_offsets)
        let transforms = layer_spec["transform"].as_array().unwrap();

        // Find the violin_offsets calculation (mirrors offset on both sides)
        let mirror_calc = transforms
            .iter()
            .find(|t| t.get("as").and_then(|a| a.as_str()) == Some("violin_offsets"));
        assert!(
            mirror_calc.is_some(),
            "Should have violin_offsets mirroring calculation"
        );

        let calc_expr = mirror_calc.unwrap()["calculate"].as_str().unwrap();
        let offset_col = naming::aesthetic_column("offset");
        // Should mirror the offset column: [datum.offset, -datum.offset]
        assert!(
            calc_expr.contains(&offset_col),
            "Mirror calculation should use offset column: {}",
            calc_expr
        );
        assert!(
            calc_expr.contains("-datum"),
            "Mirror calculation should negate: {}",
            calc_expr
        );

        // Verify flatten transform exists
        let flatten = transforms.iter().find(|t| t.get("flatten").is_some());
        assert!(
            flatten.is_some(),
            "Should have flatten transform for violin_offsets"
        );

        // Verify __final_offset calculation (combines with dodge offset)
        let final_offset = transforms
            .iter()
            .find(|t| t.get("as").and_then(|a| a.as_str()) == Some("__final_offset"));
        assert!(
            final_offset.is_some(),
            "Should have __final_offset calculation"
        );
    }
}
