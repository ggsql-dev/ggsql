//! Encoding channel construction for Vega-Lite writer
//!
//! This module handles building Vega-Lite encoding channels from ggsql aesthetic mappings,
//! including type inference, scale properties, and title handling.

use crate::plot::layer::geom::GeomAesthetics;
use crate::plot::scale::{linetype_to_stroke_dash, shape_to_svg_path, ScaleTypeKind};
use crate::plot::ParameterValue;
use crate::{AestheticValue, DataFrame, Plot, Result};
use polars::prelude::*;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};

use super::{POINTS_TO_AREA, POINTS_TO_PIXELS};

/// Build a Vega-Lite labelExpr from label mappings
///
/// Generates a conditional expression that renames or suppresses labels:
/// - `Some(label)` -> rename to that label
/// - `None` -> suppress label (empty string)
///
/// For non-temporal scales:
/// - Uses `datum.label` for comparisons
/// - Example: `"datum.label == 'A' ? 'Alpha' : datum.label == 'B' ? 'Beta' : datum.label"`
///
/// For temporal scales:
/// - Uses `timeFormat(datum.value, 'fmt')` for comparisons
/// - This is necessary because `datum.label` contains Vega-Lite's formatted label (e.g., "Jan 1, 2024")
///   but our label_mapping keys are ISO format strings (e.g., "2024-01-01")
/// - Example: `"timeFormat(datum.value, '%Y-%m-%d') == '2024-01-01' ? 'Q1 Start' : datum.label"`
pub(super) fn build_label_expr(
    mappings: &HashMap<String, Option<String>>,
    time_format: Option<&str>,
) -> String {
    if mappings.is_empty() {
        return "datum.label".to_string();
    }

    // Build the comparison expression based on whether this is temporal
    let comparison_expr = match time_format {
        Some(fmt) => format!("timeFormat(datum.value, '{}')", fmt),
        None => "datum.label".to_string(),
    };

    let mut parts: Vec<String> = mappings
        .iter()
        .map(|(from, to)| {
            let from_escaped = from.replace('\'', "\\'");
            match to {
                Some(label) => {
                    let to_escaped = label.replace('\'', "\\'");
                    format!(
                        "{} == '{}' ? '{}'",
                        comparison_expr, from_escaped, to_escaped
                    )
                }
                None => {
                    // NULL suppresses the label (empty string)
                    format!("{} == '{}' ? ''", comparison_expr, from_escaped)
                }
            }
        })
        .collect();

    // Fallback to original label
    parts.push("datum.label".to_string());
    parts.join(" : ")
}

/// Check if a string column contains numeric values
pub(super) fn is_numeric_string_column(series: &Series) -> bool {
    if let Ok(ca) = series.str() {
        // Check first few non-null values to see if they're numeric
        for val in ca.into_iter().flatten().take(5) {
            if val.parse::<f64>().is_err() {
                return false;
            }
        }
        true
    } else {
        false
    }
}

/// Infer Vega-Lite field type from DataFrame column
pub(super) fn infer_field_type(df: &DataFrame, field: &str) -> String {
    if let Ok(column) = df.column(field) {
        use DataType::*;
        match column.dtype() {
            Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 | Float32 | Float64 => {
                "quantitative"
            }
            Boolean => "nominal",
            String => {
                // Check if string column contains numeric values
                if is_numeric_string_column(column.as_materialized_series()) {
                    "quantitative"
                } else {
                    "nominal"
                }
            }
            Date | Datetime(_, _) | Time => "temporal",
            _ => "nominal",
        }
        .to_string()
    } else {
        "nominal".to_string()
    }
}

/// Determine Vega-Lite field type from scale specification
pub(super) fn determine_field_type_from_scale(
    scale: &crate::plot::Scale,
    inferred: &str,
    _aesthetic: &str,
    identity_scale: &mut bool,
) -> String {
    // Use scale type if explicitly specified
    if let Some(scale_type) = &scale.scale_type {
        use crate::plot::ScaleTypeKind;
        match scale_type.scale_type_kind() {
            ScaleTypeKind::Continuous => "quantitative",
            ScaleTypeKind::Discrete => "nominal",
            ScaleTypeKind::Binned => "quantitative", // Binned data is still quantitative
            ScaleTypeKind::Ordinal => "ordinal",     // Native Vega-Lite ordinal type
            ScaleTypeKind::Identity => {
                *identity_scale = true;
                inferred
            }
        }
        .to_string()
    } else {
        // Scale exists but no type specified, use inferred
        inferred.to_string()
    }
}

/// Build encoding channel from aesthetic mapping
///
/// The `titled_families` set tracks which aesthetic families have already received
/// a title, ensuring only one title per family (e.g., one title for x/xmin/xmax).
///
/// The `primary_aesthetics` set contains primary aesthetics that exist in the layer.
/// When a primary exists, variant aesthetics (xmin, ymin, etc.) get `title: null`.
#[allow(clippy::too_many_lines)]
pub(super) fn build_encoding_channel(
    aesthetic: &str,
    value: &AestheticValue,
    df: &DataFrame,
    spec: &Plot,
    titled_families: &mut HashSet<String>,
    primary_aesthetics: &HashSet<String>,
) -> Result<Value> {
    match value {
        AestheticValue::Column {
            name: col,
            original_name,
            is_dummy,
        } => {
            // Check if there's a scale specification for this aesthetic or its primary
            // E.g., "xmin" should use the "x" scale
            let primary = GeomAesthetics::primary_aesthetic(aesthetic);
            let inferred = infer_field_type(df, col);
            let mut identity_scale = false;

            let field_type = if let Some(scale) = spec.find_scale(primary) {
                // Check if the transform indicates temporal data
                // (Transform takes precedence since it's resolved from column dtype)
                if let Some(ref transform) = scale.transform {
                    if transform.is_temporal() {
                        "temporal".to_string()
                    } else {
                        // Non-temporal transform, fall through to scale type check
                        determine_field_type_from_scale(
                            scale,
                            &inferred,
                            aesthetic,
                            &mut identity_scale,
                        )
                    }
                } else {
                    // No transform, check scale type
                    determine_field_type_from_scale(
                        scale,
                        &inferred,
                        aesthetic,
                        &mut identity_scale,
                    )
                }
            } else {
                // No scale specification, infer from data
                inferred
            };

            // Check if this aesthetic has a binned scale
            let is_binned = spec
                .find_scale(primary)
                .and_then(|s| s.scale_type.as_ref())
                .map(|st| st.scale_type_kind() == ScaleTypeKind::Binned)
                .unwrap_or(false);

            let mut encoding = json!({
                "field": col,
                "type": field_type,
            });

            // For binned scales, add bin: "binned" to enable Vega-Lite's binned data handling
            // This allows proper axis tick placement at bin edges and range labels in legends
            if is_binned {
                encoding["bin"] = json!("binned");
            }

            // Apply title handling:
            // - Primary aesthetics (x, y, color) can set the title
            // - Variant aesthetics (xmin, ymin, etc.) only get title if no primary exists
            // - When a primary exists, variants get title: null to prevent axis label conflicts
            let is_primary = aesthetic == primary;
            let primary_exists = primary_aesthetics.contains(primary);

            if is_primary && !titled_families.contains(primary) {
                // Primary aesthetic: set title from explicit label or original_name
                let explicit_label = spec
                    .labels
                    .as_ref()
                    .and_then(|labels| labels.labels.get(primary));

                if let Some(label) = explicit_label {
                    encoding["title"] = json!(label);
                    titled_families.insert(primary.to_string());
                } else if let Some(orig) = original_name {
                    // Use original column name as default title when available
                    // (preserves readable names when columns are renamed to internal names)
                    encoding["title"] = json!(orig);
                    titled_families.insert(primary.to_string());
                }
            } else if !is_primary && primary_exists {
                // Variant with primary present: suppress title to avoid axis label conflicts
                encoding["title"] = Value::Null;
            } else if !is_primary && !primary_exists && !titled_families.contains(primary) {
                // Variant without primary: allow first variant to claim title (for explicit labels)
                if let Some(ref labels) = spec.labels {
                    if let Some(label) = labels.labels.get(primary) {
                        encoding["title"] = json!(label);
                        titled_families.insert(primary.to_string());
                    }
                }
            }

            let mut scale_obj = serde_json::Map::new();
            // Track if we're using a color range array (needs gradient legend)
            let mut needs_gradient_legend = false;

            // Use scale properties from the primary aesthetic's scale
            // (same scale lookup as used above for field_type)
            if let Some(scale) = spec.find_scale(primary) {
                // Apply scale properties from SCALE if specified
                use crate::plot::{ArrayElement, OutputRange};

                // Apply domain from input_range (FROM clause)
                if let Some(ref domain_values) = scale.input_range {
                    let domain_json: Vec<Value> =
                        domain_values.iter().map(|elem| elem.to_json()).collect();
                    scale_obj.insert("domain".to_string(), json!(domain_json));
                }

                // Apply range from output_range (TO clause)

                if let Some(ref output_range) = scale.output_range {
                    match output_range {
                        OutputRange::Array(range_values) => {
                            let range_json: Vec<Value> = range_values
                                .iter()
                                .map(|elem| match elem {
                                    ArrayElement::String(s) => {
                                        // For shape aesthetic, convert to SVG path
                                        if aesthetic == "shape" {
                                            if let Some(svg_path) = shape_to_svg_path(s) {
                                                json!(svg_path)
                                            } else {
                                                // Unknown shape, pass through
                                                json!(s)
                                            }
                                        // For linetype aesthetic, convert to dash array
                                        } else if aesthetic == "linetype" {
                                            if let Some(dash_array) = linetype_to_stroke_dash(s) {
                                                json!(dash_array)
                                            } else {
                                                // Unknown linetype, pass through
                                                json!(s)
                                            }
                                        } else {
                                            json!(s)
                                        }
                                    }
                                    ArrayElement::Number(n) => {
                                        match aesthetic {
                                            // Size: convert radius (points) to area (pixels²)
                                            // area = r² × π × (96/72)²
                                            "size" => json!(n * n * POINTS_TO_AREA),
                                            // Linewidth: convert points to pixels
                                            "linewidth" => json!(n * POINTS_TO_PIXELS),
                                            // Other aesthetics: pass through unchanged
                                            _ => json!(n),
                                        }
                                    }
                                    // All other types use to_json()
                                    other => other.to_json(),
                                })
                                .collect();
                            scale_obj.insert("range".to_string(), json!(range_json));

                            // For continuous color scales with range array, use gradient legend
                            if matches!(aesthetic, "fill" | "stroke")
                                && matches!(
                                    scale.scale_type.as_ref().map(|st| st.scale_type_kind()),
                                    Some(ScaleTypeKind::Continuous)
                                )
                            {
                                needs_gradient_legend = true;
                            }
                        }
                        OutputRange::Palette(palette_name) => {
                            // Named palette - expand to color scheme
                            scale_obj
                                .insert("scheme".to_string(), json!(palette_name.to_lowercase()));
                        }
                    }
                }

                // Handle transform (VIA clause)
                if let Some(ref transform) = scale.transform {
                    use crate::plot::scale::TransformKind;
                    match transform.transform_kind() {
                        TransformKind::Identity => {} // Linear (default), no additional scale properties needed
                        TransformKind::Log10 => {
                            scale_obj.insert("type".to_string(), json!("log"));
                            scale_obj.insert("base".to_string(), json!(10));
                            scale_obj.insert("zero".to_string(), json!(false));
                        }
                        TransformKind::Log => {
                            // Natural logarithm - Vega-Lite uses "log" with base e
                            scale_obj.insert("type".to_string(), json!("log"));
                            scale_obj.insert("base".to_string(), json!(std::f64::consts::E));
                            scale_obj.insert("zero".to_string(), json!(false));
                        }
                        TransformKind::Log2 => {
                            scale_obj.insert("type".to_string(), json!("log"));
                            scale_obj.insert("base".to_string(), json!(2));
                            scale_obj.insert("zero".to_string(), json!(false));
                        }
                        TransformKind::Sqrt => {
                            scale_obj.insert("type".to_string(), json!("sqrt"));
                        }
                        TransformKind::Square => {
                            scale_obj.insert("type".to_string(), json!("pow"));
                            scale_obj.insert("exponent".to_string(), json!(2));
                        }
                        TransformKind::Exp10 | TransformKind::Exp2 | TransformKind::Exp => {
                            // Vega-Lite doesn't have native exp scales
                            // Using linear scale; data is already transformed in data space
                            eprintln!(
                                "Warning: {} transform has no native Vega-Lite equivalent, using linear scale",
                                transform.name()
                            );
                        }
                        TransformKind::Asinh | TransformKind::PseudoLog => {
                            scale_obj.insert("type".to_string(), json!("symlog"));
                        }
                        // Temporal transforms are identity in numeric space;
                        // the field type ("temporal") is set based on the transform kind
                        TransformKind::Date | TransformKind::DateTime | TransformKind::Time => {}
                        // Discrete transforms (String, Bool) don't affect Vega-Lite scale type;
                        // the data casting happens at the SQL level before reaching the writer
                        TransformKind::String | TransformKind::Bool => {}
                        // Integer transform is linear scale; casting happens at SQL level
                        TransformKind::Integer => {}
                    }
                }

                // Handle reverse property (SETTING clause)
                use crate::plot::ParameterValue;
                if let Some(ParameterValue::Boolean(true)) = scale.properties.get("reverse") {
                    scale_obj.insert("reverse".to_string(), json!(true));

                    // For discrete/ordinal scales with legends, also reverse the legend order
                    // Vega-Lite's scale.reverse only reverses the visual mapping, not the legend
                    if let Some(ref scale_type) = scale.scale_type {
                        let kind = scale_type.scale_type_kind();
                        if matches!(kind, ScaleTypeKind::Discrete | ScaleTypeKind::Ordinal) {
                            // Only for non-positional aesthetics (those with legends)
                            if !matches!(
                                aesthetic,
                                "x" | "y" | "xmin" | "xmax" | "ymin" | "ymax" | "xend" | "yend"
                            ) {
                                // Use the input_range (domain) if available
                                if let Some(ref domain) = scale.input_range {
                                    let reversed_domain: Vec<Value> =
                                        domain.iter().rev().map(|e| e.to_json()).collect();
                                    // Set legend.values with reversed order
                                    if !encoding.get("legend").is_some_and(|v| v.is_null()) {
                                        let legend = encoding
                                            .get_mut("legend")
                                            .and_then(|v| v.as_object_mut());
                                        if let Some(legend_map) = legend {
                                            legend_map.insert(
                                                "values".to_string(),
                                                json!(reversed_domain),
                                            );
                                        } else {
                                            encoding["legend"] = json!({"values": reversed_domain});
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                // Handle resolved breaks -> axis.values or legend.values
                // breaks is stored as Array in properties after resolution
                // For binned scales, we still need to set axis.values manually because
                // Vega-Lite's automatic tick placement with bin:"binned" only works for equal-width bins
                if let Some(ParameterValue::Array(breaks)) = scale.properties.get("breaks") {
                    // Filter out values that have label_mapping = None (suppressed labels)
                    // This respects decisions made during scale resolution
                    let values: Vec<Value> = breaks
                        .iter()
                        .filter(|e| {
                            if let Some(ref label_mapping) = scale.label_mapping {
                                // Keep value only if it's not mapped to None
                                let key = e.to_key_string();
                                !matches!(label_mapping.get(&key), Some(None))
                            } else {
                                true // No label_mapping, keep all values
                            }
                        })
                        .map(|e| e.to_json())
                        .collect();

                    // Positional aesthetics use axis.values, others use legend.values
                    if matches!(
                        aesthetic,
                        "x" | "y" | "xmin" | "xmax" | "ymin" | "ymax" | "xend" | "yend"
                    ) {
                        // Add to axis object
                        if !encoding.get("axis").is_some_and(|v| v.is_null()) {
                            let axis = encoding.get_mut("axis").and_then(|v| v.as_object_mut());
                            if let Some(axis_map) = axis {
                                axis_map.insert("values".to_string(), json!(values));
                            } else {
                                encoding["axis"] = json!({"values": values});
                            }
                        }
                    } else {
                        // Add to legend object for non-positional aesthetics
                        if !encoding.get("legend").is_some_and(|v| v.is_null()) {
                            let legend = encoding.get_mut("legend").and_then(|v| v.as_object_mut());
                            if let Some(legend_map) = legend {
                                legend_map.insert("values".to_string(), json!(values));
                            } else {
                                encoding["legend"] = json!({"values": values});
                            }
                        }
                    }
                }

                // Handle label_mapping -> labelExpr (RENAMING clause)
                if let Some(ref label_mapping) = scale.label_mapping {
                    if !label_mapping.is_empty() {
                        // For temporal scales, use timeFormat() to compare against ISO keys
                        // because datum.label contains Vega-Lite's formatted label (e.g., "Jan 1, 2024")
                        // but our label_mapping keys are ISO format strings (e.g., "2024-01-01")
                        use crate::plot::scale::TransformKind;
                        let time_format =
                            scale
                                .transform
                                .as_ref()
                                .and_then(|t| match t.transform_kind() {
                                    TransformKind::Date => Some("%Y-%m-%d"),
                                    TransformKind::DateTime => Some("%Y-%m-%dT%H:%M:%S"),
                                    TransformKind::Time => Some("%H:%M:%S"),
                                    _ => None,
                                });
                        let label_expr = build_label_expr(label_mapping, time_format);

                        if matches!(
                            aesthetic,
                            "x" | "y" | "xmin" | "xmax" | "ymin" | "ymax" | "xend" | "yend"
                        ) {
                            // Add to axis object
                            let axis = encoding.get_mut("axis").and_then(|v| v.as_object_mut());
                            if let Some(axis_map) = axis {
                                axis_map.insert("labelExpr".to_string(), json!(label_expr));
                            } else {
                                encoding["axis"] = json!({"labelExpr": label_expr});
                            }
                        } else {
                            // Add to legend object for non-positional aesthetics
                            let legend = encoding.get_mut("legend").and_then(|v| v.as_object_mut());
                            if let Some(legend_map) = legend {
                                legend_map.insert("labelExpr".to_string(), json!(label_expr));
                            } else {
                                encoding["legend"] = json!({"labelExpr": label_expr});
                            }
                        }
                    }
                }
            }
            // We don't automatically want to include 0 in our position scales
            if aesthetic == "x" || aesthetic == "y" {
                scale_obj.insert("zero".to_string(), json!(Value::Bool(false)));
            }

            if identity_scale {
                // When we have an identity scale, these scale properties don't matter.
                // We should return a `"scale": null`` in the encoding channel
                encoding["scale"] = json!(Value::Null)
            } else if !scale_obj.is_empty() {
                encoding["scale"] = json!(scale_obj);
            }

            // For continuous color scales with range array, use gradient legend
            // (scheme-based scales automatically get gradient legends from Vega-Lite)
            if needs_gradient_legend {
                // Merge gradient type into existing legend object (preserves values, etc.)
                if let Some(legend_obj) = encoding.get_mut("legend").and_then(|v| v.as_object_mut())
                {
                    legend_obj.insert("type".to_string(), json!("gradient"));
                } else if !encoding.get("legend").is_some_and(|v| v.is_null()) {
                    // No legend object yet, create one with gradient type
                    encoding["legend"] = json!({"type": "gradient"});
                }
                // If legend is explicitly null, leave it (user disabled legend via GUIDE)
            }

            // Hide axis for dummy columns (e.g., x when bar chart has no x mapped)
            if *is_dummy {
                encoding["axis"] = json!(null);
            }

            Ok(encoding)
        }
        AestheticValue::Literal(lit) => {
            // For literal values, use constant value encoding
            // Size and linewidth need unit conversion from points to Vega-Lite units
            let val = match lit {
                ParameterValue::String(s) => json!(s),
                ParameterValue::Number(n) => {
                    match aesthetic {
                        // Size: interpret as radius in points, convert to area in pixels²
                        // area = r² × π × (96/72)²
                        "size" => json!(n * n * POINTS_TO_AREA),
                        // Linewidth: interpret as width in points, convert to pixels
                        "linewidth" => json!(n * POINTS_TO_PIXELS),
                        // Other aesthetics: pass through unchanged
                        _ => json!(n),
                    }
                }
                ParameterValue::Boolean(b) => json!(b),
                // Grammar prevents arrays and null in literal aesthetic mappings
                ParameterValue::Array(_) | ParameterValue::Null => {
                    unreachable!("Grammar prevents arrays and null in literal aesthetic mappings")
                }
            };
            Ok(json!({"value": val}))
        }
    }
}

/// Map ggsql aesthetic name to Vega-Lite encoding channel name
pub(super) fn map_aesthetic_name(aesthetic: &str) -> String {
    match aesthetic {
        // Line aesthetics
        "linetype" => "strokeDash",
        "linewidth" => "strokeWidth",
        // Text aesthetics
        "label" => "text",
        // All other aesthetics pass through directly
        // (fill and stroke map to Vega-Lite's separate fill/stroke channels)
        _ => aesthetic,
    }
    .to_string()
}

/// Build detail encoding from partition_by columns
/// Maps partition_by columns to Vega-Lite's detail channel for grouping
pub(super) fn build_detail_encoding(partition_by: &[String]) -> Option<Value> {
    if partition_by.is_empty() {
        return None;
    }

    if partition_by.len() == 1 {
        // Single column: simple object
        Some(json!({
            "field": partition_by[0],
            "type": "nominal"
        }))
    } else {
        // Multiple columns: array of detail specifications
        let details: Vec<Value> = partition_by
            .iter()
            .map(|col| {
                json!({
                    "field": col,
                    "type": "nominal"
                })
            })
            .collect();
        Some(json!(details))
    }
}
