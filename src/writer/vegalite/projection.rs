//! Projection transformations for Vega-Lite writer
//!
//! This module handles projection transformations (cartesian, polar)
//! that modify the Vega-Lite spec structure based on the PROJECT clause.

use crate::plot::{CoordKind, ParameterValue, Projection};
use crate::{DataFrame, GgsqlError, Plot, Result};
use serde_json::{json, Value};

/// Apply projection transformations to the spec and data
/// Returns (possibly transformed DataFrame, possibly modified spec)
/// free_x/free_y indicate whether facets have independent scales (affects domain application)
pub(super) fn apply_project_transforms(
    spec: &Plot,
    data: &DataFrame,
    vl_spec: &mut Value,
    free_x: bool,
    free_y: bool,
) -> Result<Option<DataFrame>> {
    if let Some(ref project) = spec.project {
        // Apply coord-specific transformations
        let result = match project.coord.coord_kind() {
            CoordKind::Cartesian => {
                apply_cartesian_project(project, vl_spec, free_x, free_y)?;
                None
            }
            CoordKind::Polar => Some(apply_polar_project(project, spec, data, vl_spec)?),
        };

        // Apply clip setting (applies to all projection types)
        if let Some(ParameterValue::Boolean(clip)) = project.properties.get("clip") {
            apply_clip_to_layers(vl_spec, *clip);
        }

        Ok(result)
    } else {
        Ok(None)
    }
}

/// Apply clip setting to all layers
fn apply_clip_to_layers(vl_spec: &mut Value, clip: bool) {
    if let Some(layers) = vl_spec.get_mut("layer") {
        if let Some(layers_arr) = layers.as_array_mut() {
            for layer in layers_arr {
                if let Some(mark) = layer.get_mut("mark") {
                    if mark.is_string() {
                        // Convert "point" to {"type": "point", "clip": ...}
                        let mark_type = mark.as_str().unwrap().to_string();
                        *mark = json!({"type": mark_type, "clip": clip});
                    } else if let Some(obj) = mark.as_object_mut() {
                        obj.insert("clip".to_string(), json!(clip));
                    }
                }
            }
        }
    }
}

/// Apply Cartesian projection properties
fn apply_cartesian_project(
    _project: &Projection,
    _vl_spec: &mut Value,
    _free_x: bool,
    _free_y: bool,
) -> Result<()> {
    // ratio - not yet implemented
    Ok(())
}

/// Apply Polar projection transformation (bar->arc, point->arc with radius)
fn apply_polar_project(
    project: &Projection,
    spec: &Plot,
    data: &DataFrame,
    vl_spec: &mut Value,
) -> Result<DataFrame> {
    // Get theta field (defaults to 'y')
    let theta_field = project
        .properties
        .get("theta")
        .and_then(|v| match v {
            ParameterValue::String(s) => Some(s.clone()),
            _ => None,
        })
        .unwrap_or_else(|| "y".to_string());

    // Get start angle in degrees (defaults to 0 = 12 o'clock)
    let start_degrees = project
        .properties
        .get("start")
        .and_then(|v| match v {
            ParameterValue::Number(n) => Some(*n),
            _ => None,
        })
        .unwrap_or(0.0);

    // Convert degrees to radians for Vega-Lite
    let start_radians = start_degrees * std::f64::consts::PI / 180.0;

    // Convert geoms to polar equivalents
    convert_geoms_to_polar(spec, vl_spec, &theta_field, start_radians)?;

    // No DataFrame transformation needed - Vega-Lite handles polar math
    Ok(data.clone())
}

/// Convert geoms to polar equivalents (bar->arc, point->arc with radius)
fn convert_geoms_to_polar(
    spec: &Plot,
    vl_spec: &mut Value,
    theta_field: &str,
    start_radians: f64,
) -> Result<()> {
    if let Some(layers) = vl_spec.get_mut("layer") {
        if let Some(layers_arr) = layers.as_array_mut() {
            for layer in layers_arr {
                if let Some(mark) = layer.get_mut("mark") {
                    *mark = convert_mark_to_polar(mark, spec)?;

                    if let Some(encoding) = layer.get_mut("encoding") {
                        update_encoding_for_polar(encoding, theta_field, start_radians)?;
                    }
                }
            }
        }
    }

    Ok(())
}

/// Convert a mark type to its polar equivalent
fn convert_mark_to_polar(mark: &Value, _spec: &Plot) -> Result<Value> {
    let mark_str = if mark.is_string() {
        mark.as_str().unwrap()
    } else if let Some(mark_type) = mark.get("type") {
        mark_type.as_str().unwrap_or("bar")
    } else {
        "bar"
    };

    // Convert geom types to polar equivalents
    let polar_mark = match mark_str {
        "bar" | "col" => {
            // Bar/col in polar becomes arc (pie/donut slices)
            "arc"
        }
        "point" => {
            // Points in polar can stay as points or become arcs with radius
            // For now, keep as points (they'll plot at radius based on value)
            "point"
        }
        "line" => {
            // Lines in polar become circular/spiral lines
            "line"
        }
        "area" => {
            // Area in polar becomes arc with radius
            "arc"
        }
        _ => {
            // Other geoms: keep as-is or convert to arc
            "arc"
        }
    };

    Ok(json!(polar_mark))
}

/// Update encoding channels for polar projection
///
/// Uses theta_field to determine which aesthetic maps to theta:
/// - If theta_field is "y" (default): y → theta, x → color (standard pie chart)
/// - If theta_field is "x": x → theta, y → radius
fn update_encoding_for_polar(
    encoding: &mut Value,
    theta_field: &str,
    start_radians: f64,
) -> Result<()> {
    let enc_obj = encoding
        .as_object_mut()
        .ok_or_else(|| GgsqlError::WriterError("Encoding is not an object".to_string()))?;

    // Map the theta field to theta channel based on theta property
    if theta_field == "y" {
        // Standard pie chart: y → theta, x → color/category
        if let Some(y_enc) = enc_obj.remove("y") {
            enc_obj.insert("theta".to_string(), y_enc);
        }
        // Map x to color if not already mapped, and remove x from positional encoding
        if !enc_obj.contains_key("color") {
            if let Some(x_enc) = enc_obj.remove("x") {
                enc_obj.insert("color".to_string(), x_enc);
            }
        } else {
            // If color is already mapped, just remove x from positional encoding
            enc_obj.remove("x");
        }
    } else if theta_field == "x" {
        // Reversed: x → theta, y → radius
        if let Some(x_enc) = enc_obj.remove("x") {
            enc_obj.insert("theta".to_string(), x_enc);
        }
        if let Some(y_enc) = enc_obj.remove("y") {
            enc_obj.insert("radius".to_string(), y_enc);
        }
    }

    // Apply start angle offset to theta encoding if non-zero
    if start_radians.abs() > f64::EPSILON {
        if let Some(theta_enc) = enc_obj.get_mut("theta") {
            if let Some(theta_obj) = theta_enc.as_object_mut() {
                // Set the scale range to offset by the start angle
                // Vega-Lite theta scale default is [0, 2π], we offset it
                let end_radians = start_radians + 2.0 * std::f64::consts::PI;
                theta_obj.insert(
                    "scale".to_string(),
                    json!({
                        "range": [start_radians, end_radians]
                    }),
                );
            }
        }
    }

    Ok(())
}

