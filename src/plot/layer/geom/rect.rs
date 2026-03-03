//! Rect geom implementation with flexible parameter specification

use std::collections::HashMap;

use super::types::get_column_name;
use super::{DefaultAesthetics, DefaultParam, DefaultParamValue, GeomTrait, GeomType, StatResult};
use crate::naming;
use crate::plot::types::{DefaultAestheticValue, ParameterValue};
use crate::{DataFrame, GgsqlError, Mappings, Result};

use super::types::Schema;

/// Rect geom - rectangles with flexible parameter specification
///
/// Supports multiple ways to specify rectangles:
/// - X-direction: any 2 of {x (center), width, xmin, xmax}
/// - Y-direction: any 2 of {y (center), height, ymin, ymax}
///
/// For continuous scales, computes xmin/xmax and ymin/ymax
/// For discrete scales, uses x/y with width/height as band fractions
#[derive(Debug, Clone, Copy)]
pub struct Rect;

impl GeomTrait for Rect {
    fn geom_type(&self) -> GeomType {
        GeomType::Rect
    }

    fn aesthetics(&self) -> DefaultAesthetics {
        DefaultAesthetics {
            defaults: &[
                // All positional aesthetics are optional inputs (Null)
                // They become Delayed after stat transform
                ("pos1", DefaultAestheticValue::Null),       // x (center)
                ("pos1min", DefaultAestheticValue::Null),    // xmin
                ("pos1max", DefaultAestheticValue::Null),    // xmax
                ("width", DefaultAestheticValue::Null),      // width (aesthetic, can map to column)
                ("pos2", DefaultAestheticValue::Null),       // y (center)
                ("pos2min", DefaultAestheticValue::Null),    // ymin
                ("pos2max", DefaultAestheticValue::Null),    // ymax
                ("height", DefaultAestheticValue::Null),     // height (aesthetic, can map to column)
                // Visual aesthetics
                ("fill", DefaultAestheticValue::String("black")),
                ("stroke", DefaultAestheticValue::String("black")),
                ("opacity", DefaultAestheticValue::Number(1.0)),
                ("linewidth", DefaultAestheticValue::Number(1.0)),
                ("linetype", DefaultAestheticValue::String("solid")),
            ],
        }
    }

    fn default_remappings(&self) -> &'static [(&'static str, DefaultAestheticValue)] {
        &[
            // For continuous scales: remap to min/max
            ("pos1min", DefaultAestheticValue::Column("pos1min")),
            ("pos1max", DefaultAestheticValue::Column("pos1max")),
            ("pos2min", DefaultAestheticValue::Column("pos2min")),
            ("pos2max", DefaultAestheticValue::Column("pos2max")),
            // For discrete scales: remap to center
            ("pos1", DefaultAestheticValue::Column("pos1")),
            ("pos2", DefaultAestheticValue::Column("pos2")),
        ]
    }

    fn valid_stat_columns(&self) -> &'static [&'static str] {
        &["pos1", "pos2", "pos1min", "pos1max", "pos2min", "pos2max"]
    }

    fn default_params(&self) -> &'static [DefaultParam] {
        &[
            DefaultParam {
                name: "width",
                default: DefaultParamValue::Number(0.9),
            },
            DefaultParam {
                name: "height",
                default: DefaultParamValue::Number(0.9),
            },
        ]
    }

    fn stat_consumed_aesthetics(&self) -> &'static [&'static str] {
        &[
            "pos1", "pos1min", "pos1max", "width", "pos2", "pos2min", "pos2max", "height",
        ]
    }

    fn needs_stat_transform(&self, _aesthetics: &Mappings) -> bool {
        // Always apply stat transform to validate and consolidate parameters
        true
    }

    fn apply_stat_transform(
        &self,
        query: &str,
        schema: &Schema,
        aesthetics: &Mappings,
        group_by: &[String],
        parameters: &HashMap<String, ParameterValue>,
        _execute_query: &dyn Fn(&str) -> Result<DataFrame>,
    ) -> Result<StatResult> {
        stat_rect(query, schema, aesthetics, group_by, parameters)
    }
}

impl std::fmt::Display for Rect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "rect")
    }
}

/// Statistical transformation for rect: consolidate parameters and compute min/max
fn stat_rect(
    query: &str,
    schema: &Schema,
    aesthetics: &Mappings,
    group_by: &[String],
    _parameters: &HashMap<String, ParameterValue>,
) -> Result<StatResult> {
    // Extract x-direction aesthetics
    let x = get_column_name(aesthetics, "pos1");
    let xmin = get_column_name(aesthetics, "pos1min");
    let xmax = get_column_name(aesthetics, "pos1max");
    let width = get_column_name(aesthetics, "width");

    // Extract y-direction aesthetics
    let y = get_column_name(aesthetics, "pos2");
    let ymin = get_column_name(aesthetics, "pos2min");
    let ymax = get_column_name(aesthetics, "pos2max");
    let height = get_column_name(aesthetics, "height");

    // Detect if x and y are discrete by checking schema
    let is_x_discrete = x
        .as_ref()
        .and_then(|col| schema.iter().find(|c| c.name == *col))
        .map(|c| c.is_discrete)
        .unwrap_or(false);
    let is_y_discrete = y
        .as_ref()
        .and_then(|col| schema.iter().find(|c| c.name == *col))
        .map(|c| c.is_discrete)
        .unwrap_or(false);

    // Generate SQL expressions based on parameter combinations
    // Validation (exactly 2 params, discrete + min/max check) happens inside
    let (x_expr_min, x_expr_max) = generate_position_expressions(
        x.as_deref(),
        xmin.as_deref(),
        xmax.as_deref(),
        width.as_deref(),
        is_x_discrete,
        "x",
    )?;
    let (y_expr_min, y_expr_max) = generate_position_expressions(
        y.as_deref(),
        ymin.as_deref(),
        ymax.as_deref(),
        height.as_deref(),
        is_y_discrete,
        "y",
    )?;

    // Build stat column names
    let stat_x = naming::stat_column("pos1");
    let stat_xmin = naming::stat_column("pos1min");
    let stat_xmax = naming::stat_column("pos1max");
    let stat_y = naming::stat_column("pos2");
    let stat_ymin = naming::stat_column("pos2min");
    let stat_ymax = naming::stat_column("pos2max");

    // Build SELECT list
    let mut select_parts = vec![];

    // Add group_by columns first
    if !group_by.is_empty() {
        select_parts.push(group_by.join(", "));
    }

    // Add position columns based on discrete vs continuous
    if is_x_discrete {
        select_parts.push(format!("{} AS {}", x_expr_min, stat_x));
    } else {
        select_parts.push(format!("{} AS {}", x_expr_min, stat_xmin));
        select_parts.push(format!("{} AS {}", x_expr_max, stat_xmax));
    }

    if is_y_discrete {
        select_parts.push(format!("{} AS {}", y_expr_min, stat_y));
    } else {
        select_parts.push(format!("{} AS {}", y_expr_min, stat_ymin));
        select_parts.push(format!("{} AS {}", y_expr_max, stat_ymax));
    }

    let select_list = select_parts.join(", ");

    // Build transformed query
    let transformed_query = format!(
        "SELECT {} FROM ({}) AS __ggsql_rect_stat__",
        select_list, query
    );

    // Return stat columns based on discrete vs continuous
    let stat_columns = match (is_x_discrete, is_y_discrete) {
        (true, true) => vec!["pos1".to_string(), "pos2".to_string()],
        (true, false) => vec![
            "pos1".to_string(),
            "pos2min".to_string(),
            "pos2max".to_string(),
        ],
        (false, true) => vec![
            "pos1min".to_string(),
            "pos1max".to_string(),
            "pos2".to_string(),
        ],
        (false, false) => vec![
            "pos1min".to_string(),
            "pos1max".to_string(),
            "pos2min".to_string(),
            "pos2max".to_string(),
        ],
    };

    Ok(StatResult::Transformed {
        query: transformed_query,
        stat_columns,
        dummy_columns: vec![],
        consumed_aesthetics: vec![
            "pos1".to_string(),
            "pos1min".to_string(),
            "pos1max".to_string(),
            "width".to_string(),
            "pos2".to_string(),
            "pos2min".to_string(),
            "pos2max".to_string(),
            "height".to_string(),
        ],
    })
}

/// Generate SQL expressions for position min/max based on parameter combinations
///
/// Returns (min_expr, max_expr) or (center_expr, center_expr) for discrete
///
/// Validates:
/// - Discrete scales cannot use min/max aesthetics
/// - Exactly 2 parameters provided (via match statement)
fn generate_position_expressions(
    center: Option<&str>,
    min: Option<&str>,
    max: Option<&str>,
    size: Option<&str>,
    is_discrete: bool,
    axis: &str,
) -> Result<(String, String)> {
    // Validate: discrete scales cannot use min/max
    if is_discrete && (min.is_some() || max.is_some()) {
        return Err(GgsqlError::ValidationError(format!(
            "Cannot use {}min/{}max with discrete {} aesthetic. Use {} + {} instead.",
            axis,
            axis,
            axis,
            axis,
            if axis == "x" { "width" } else { "height" }
        )));
    }

    // For discrete, only center + size is valid
    if is_discrete {
        if let (Some(c), Some(_)) = (center, size) {
            return Ok((c.to_string(), c.to_string()));
        }
        return Err(GgsqlError::ValidationError(format!(
            "Discrete {} requires {} and {}.",
            axis,
            axis,
            if axis == "x" { "width" } else { "height" }
        )));
    }

    // For continuous, handle all 6 combinations
    // The _ arm catches invalid parameter counts (not exactly 2)
    match (center, min, max, size) {
        // Case 1: min + max
        (None, Some(min_col), Some(max_col), None) => {
            Ok((min_col.to_string(), max_col.to_string()))
        }
        // Case 2: center + size
        (Some(c), None, None, Some(s)) => Ok((
            format!("({} - {} / 2.0)", c, s),
            format!("({} + {} / 2.0)", c, s),
        )),
        // Case 3: center + min
        (Some(c), Some(min_col), None, None) => {
            Ok((min_col.to_string(), format!("(2 * {} - {})", c, min_col)))
        }
        // Case 4: center + max
        (Some(c), None, Some(max_col), None) => {
            Ok((format!("(2 * {} - {})", c, max_col), max_col.to_string()))
        }
        // Case 5: min + size
        (None, Some(min_col), None, Some(s)) => {
            Ok((min_col.to_string(), format!("({} + {})", min_col, s)))
        }
        // Case 6: max + size
        (None, None, Some(max_col), Some(s)) => {
            Ok((format!("({} - {})", max_col, s), max_col.to_string()))
        }
        // Invalid: wrong number of parameters or invalid combination
        _ => Err(GgsqlError::ValidationError(format!(
            "Rect requires exactly 2 {}-direction parameters from {{{}, {}min, {}max, {}}}.",
            axis,
            axis,
            axis,
            axis,
            if axis == "x" { "width" } else { "height" }
        ))),
    }
}
