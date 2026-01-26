//! Boxplot geom implementation

use std::collections::HashMap;

use super::{GeomAesthetics, GeomTrait, GeomType};
use crate::{
    naming,
    plot::{
        geom::types::get_column_name, DefaultParam, DefaultParamValue, ParameterValue, StatResult,
    },
    DataFrame, GgsqlError, Mappings, Result,
};

/// Boxplot geom - box and whisker plots
#[derive(Debug, Clone, Copy)]
pub struct Boxplot;

impl GeomTrait for Boxplot {
    fn geom_type(&self) -> GeomType {
        GeomType::Boxplot
    }

    fn aesthetics(&self) -> GeomAesthetics {
        GeomAesthetics {
            supported: &["x", "y", "color", "colour", "fill", "stroke", "opacity"],
            required: &["x", "y"],
            // Internal aesthetics produced by stat transform
            hidden: &["ymin", "ymax", "y", "q1", "q3"],
        }
    }

    fn stat_consumed_aesthetics(&self) -> &'static [&'static str] {
        &["y"]
    }

    fn needs_stat_transform(&self, _aesthetics: &Mappings) -> bool {
        true
    }

    fn default_params(&self) -> &'static [super::DefaultParam] {
        &[
            DefaultParam {
                name: "outliers",
                default: super::DefaultParamValue::Boolean(true),
            },
            DefaultParam {
                name: "coef",
                default: DefaultParamValue::Number(1.5),
            },
            DefaultParam {
                name: "orientation",
                default: super::DefaultParamValue::Null,
            },
            DefaultParam {
                name: "width",
                default: DefaultParamValue::Number(0.9),
            },
        ]
    }

    fn default_remappings(&self) -> &'static [(&'static str, &'static str)] {
        &[
            ("lower", "ymin"),
            ("q1", "q1"),
            ("median", "y"),
            ("q3", "q3"),
            ("upper", "ymax"),
        ]
    }

    fn apply_stat_transform(
        &self,
        query: &str,
        schema: &crate::plot::Schema,
        aesthetics: &Mappings,
        group_by: &[String],
        parameters: &HashMap<String, ParameterValue>,
        execute_query: &dyn Fn(&str) -> Result<DataFrame>,
    ) -> Result<StatResult> {
        stat_boxplot(
            query,
            schema,
            aesthetics,
            group_by,
            parameters,
            execute_query,
        )
    }
}

impl std::fmt::Display for Boxplot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "boxplot")
    }
}

fn stat_boxplot(
    query: &str,
    schema: &crate::plot::Schema,
    aesthetics: &Mappings,
    group_by: &[String],
    parameters: &HashMap<String, ParameterValue>,
    _execute_query: &dyn Fn(&str) -> Result<DataFrame>,
) -> Result<StatResult> {
    let (value_col, group_col) = set_boxplot_orientation(aesthetics, schema, parameters)?;

    // Sort out grouping
    let mut groups = group_by.to_vec();
    if groups.contains(&group_col) {
        // Regular groups should not contain x-axis group
        groups.retain(|s| s != &group_col);
    }
    let non_axis_groups = if groups.is_empty() {
        String::new()
    } else {
        format!(", {}", groups.join(", "))
    };
    let group_by_clause = format!("GROUP BY {}{}", group_col, non_axis_groups);

    // Get coef parameter for whisker calculation
    let coef;
    if let Some(ParameterValue::Number(num)) = parameters.get("coef") {
        coef = num;
    } else {
        return Err(GgsqlError::InternalError(
            "The 'coef' boxplot parameter must a numeric value".to_string(),
        ));
    }

    // Generate prefixed column names for stat outputs
    let stat_lower = naming::stat_column("lower");
    let stat_q1 = naming::stat_column("q1");
    let stat_median = naming::stat_column("median");
    let stat_q3 = naming::stat_column("q3");
    let stat_upper = naming::stat_column("upper");

    // Create 5-number summary query with IQR and whisker endpoints
    let stats_query = format!(
        "SELECT
          MIN({v})                 AS min,
          quantile_cont({v}, 0.25) AS q1,
          median({v})              AS median,
          quantile_cont({v}, 0.75) AS q3,
          MAX({v})                 AS max,
          {group_col}
          {non_axis_groups}
        FROM ({query})
        {group_by_clause}
        ",
        v = value_col,
        group_col = group_col,
        non_axis_groups = non_axis_groups,
        query = query,
        group_by_clause = group_by_clause
    );

    // Add IQR calculation and whisker endpoints with proper prefixes
    let stats_query = format!(
        "SELECT
          *,
          q3 - q1 AS iqr,
          GREATEST(q1 - {coef} * (q3 - q1), min) AS {lower},
          q1 AS {q1},
          median AS {median},
          q3 AS {q3},
          LEAST(q3 + {coef} * (q3 - q1), max) AS {upper}
        FROM (
          {stats_query}
         ) s
        ",
        coef = coef,
        lower = stat_lower,
        q1 = stat_q1,
        median = stat_median,
        q3 = stat_q3,
        upper = stat_upper,
        stats_query = stats_query
    );

    Ok(StatResult::Transformed {
        query: stats_query,
        stat_columns: vec![
            "lower".to_string(),
            "q1".to_string(),
            "median".to_string(),
            "q3".to_string(),
            "upper".to_string(),
        ],
        dummy_columns: vec![],
        consumed_aesthetics: vec!["y".to_string()],
    })
}

fn set_boxplot_orientation(
    aesthetics: &Mappings,
    schema: &crate::plot::Schema,
    parameters: &HashMap<String, ParameterValue>,
) -> Result<(String, String)> {
    let y = get_column_name(aesthetics, "y").ok_or_else(|| {
        GgsqlError::ValidationError("Boxplot requires 'y' aesthetic mapping".to_string())
    })?;
    let x = get_column_name(aesthetics, "x").ok_or_else(|| {
        GgsqlError::ValidationError("Boxplot requires 'x' aesthetic mapping".to_string())
    })?;

    let mut orientation = parameters.get("orientation").cloned();

    if orientation.is_none() {
        let mut seen_x = false;
        let mut seen_y = false;
        let mut is_discrete_x = false;
        let mut is_discrete_y = false;

        for column in schema {
            if column.name == x {
                seen_x = true;
                is_discrete_x = column.is_discrete
            }
            if column.name == y {
                seen_y = true;
                is_discrete_y = column.is_discrete
            }
        }
        if !seen_x {
            return Err(GgsqlError::InternalError(format!(
                "Missing column info for 'x' ({})",
                x
            )));
        }
        if !seen_y {
            return Err(GgsqlError::InternalError(format!(
                "Missing column info for 'y' ({})",
                y
            )));
        }

        if is_discrete_x && !is_discrete_y {
            orientation = Some(ParameterValue::String("x".to_string()));
        } else if !is_discrete_x && is_discrete_y {
            orientation = Some(ParameterValue::String("y".to_string()));
        } else {
            // We cannot reliably infer the orientation from the data types,
            // and fall back to x-orientation.
            orientation = Some(ParameterValue::String("x".to_string()));
        }
    }

    match orientation {
        Some(ParameterValue::String(s)) if s == "x" => Ok((y, x)),
        Some(ParameterValue::String(s)) if s == "y" => Ok((x, y)),
        _ => Err(GgsqlError::InternalError(format!(
            "The boxplot 'orientation' parameter must be 'x' or 'y', not {:?}",
            orientation
        ))),
    }
}
