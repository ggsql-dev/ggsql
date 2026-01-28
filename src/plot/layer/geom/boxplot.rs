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
        &[("value", "y")]
    }

    fn apply_stat_transform(
        &self,
        query: &str,
        schema: &crate::plot::Schema,
        aesthetics: &Mappings,
        group_by: &[String],
        parameters: &HashMap<String, ParameterValue>,
        _execute_query: &dyn Fn(&str) -> Result<DataFrame>,
    ) -> Result<StatResult> {
        stat_boxplot(query, schema, aesthetics, group_by, parameters)
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
) -> Result<StatResult> {
    // Fetch coef parameter
    let coef = match parameters.get("coef") {
        Some(ParameterValue::Number(num)) => num,
        _ => {
            return Err(GgsqlError::InternalError(
                "The 'coef' boxplot parameter must be a numeric value.".to_string(),
            ))
        }
    };

    // Fetch outliers parameter
    let outliers = match parameters.get("outliers") {
        Some(ParameterValue::Boolean(draw)) => draw,
        _ => {
            return Err(GgsqlError::InternalError(
                "The 'outliers' parameter must be `true` or `false`.".to_string(),
            ))
        }
    };

    // Determine horizontal or vertical boxplot
    let (value_col, group_col) = set_boxplot_orientation(aesthetics, schema, parameters)?;

    // The `groups` vector is never empty, it contains at least the opposite axis as column
    // This absolves us from every having to guard against empty groups
    let mut groups = group_by.to_vec();
    if !groups.contains(&group_col) {
        groups.push(group_col);
    }
    if groups.is_empty() {
        // We should never end up here, but this is just to enforce the assumption above.
        return Err(GgsqlError::InternalError(
            "Boxplots cannot have empty groups".to_string(),
        ));
    }

    // Query for boxplot summary statistics
    let summary = boxplot_sql_compute_summary(query, &groups, &value_col, coef);
    let stats_query = boxplot_sql_append_outliers(&summary, &groups, &value_col, query, outliers);

    Ok(StatResult::Transformed {
        query: stats_query,
        stat_columns: vec!["type".to_string(), "value".to_string()],
        dummy_columns: vec![],
        consumed_aesthetics: vec!["y".to_string()],
    })
}

fn boxplot_sql_assign_quartiles(from: &str, groups: &[String], value: &str) -> String {
    // Selects all relevant columns and adds a quartile column.
    // NTILE(4) may create uneven groups
    format!(
        "SELECT
          {value},
          {groups},
          NTILE(4) OVER (PARTITION BY {groups} ORDER BY {value} ASC) AS _Q
        FROM ({from})
        WHERE {value} IS NOT NULL",
        value = value,
        groups = groups.join(", "),
        from = from
    )
}

fn boxplot_sql_quartile_minmax(from: &str, groups: &[String], value: &str) -> String {
    // Compute the min and max for every quartile.
    // The verbosity here is to pivot the table to a wide format.
    // The output is a table with 1 row per groups annotated with quartile metrics
    format!(
        "SELECT
          MIN(CASE WHEN _Q = 1 THEN {value} END) AS Q1_min,
          MAX(CASE WHEN _Q = 1 THEN {value} END) AS Q1_max,
          MIN(CASE WHEN _Q = 2 THEN {value} END) AS Q2_min,
          MAX(CASE WHEN _Q = 2 THEN {value} END) AS Q2_max,
          MIN(CASE WHEN _Q = 3 THEN {value} END) AS Q3_min,
          MAX(CASE WHEN _Q = 3 THEN {value} END) AS Q3_max,
          MIN(CASE WHEN _Q = 4 THEN {value} END) AS Q4_min,
          MAX(CASE WHEN _Q = 4 THEN {value} END) AS Q4_max,
          {groups}
        FROM ({from})
        GROUP BY {groups}",
        groups = groups.join(", "),
        value = value,
        from = from
    )
}

fn boxplot_sql_compute_fivenum(from: &str, groups: &[String], coef: &f64) -> String {
    // Here we compute the 5 statistics:
    // * lower: lower whisker
    // * upper: upper whisker
    // * q1: box start
    // * q3: box end
    // * median
    // We're assuming equally sized quartiles here, but we may have 1-member
    // differences. For large datasets this shouldn't be a problem, but in smaller
    // datasets one might notice.
    format!(
        "SELECT
          *,
          GREATEST(q1 - {coef} * (q3 - q1), min) AS lower,
          LEAST(   q3 + {coef} * (q3 - q1), max) AS upper
        FROM (
          SELECT
            Q1_min AS min,
            Q4_max AS max,
            (Q2_max + Q3_min) / 2.0 AS median,
            (Q1_max + Q2_min) / 2.0 AS q1,
            (Q3_max + Q4_min) / 2.0 AS q3,
            {groups}
          FROM ({from})
        )",
        coef = coef,
        groups = groups.join(", "),
        from = from
    )
}

fn boxplot_sql_compute_summary(from: &str, groups: &[String], value: &str, coef: &f64) -> String {
    let query = boxplot_sql_assign_quartiles(from, groups, value);
    let query = boxplot_sql_quartile_minmax(&query, groups, value);
    boxplot_sql_compute_fivenum(&query, groups, coef)
}

fn boxplot_sql_filter_outliers(groups: &[String], value: &str, from: &str) -> String {
    let mut join_pairs = Vec::new();
    let mut keep_columns = Vec::new();
    for column in groups {
        join_pairs.push(format!("raw.{} = summary.{}", column, column));
        keep_columns.push(format!("raw.{}", column));
    }

    // We're joining outliers with the summary to use the lower/upper whisker
    // values as a filter
    format!(
        "SELECT
          raw.{value} AS value,
          'outlier' AS type,
          {groups}
        FROM ({from}) raw
        JOIN summary ON {pairs}
        WHERE raw.{value} NOT BETWEEN summary.lower AND summary.upper",
        value = value,
        groups = keep_columns.join(", "),
        pairs = join_pairs.join(" AND "),
        from = from
    )
}

fn boxplot_sql_append_outliers(
    from: &str,
    groups: &[String],
    value: &str,
    raw_query: &str,
    draw_outliers: &bool,
) -> String {
    let value_name = naming::stat_column("value");
    let type_name = naming::stat_column("type");

    if !*draw_outliers {
        // Just reshape summary to long format
        let sql = format!(
            "SELECT {groups}, type AS {type_name}, value AS {value_name}
            FROM ({summary})
            UNPIVOT(value FOR type IN (min, max, median, q1, q3, upper, lower))",
            groups = groups.join(", "),
            value_name = value_name,
            type_name = type_name,
            summary = from
        );
        return sql;
    }

    // Grab query for outliers. Outcome is long format data.
    let outliers = boxplot_sql_filter_outliers(groups, value, raw_query);

    // Reshape summary to long format and combine with outliers in single table
    format!(
        "WITH
        summary AS (
          {summary}
        ),
        outliers AS (
          {outliers}
        )
        (
          SELECT {groups}, type AS {type_name}, value AS {value_name}
          FROM summary
          UNPIVOT(value FOR type IN (min, max, median, q1, q3, upper, lower))
        )
        UNION ALL
        (
          SELECT {groups}, type AS {type_name}, value AS {value_name}
          FROM outliers
        )
        ",
        summary = from,
        outliers = outliers,
        type_name = type_name,
        value_name = value_name,
        groups = groups.join(", ")
    )
}

// Tries to figure out wether we should have horizontal or vertical boxplots.
// If the data is ambiguous, because x *and* y are both discrete or both continuous,
//  we fallback to vertical boxplots.
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
