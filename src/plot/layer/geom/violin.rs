//! Violin geom implementation

use super::{GeomAesthetics, GeomTrait, GeomType, StatResult};
use crate::{
    plot::{geom::types::get_column_name, DefaultParam, DefaultParamValue, ParameterValue},
    GgsqlError, Mappings, Result,
};
use std::collections::HashMap;

/// Violin geom - violin plots (mirrored density)
#[derive(Debug, Clone, Copy)]
pub struct Violin;

impl GeomTrait for Violin {
    fn geom_type(&self) -> GeomType {
        GeomType::Violin
    }

    fn aesthetics(&self) -> GeomAesthetics {
        GeomAesthetics {
            supported: &["x", "y", "weight", "fill", "stroke", "opacity", "linewidth"],
            required: &["x", "y"],
            hidden: &["density"],
        }
    }

    fn needs_stat_transform(&self, _aesthetics: &Mappings) -> bool {
        true
    }

    fn default_params(&self) -> &'static [DefaultParam] {
        &[
            DefaultParam {
                name: "bandwidth",
                default: DefaultParamValue::Null,
            },
            DefaultParam {
                name: "adjust",
                default: DefaultParamValue::Number(1.0),
            },
            DefaultParam {
                name: "kernel",
                default: DefaultParamValue::String("gaussian"),
            },
        ]
    }

    fn default_remappings(&self) -> &'static [(&'static str, &'static str)] {
        &[("y", "y")]
    }

    fn valid_stat_columns(&self) -> &'static [&'static str] {
        &["y", "density"]
    }

    fn stat_consumed_aesthetics(&self) -> &'static [&'static str] {
        &["y", "weight"]
    }

    fn apply_stat_transform(
        &self,
        query: &str,
        _schema: &crate::plot::Schema,
        aesthetics: &Mappings,
        group_by: &[String],
        parameters: &HashMap<String, ParameterValue>,
        execute_query: &dyn Fn(&str) -> crate::Result<polars::prelude::DataFrame>,
    ) -> Result<StatResult> {
        stat_violin(query, aesthetics, group_by, parameters, execute_query)
    }
}

impl std::fmt::Display for Violin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "violin")
    }
}

fn stat_violin(
    query: &str,
    aesthetics: &Mappings,
    group_by: &[String],
    parameters: &HashMap<String, ParameterValue>,
    execute: &dyn Fn(&str) -> crate::Result<polars::prelude::DataFrame>,
) -> Result<StatResult> {
    // Verify y exists
    if get_column_name(aesthetics, "y").is_none() {
        return Err(GgsqlError::ValidationError(
            "Violin requires 'y' aesthetic mapping (continuous)".to_string(),
        ));
    }

    let mut group_by = group_by.to_vec();
    if let Some(x_col) = get_column_name(aesthetics, "x") {
        // We want to ensure x is included as a grouping
        if !group_by.contains(&x_col) {
            group_by.push(x_col);
        }
    } else {
        return Err(GgsqlError::ValidationError(
            "Violin requires 'x' aesthetic mapping (categorical)".to_string(),
        ));
    }

    super::density::stat_density(
        query,
        aesthetics,
        "y",
        group_by.as_slice(),
        parameters,
        execute,
    )
}
