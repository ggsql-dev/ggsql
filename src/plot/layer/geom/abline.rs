//! AbLine geom implementation

use super::{DefaultAesthetics, DefaultParam, DefaultParamValue, GeomTrait, GeomType};
use crate::plot::types::DefaultAestheticValue;

/// AbLine geom - lines with slope and intercept
#[derive(Debug, Clone, Copy)]
pub struct AbLine;

impl GeomTrait for AbLine {
    fn geom_type(&self) -> GeomType {
        GeomType::AbLine
    }

    fn aesthetics(&self) -> DefaultAesthetics {
        DefaultAesthetics {
            defaults: &[
                ("slope", DefaultAestheticValue::Required),
                ("intercept", DefaultAestheticValue::Required),
                ("stroke", DefaultAestheticValue::String("black")),
                ("linewidth", DefaultAestheticValue::Number(1.0)),
                ("opacity", DefaultAestheticValue::Number(1.0)),
                ("linetype", DefaultAestheticValue::String("solid")),
            ],
        }
    }

    fn default_params(&self) -> &'static [DefaultParam] {
        &[DefaultParam {
            name: "position",
            default: DefaultParamValue::String("identity"),
        }]
    }
}

impl std::fmt::Display for AbLine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "abline")
    }
}
