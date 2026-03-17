//! Linear geom implementation

use super::{
    DefaultAesthetics, DefaultParam, DefaultParamValue, GeomTrait, GeomType, ParamConstraint,
};
use crate::plot::layer::orientation::{ALIGNED, ORIENTATION_VALUES};
use crate::plot::types::DefaultAestheticValue;

/// Linear geom - lines with coefficient and intercept
#[derive(Debug, Clone, Copy)]
pub struct Linear;

impl GeomTrait for Linear {
    fn geom_type(&self) -> GeomType {
        GeomType::Linear
    }

    fn aesthetics(&self) -> DefaultAesthetics {
        DefaultAesthetics {
            defaults: &[
                ("coef", DefaultAestheticValue::Required),
                ("intercept", DefaultAestheticValue::Required),
                ("stroke", DefaultAestheticValue::String("black")),
                ("linewidth", DefaultAestheticValue::Number(1.0)),
                ("opacity", DefaultAestheticValue::Number(1.0)),
                ("linetype", DefaultAestheticValue::String("solid")),
            ],
        }
    }

    fn default_params(&self) -> &'static [DefaultParam] {
        const PARAMS: &[DefaultParam] = &[DefaultParam {
            name: "orientation",
            default: DefaultParamValue::String(ALIGNED),
            constraint: ParamConstraint::string_enum(ORIENTATION_VALUES),
        }];
        PARAMS
    }
}

impl std::fmt::Display for Linear {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "linear")
    }
}
