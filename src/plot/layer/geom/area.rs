//! Area geom implementation

use crate::plot::{DefaultParam, DefaultParamValue, types::DefaultAestheticValue};

use super::{DefaultAesthetics, GeomTrait, GeomType};

/// Area geom - filled area charts
#[derive(Debug, Clone, Copy)]
pub struct Area;

impl GeomTrait for Area {
    fn geom_type(&self) -> GeomType {
        GeomType::Area
    }

    fn aesthetics(&self) -> DefaultAesthetics {
        DefaultAesthetics {
            defaults: &[
                ("x", DefaultAestheticValue::Required),
                ("y", DefaultAestheticValue::Required),
                ("fill", DefaultAestheticValue::String("steelblue")),
                ("stroke", DefaultAestheticValue::Null),
                ("opacity", DefaultAestheticValue::Number(1.0)),
                ("linewidth", DefaultAestheticValue::Null),
                // "linetype", // vegalite doesn't support strokeDash
            ],
        }
    }

    fn default_params(&self) -> &'static [DefaultParam] {
        &[DefaultParam {
            name: "stacking",
            default: DefaultParamValue::String("off"),
        }]
    }
}

impl std::fmt::Display for Area {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "area")
    }
}
