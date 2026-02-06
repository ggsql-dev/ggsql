//! Area geom implementation

use crate::plot::{DefaultParam, DefaultParamValue};

use super::{GeomAesthetics, GeomTrait, GeomType};

/// Area geom - filled area charts
#[derive(Debug, Clone, Copy)]
pub struct Area;

impl GeomTrait for Area {
    fn geom_type(&self) -> GeomType {
        GeomType::Area
    }

    fn aesthetics(&self) -> GeomAesthetics {
        GeomAesthetics {
            supported: &[
                "x",
                "y",
                "fill",
                "stroke",
                "opacity",
                "linewidth",
                // "linetype", // vegalite doesn't support strokeDash
            ],
            required: &["x", "y"],
            hidden: &[],
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
