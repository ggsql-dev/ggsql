//! Label geom implementation
use crate::plot::{DefaultParam, DefaultParamValue};

use super::{GeomAesthetics, GeomTrait, GeomType};

/// Label geom - text labels with background
#[derive(Debug, Clone, Copy)]
pub struct Label;

impl GeomTrait for Label {
    fn geom_type(&self) -> GeomType {
        GeomType::Label
    }

    fn aesthetics(&self) -> GeomAesthetics {
        GeomAesthetics {
            supported: &[
                "x", "y", "label", "fill", "stroke", "fontsize", "opacity", "family", "fontface",
                "hjust", "vjust", "angle",
            ],
            required: &["x", "y"],
            hidden: &[],
        }
    }

    fn default_params(&self) -> &'static [DefaultParam] {
        &[
            DefaultParam {
                name: "nudge_x",
                default: DefaultParamValue::Null,
            },
            DefaultParam {
                name: "nudge_y",
                default: DefaultParamValue::Null,
            },
        ]
    }
}

impl std::fmt::Display for Label {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "label")
    }
}
