//! Text geom implementation

use crate::plot::{DefaultParam, DefaultParamValue};

use super::{GeomAesthetics, GeomTrait, GeomType};

/// Text geom - text labels at positions
#[derive(Debug, Clone, Copy)]
pub struct Text;

impl GeomTrait for Text {
    fn geom_type(&self) -> GeomType {
        GeomType::Text
    }

    fn aesthetics(&self) -> GeomAesthetics {
        GeomAesthetics {
            supported: &[
                "x", "y", "label", "stroke", "fontsize", "opacity", "family", "fontface", "hjust",
                "vjust", "angle",
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

impl std::fmt::Display for Text {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "text")
    }
}
