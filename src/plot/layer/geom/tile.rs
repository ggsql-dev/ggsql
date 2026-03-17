//! Tile geom implementation

use super::types::POSITION_VALUES;
use super::{
    DefaultAesthetics, DefaultParam, DefaultParamValue, GeomTrait, GeomType, ParamConstraint,
};
use crate::plot::types::DefaultAestheticValue;

/// Tile geom - heatmaps and tile-based visualizations
#[derive(Debug, Clone, Copy)]
pub struct Tile;

impl GeomTrait for Tile {
    fn geom_type(&self) -> GeomType {
        GeomType::Tile
    }

    fn aesthetics(&self) -> DefaultAesthetics {
        DefaultAesthetics {
            defaults: &[
                ("pos1", DefaultAestheticValue::Required),
                ("pos2", DefaultAestheticValue::Required),
                ("fill", DefaultAestheticValue::String("black")),
                ("stroke", DefaultAestheticValue::String("black")),
                ("width", DefaultAestheticValue::Null),
                ("height", DefaultAestheticValue::Null),
                ("opacity", DefaultAestheticValue::Number(1.0)),
            ],
        }
    }

    fn default_params(&self) -> &'static [DefaultParam] {
        const PARAMS: &[DefaultParam] = &[DefaultParam {
            name: "position",
            default: DefaultParamValue::String("identity"),
            constraint: ParamConstraint::string_enum(POSITION_VALUES),
        }];
        PARAMS
    }
}

impl std::fmt::Display for Tile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "tile")
    }
}
