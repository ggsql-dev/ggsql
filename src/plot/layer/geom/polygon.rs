//! Polygon geom implementation

use super::{DefaultAesthetics, GeomTrait, GeomType};
use crate::plot::types::DefaultAestheticValue;

/// Polygon geom - arbitrary polygons
#[derive(Debug, Clone, Copy)]
pub struct Polygon;

impl GeomTrait for Polygon {
    fn geom_type(&self) -> GeomType {
        GeomType::Polygon
    }

    fn aesthetics(&self) -> DefaultAesthetics {
        DefaultAesthetics {
            defaults: &[
                ("x", DefaultAestheticValue::Required),
                ("y", DefaultAestheticValue::Required),
                ("fill", DefaultAestheticValue::String("#888888")),
                ("stroke", DefaultAestheticValue::String("#888888")),
                ("opacity", DefaultAestheticValue::Number(1.0)),
                ("linewidth", DefaultAestheticValue::Null),
                ("linetype", DefaultAestheticValue::Null),
            ],
        }
    }
}

impl std::fmt::Display for Polygon {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "polygon")
    }
}
