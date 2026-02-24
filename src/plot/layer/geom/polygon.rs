//! Polygon geom implementation

use super::{GeomAesthetics, GeomTrait, GeomType};

/// Polygon geom - arbitrary polygons
#[derive(Debug, Clone, Copy)]
pub struct Polygon;

impl GeomTrait for Polygon {
    fn geom_type(&self) -> GeomType {
        GeomType::Polygon
    }

    fn aesthetics(&self) -> GeomAesthetics {
        GeomAesthetics {
            supported: &[
                "pos1",
                "pos2",
                "fill",
                "stroke",
                "opacity",
                "linewidth",
                "linetype",
            ],
            required: &["pos1", "pos2"],
            hidden: &[],
        }
    }
}

impl std::fmt::Display for Polygon {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "polygon")
    }
}
