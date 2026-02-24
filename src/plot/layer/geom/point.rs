//! Point geom implementation

use super::{GeomAesthetics, GeomTrait, GeomType};

/// Point geom - scatter plots and similar
#[derive(Debug, Clone, Copy)]
pub struct Point;

impl GeomTrait for Point {
    fn geom_type(&self) -> GeomType {
        GeomType::Point
    }

    fn aesthetics(&self) -> GeomAesthetics {
        GeomAesthetics {
            supported: &[
                "pos1",
                "pos2",
                "fill",
                "stroke",
                "size",
                "shape",
                "opacity",
                "linewidth",
            ],
            required: &["pos1", "pos2"],
            hidden: &[],
        }
    }
}

impl std::fmt::Display for Point {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "point")
    }
}
