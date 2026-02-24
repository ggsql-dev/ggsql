//! Arrow geom implementation

use super::{GeomAesthetics, GeomTrait, GeomType};

/// Arrow geom - line segments with arrowheads
#[derive(Debug, Clone, Copy)]
pub struct Arrow;

impl GeomTrait for Arrow {
    fn geom_type(&self) -> GeomType {
        GeomType::Arrow
    }

    fn aesthetics(&self) -> GeomAesthetics {
        GeomAesthetics {
            supported: &[
                "pos1",
                "pos2",
                "pos1end",
                "pos2end",
                "stroke",
                "linetype",
                "linewidth",
                "opacity",
            ],
            required: &["pos1", "pos2", "pos1end", "pos2end"],
            hidden: &[],
        }
    }
}

impl std::fmt::Display for Arrow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "arrow")
    }
}
