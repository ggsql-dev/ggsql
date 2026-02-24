//! Ribbon geom implementation

use super::{GeomAesthetics, GeomTrait, GeomType};

/// Ribbon geom - confidence bands and ranges
#[derive(Debug, Clone, Copy)]
pub struct Ribbon;

impl GeomTrait for Ribbon {
    fn geom_type(&self) -> GeomType {
        GeomType::Ribbon
    }

    fn aesthetics(&self) -> GeomAesthetics {
        GeomAesthetics {
            supported: &[
                "pos1",
                "pos2min",
                "pos2max",
                "fill",
                "stroke",
                "opacity",
                "linewidth",
                // "linetype" // vegalite doesn't support strokeDash
            ],
            required: &["pos1", "pos2min", "pos2max"],
            hidden: &[],
        }
    }
}

impl std::fmt::Display for Ribbon {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ribbon")
    }
}
