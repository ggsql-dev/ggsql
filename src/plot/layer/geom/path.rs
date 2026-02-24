//! Path geom implementation

use super::{GeomAesthetics, GeomTrait, GeomType};

/// Path geom - connected line segments in order
#[derive(Debug, Clone, Copy)]
pub struct Path;

impl GeomTrait for Path {
    fn geom_type(&self) -> GeomType {
        GeomType::Path
    }

    fn aesthetics(&self) -> GeomAesthetics {
        GeomAesthetics {
            supported: &["pos1", "pos2", "stroke", "linetype", "linewidth", "opacity"],
            required: &["pos1", "pos2"],
            hidden: &[],
        }
    }
}

impl std::fmt::Display for Path {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "path")
    }
}
