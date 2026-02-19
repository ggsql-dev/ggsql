//! Ribbon geom implementation

use super::{DefaultAesthetics, GeomTrait, GeomType};
use crate::plot::types::DefaultAestheticValue;

/// Ribbon geom - confidence bands and ranges
#[derive(Debug, Clone, Copy)]
pub struct Ribbon;

impl GeomTrait for Ribbon {
    fn geom_type(&self) -> GeomType {
        GeomType::Ribbon
    }

    fn aesthetics(&self) -> DefaultAesthetics {
        DefaultAesthetics {
            defaults: &[
                ("x", DefaultAestheticValue::Required),
                ("ymin", DefaultAestheticValue::Required),
                ("ymax", DefaultAestheticValue::Required),
                ("fill", DefaultAestheticValue::String("steelblue")),
                ("stroke", DefaultAestheticValue::Null),
                ("opacity", DefaultAestheticValue::Number(0.5)),
                ("linewidth", DefaultAestheticValue::Null),
                // "linetype" // vegalite doesn't support strokeDash
            ],
        }
    }
}

impl std::fmt::Display for Ribbon {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ribbon")
    }
}
