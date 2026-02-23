//! Flip coordinate system implementation

use super::{CoordKind, CoordTrait};

/// Flip coordinate system - swaps x and y axes
#[derive(Debug, Clone, Copy)]
pub struct Flip;

impl CoordTrait for Flip {
    fn coord_kind(&self) -> CoordKind {
        CoordKind::Flip
    }

    fn name(&self) -> &'static str {
        "flip"
    }

    fn allowed_properties(&self) -> &'static [&'static str] {
        &[] // Flip only allows aesthetic properties
    }

    fn allows_aesthetic_properties(&self) -> bool {
        true
    }
}

impl std::fmt::Display for Flip {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plot::{ArrayElement, ParameterValue};
    use std::collections::HashMap;

    #[test]
    fn test_flip_properties() {
        let flip = Flip;
        assert_eq!(flip.coord_kind(), CoordKind::Flip);
        assert_eq!(flip.name(), "flip");
        assert!(flip.allows_aesthetic_properties());
    }

    #[test]
    fn test_flip_no_specific_properties() {
        let flip = Flip;
        assert!(flip.allowed_properties().is_empty());
    }

    #[test]
    fn test_flip_accepts_aesthetic_properties() {
        let flip = Flip;
        let mut props = HashMap::new();
        props.insert(
            "color".to_string(),
            ParameterValue::Array(vec![
                ArrayElement::String("red".to_string()),
                ArrayElement::String("blue".to_string()),
            ]),
        );

        let resolved = flip.resolve_properties(&props);
        assert!(resolved.is_ok());
    }

    #[test]
    fn test_flip_rejects_xlim() {
        let flip = Flip;
        let mut props = HashMap::new();
        props.insert(
            "xlim".to_string(),
            ParameterValue::Array(vec![ArrayElement::Number(0.0), ArrayElement::Number(100.0)]),
        );

        let resolved = flip.resolve_properties(&props);
        assert!(resolved.is_err());
        let err = resolved.unwrap_err();
        assert!(err.contains("xlim"));
        assert!(err.contains("not valid"));
    }

    #[test]
    fn test_flip_rejects_ylim() {
        let flip = Flip;
        let mut props = HashMap::new();
        props.insert(
            "ylim".to_string(),
            ParameterValue::Array(vec![ArrayElement::Number(0.0), ArrayElement::Number(100.0)]),
        );

        let resolved = flip.resolve_properties(&props);
        assert!(resolved.is_err());
        let err = resolved.unwrap_err();
        assert!(err.contains("ylim"));
        assert!(err.contains("not valid"));
    }

    #[test]
    fn test_flip_rejects_theta() {
        let flip = Flip;
        let mut props = HashMap::new();
        props.insert("theta".to_string(), ParameterValue::String("y".to_string()));

        let resolved = flip.resolve_properties(&props);
        assert!(resolved.is_err());
        let err = resolved.unwrap_err();
        assert!(err.contains("theta"));
        assert!(err.contains("not valid"));
    }
}
