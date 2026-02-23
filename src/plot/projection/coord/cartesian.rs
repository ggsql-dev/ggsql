//! Cartesian coordinate system implementation

use super::{CoordKind, CoordTrait};

/// Cartesian coordinate system - standard x/y coordinates
#[derive(Debug, Clone, Copy)]
pub struct Cartesian;

impl CoordTrait for Cartesian {
    fn coord_kind(&self) -> CoordKind {
        CoordKind::Cartesian
    }

    fn name(&self) -> &'static str {
        "cartesian"
    }

    fn allowed_properties(&self) -> &'static [&'static str] {
        &["ratio"]
    }

    fn allows_aesthetic_properties(&self) -> bool {
        true
    }
}

impl std::fmt::Display for Cartesian {
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
    fn test_cartesian_properties() {
        let cartesian = Cartesian;
        assert_eq!(cartesian.coord_kind(), CoordKind::Cartesian);
        assert_eq!(cartesian.name(), "cartesian");
        assert!(cartesian.allows_aesthetic_properties());
    }

    #[test]
    fn test_cartesian_allowed_properties() {
        let cartesian = Cartesian;
        let allowed = cartesian.allowed_properties();
        assert!(allowed.contains(&"ratio"));
        // xlim/ylim removed - use SCALE x/y FROM instead
        assert!(!allowed.contains(&"xlim"));
        assert!(!allowed.contains(&"ylim"));
    }

    #[test]
    fn test_cartesian_resolve_valid_properties() {
        let cartesian = Cartesian;
        let props = HashMap::new();
        // Empty properties should resolve successfully
        let resolved = cartesian.resolve_properties(&props);
        assert!(resolved.is_ok());
    }

    #[test]
    fn test_cartesian_rejects_xlim() {
        let cartesian = Cartesian;
        let mut props = HashMap::new();
        props.insert(
            "xlim".to_string(),
            ParameterValue::Array(vec![ArrayElement::Number(0.0), ArrayElement::Number(100.0)]),
        );

        let resolved = cartesian.resolve_properties(&props);
        assert!(resolved.is_err());
        let err = resolved.unwrap_err();
        assert!(err.contains("xlim"));
        assert!(err.contains("not valid"));
    }

    #[test]
    fn test_cartesian_rejects_ylim() {
        let cartesian = Cartesian;
        let mut props = HashMap::new();
        props.insert(
            "ylim".to_string(),
            ParameterValue::Array(vec![ArrayElement::Number(0.0), ArrayElement::Number(50.0)]),
        );

        let resolved = cartesian.resolve_properties(&props);
        assert!(resolved.is_err());
        let err = resolved.unwrap_err();
        assert!(err.contains("ylim"));
        assert!(err.contains("not valid"));
    }

    #[test]
    fn test_cartesian_accepts_aesthetic_properties() {
        let cartesian = Cartesian;
        let mut props = HashMap::new();
        props.insert(
            "color".to_string(),
            ParameterValue::Array(vec![
                ArrayElement::String("red".to_string()),
                ArrayElement::String("blue".to_string()),
            ]),
        );

        let resolved = cartesian.resolve_properties(&props);
        assert!(resolved.is_ok());
    }

    #[test]
    fn test_cartesian_rejects_theta() {
        let cartesian = Cartesian;
        let mut props = HashMap::new();
        props.insert("theta".to_string(), ParameterValue::String("y".to_string()));

        let resolved = cartesian.resolve_properties(&props);
        assert!(resolved.is_err());
        let err = resolved.unwrap_err();
        assert!(err.contains("theta"));
        assert!(err.contains("not valid"));
    }
}
