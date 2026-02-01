//! Boolean transform implementation (for discrete scales)

use super::{TransformKind, TransformTrait};
use crate::plot::ArrayElement;

/// Boolean transform - casts values to boolean for discrete scales
#[derive(Debug, Clone, Copy)]
pub struct Bool;

impl TransformTrait for Bool {
    fn transform_kind(&self) -> TransformKind {
        TransformKind::Bool
    }

    fn name(&self) -> &'static str {
        "bool"
    }

    fn allowed_domain(&self) -> (f64, f64) {
        (f64::NEG_INFINITY, f64::INFINITY)
    }

    fn is_value_in_domain(&self, value: f64) -> bool {
        value.is_finite()
    }

    fn calculate_breaks(&self, _min: f64, _max: f64, _n: usize, _pretty: bool) -> Vec<f64> {
        // Bool transform is for discrete scales - no breaks calculation
        Vec::new()
    }

    fn calculate_minor_breaks(
        &self,
        _major_breaks: &[f64],
        _n: usize,
        _range: Option<(f64, f64)>,
    ) -> Vec<f64> {
        // Bool transform is for discrete scales - no minor breaks
        Vec::new()
    }

    fn transform(&self, value: f64) -> f64 {
        // Pass-through - bool transform doesn't apply numeric transformations
        value
    }

    fn inverse(&self, value: f64) -> f64 {
        // Pass-through - bool transform doesn't apply numeric transformations
        value
    }

    fn wrap_numeric(&self, value: f64) -> ArrayElement {
        // Convert numeric values to boolean (non-zero = true)
        ArrayElement::Boolean(value != 0.0)
    }
}

impl std::fmt::Display for Bool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bool_transform_kind() {
        let t = Bool;
        assert_eq!(t.transform_kind(), TransformKind::Bool);
        assert_eq!(t.name(), "bool");
    }

    #[test]
    fn test_bool_domain() {
        let t = Bool;
        let (min, max) = t.allowed_domain();
        assert!(min.is_infinite() && min.is_sign_negative());
        assert!(max.is_infinite() && max.is_sign_positive());
    }

    #[test]
    fn test_bool_is_value_in_domain() {
        let t = Bool;
        assert!(t.is_value_in_domain(0.0));
        assert!(t.is_value_in_domain(1.0));
        assert!(t.is_value_in_domain(-1.0));
        assert!(!t.is_value_in_domain(f64::INFINITY));
        assert!(!t.is_value_in_domain(f64::NAN));
    }

    #[test]
    fn test_bool_transform_passthrough() {
        let t = Bool;
        assert_eq!(t.transform(1.0), 1.0);
        assert_eq!(t.transform(0.0), 0.0);
        assert_eq!(t.inverse(1.0), 1.0);
    }

    #[test]
    fn test_bool_wrap_numeric() {
        let t = Bool;
        // Non-zero values become true
        assert_eq!(t.wrap_numeric(1.0), ArrayElement::Boolean(true));
        assert_eq!(t.wrap_numeric(-1.0), ArrayElement::Boolean(true));
        assert_eq!(t.wrap_numeric(42.0), ArrayElement::Boolean(true));
        // Zero becomes false
        assert_eq!(t.wrap_numeric(0.0), ArrayElement::Boolean(false));
    }

    #[test]
    fn test_bool_breaks_empty() {
        let t = Bool;
        // Bool transform doesn't calculate breaks
        assert!(t.calculate_breaks(0.0, 1.0, 2, true).is_empty());
        assert!(t.calculate_minor_breaks(&[0.0, 1.0], 1, None).is_empty());
    }
}
