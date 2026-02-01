//! Integer transform implementation (linear with integer rounding)

use super::{TransformKind, TransformTrait};
use crate::plot::scale::breaks::{linear_breaks, minor_breaks_linear, pretty_breaks};
use crate::plot::ArrayElement;

/// Integer transform - linear scale with integer rounding
///
/// This transform works like Identity (linear) but signals that the data
/// should be cast to integer type in SQL. The transform and inverse are
/// identity functions, but breaks are rounded to integers.
#[derive(Debug, Clone, Copy)]
pub struct Integer;

impl TransformTrait for Integer {
    fn transform_kind(&self) -> TransformKind {
        TransformKind::Integer
    }

    fn name(&self) -> &'static str {
        "integer"
    }

    fn allowed_domain(&self) -> (f64, f64) {
        (f64::NEG_INFINITY, f64::INFINITY)
    }

    fn is_value_in_domain(&self, value: f64) -> bool {
        value.is_finite()
    }

    fn calculate_breaks(&self, min: f64, max: f64, n: usize, pretty: bool) -> Vec<f64> {
        // Calculate breaks then round to integers
        let breaks = if pretty {
            pretty_breaks(min, max, n)
        } else {
            linear_breaks(min, max, n)
        };
        breaks.into_iter().map(|v| v.round()).collect()
    }

    fn calculate_minor_breaks(
        &self,
        major_breaks: &[f64],
        n: usize,
        range: Option<(f64, f64)>,
    ) -> Vec<f64> {
        minor_breaks_linear(major_breaks, n, range)
            .into_iter()
            .map(|v| v.round())
            .collect()
    }

    fn transform(&self, value: f64) -> f64 {
        value
    }

    fn inverse(&self, value: f64) -> f64 {
        value
    }

    fn wrap_numeric(&self, value: f64) -> ArrayElement {
        ArrayElement::Number(value.round())
    }
}

impl std::fmt::Display for Integer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_integer_domain() {
        let t = Integer;
        let (min, max) = t.allowed_domain();
        assert!(min.is_infinite() && min.is_sign_negative());
        assert!(max.is_infinite() && max.is_sign_positive());
    }

    #[test]
    fn test_integer_is_value_in_domain() {
        let t = Integer;
        assert!(t.is_value_in_domain(0.0));
        assert!(t.is_value_in_domain(-1000.0));
        assert!(t.is_value_in_domain(1000.0));
        assert!(t.is_value_in_domain(0.00001));
        assert!(!t.is_value_in_domain(f64::INFINITY));
        assert!(!t.is_value_in_domain(f64::NAN));
    }

    #[test]
    fn test_integer_transform() {
        let t = Integer;
        assert_eq!(t.transform(1.0), 1.0);
        assert_eq!(t.transform(-5.0), -5.0);
        assert_eq!(t.transform(0.0), 0.0);
        assert_eq!(t.transform(100.5), 100.5);
    }

    #[test]
    fn test_integer_inverse() {
        let t = Integer;
        assert_eq!(t.inverse(1.0), 1.0);
        assert_eq!(t.inverse(-5.0), -5.0);
    }

    #[test]
    fn test_integer_roundtrip() {
        let t = Integer;
        for &val in &[0.0, 1.0, -1.0, 100.0, -100.0, 0.001] {
            let transformed = t.transform(val);
            let back = t.inverse(transformed);
            assert!((back - val).abs() < 1e-10, "Roundtrip failed for {}", val);
        }
    }

    #[test]
    fn test_integer_breaks_rounded() {
        let t = Integer;
        // Breaks should be rounded to integers
        let breaks = t.calculate_breaks(0.0, 100.0, 5, true);
        for b in &breaks {
            assert_eq!(*b, b.round(), "Break {} should be rounded", b);
        }
    }

    #[test]
    fn test_integer_wrap_numeric() {
        let t = Integer;
        // wrap_numeric should round to integer
        assert_eq!(t.wrap_numeric(5.5), ArrayElement::Number(6.0));
        assert_eq!(t.wrap_numeric(5.4), ArrayElement::Number(5.0));
        assert_eq!(t.wrap_numeric(-2.7), ArrayElement::Number(-3.0));
    }

    #[test]
    fn test_integer_default_minor_break_count() {
        let t = Integer;
        assert_eq!(t.default_minor_break_count(), 1);
    }
}
