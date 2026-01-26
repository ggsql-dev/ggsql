//! Scale type trait and implementations
//!
//! This module provides a trait-based design for scale types in ggsql.
//! Each scale type is implemented as its own struct, allowing for cleaner separation
//! of concerns and easier extensibility.
//!
//! # Architecture
//!
//! - `ScaleTypeKind`: Enum for pattern matching and serialization
//! - `ScaleTypeTrait`: Trait defining scale type behavior
//! - `ScaleType`: Wrapper struct holding an Arc<dyn ScaleTypeTrait>
//!
//! # Example
//!
//! ```rust,ignore
//! use ggsql::plot::scale::{ScaleType, ScaleTypeKind};
//!
//! let continuous = ScaleType::continuous();
//! assert_eq!(continuous.scale_type_kind(), ScaleTypeKind::Continuous);
//! assert_eq!(continuous.name(), "continuous");
//! ```

use polars::prelude::{Column, DataType};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use crate::plot::{ArrayElement, ParameterValue};

// Scale type implementations
mod binned;
mod continuous;
mod date;
mod datetime;
mod discrete;
mod identity;
mod time;

// Re-export scale type structs for direct access if needed
pub use binned::Binned;
pub use continuous::Continuous;
pub use date::Date;
pub use datetime::DateTime;
pub use discrete::Discrete;
pub use identity::Identity;
pub use time::Time;

/// Enum of all scale types for pattern matching and serialization
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ScaleTypeKind {
    /// Continuous numeric data
    Continuous,
    /// Categorical/discrete data
    Discrete,
    /// Binned/bucketed data
    Binned,
    /// Date data (maps to temporal type)
    Date,
    /// DateTime data (maps to temporal type)
    DateTime,
    /// Time data (maps to temporal type)
    Time,
    /// Identity scale (use inferred type)
    Identity,
}

impl std::fmt::Display for ScaleTypeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            ScaleTypeKind::Continuous => "continuous",
            ScaleTypeKind::Discrete => "discrete",
            ScaleTypeKind::Binned => "binned",
            ScaleTypeKind::Date => "date",
            ScaleTypeKind::DateTime => "datetime",
            ScaleTypeKind::Time => "time",
            ScaleTypeKind::Identity => "identity",
        };
        write!(f, "{}", s)
    }
}

/// Core trait for scale type behavior
///
/// Each scale type implements this trait. The trait is intentionally minimal
/// and backend-agnostic - no Vega-Lite or other writer-specific details.
pub trait ScaleTypeTrait: std::fmt::Debug + std::fmt::Display + Send + Sync {
    /// Returns which scale type this is (for pattern matching)
    fn scale_type_kind(&self) -> ScaleTypeKind;

    /// Canonical name for parsing and display
    fn name(&self) -> &'static str;

    /// Returns whether this scale type represents discrete/categorical data.
    /// Defaults to `true`. Overridden to return `false` for `Discrete` and `Identity`.
    fn is_discrete(&self) -> bool {
        true
    }

    /// Returns whether this scale type accepts the given data type
    fn allows_data_type(&self, dtype: &DataType) -> bool;

    /// Validate that all columns have compatible data types for this scale.
    /// Returns Ok(()) if valid, Err with details if any column is incompatible.
    fn validate_columns(&self, columns: &[&Column]) -> Result<(), String> {
        for col in columns {
            let dtype = col.dtype();
            if !self.allows_data_type(dtype) {
                return Err(format!(
                    "Column '{}' has type {:?} which is not compatible with {} scale",
                    col.name(),
                    dtype,
                    self.name()
                ));
            }
        }
        Ok(())
    }

    /// Resolve input range from user-provided range and data columns.
    ///
    /// Behavior varies by scale type:
    /// - Continuous/Binned: Compute min/max from data, merge with user range (nulls → computed)
    /// - Date/DateTime/Time: Compute min/max as ISO strings, merge with user range
    /// - Discrete: Collect unique values if no user range; error if user range contains nulls
    /// - Identity: Return Ok(None); error if user provided any input range
    ///
    /// The `properties` parameter provides access to SETTING values, including `expand`.
    fn resolve_input_range(
        &self,
        user_range: Option<&[ArrayElement]>,
        columns: &[&Column],
        properties: &HashMap<String, ParameterValue>,
    ) -> Result<Option<Vec<ArrayElement>>, String>;

    /// Get default output range for an aesthetic.
    ///
    /// Returns sensible default ranges based on the aesthetic type and scale type.
    /// For example:
    /// - color/fill + discrete → standard categorical color palette (sized to input_range length)
    /// - size + continuous → [min_size, max_size] range
    /// - opacity + continuous → [0.2, 1.0] range
    ///
    /// The input_range is provided so discrete scales can size the output appropriately.
    ///
    /// Returns Ok(None) if no default is appropriate (e.g., x/y position aesthetics).
    /// Returns Err if the palette doesn't have enough colors for the input range.
    fn default_output_range(
        &self,
        _aesthetic: &str,
        _input_range: Option<&[ArrayElement]>,
    ) -> Result<Option<Vec<ArrayElement>>, String> {
        Ok(None) // Default implementation: no default range
    }

    /// Returns list of allowed property names for SETTING clause.
    /// The aesthetic parameter allows different properties for different aesthetics.
    /// Default: empty (no properties allowed).
    fn allowed_properties(&self, _aesthetic: &str) -> &'static [&'static str] {
        &[]
    }

    /// Returns default value for a property, if any.
    /// Called by resolve_properties for allowed properties not in user input.
    /// The aesthetic parameter allows different defaults for different aesthetics.
    fn get_property_default(&self, _aesthetic: &str, _name: &str) -> Option<ParameterValue> {
        None
    }

    /// Resolve and validate properties. NOT meant to be overridden by implementations.
    /// - Validates all properties are in allowed_properties()
    /// - Applies defaults via get_property_default()
    fn resolve_properties(
        &self,
        aesthetic: &str,
        properties: &HashMap<String, ParameterValue>,
    ) -> Result<HashMap<String, ParameterValue>, String> {
        let allowed = self.allowed_properties(aesthetic);

        // Check for unknown properties
        for key in properties.keys() {
            if !allowed.contains(&key.as_str()) {
                if allowed.is_empty() {
                    return Err(format!(
                        "{} scale does not support any SETTING properties",
                        self.name()
                    ));
                }
                return Err(format!(
                    "{} scale does not support SETTING '{}'. Allowed: {}",
                    self.name(),
                    key,
                    allowed.join(", ")
                ));
            }
        }

        // Start with user properties, add defaults for missing ones
        let mut resolved = properties.clone();
        for &prop_name in allowed {
            if !resolved.contains_key(prop_name) {
                if let Some(default) = self.get_property_default(aesthetic, prop_name) {
                    resolved.insert(prop_name.to_string(), default);
                }
            }
        }

        Ok(resolved)
    }
}

/// Wrapper struct for scale type trait objects
///
/// This provides a convenient interface for working with scale types while hiding
/// the complexity of trait objects.
#[derive(Clone)]
pub struct ScaleType(Arc<dyn ScaleTypeTrait>);

impl ScaleType {
    /// Create a Continuous scale type
    pub fn continuous() -> Self {
        Self(Arc::new(Continuous))
    }

    /// Create a Discrete scale type
    pub fn discrete() -> Self {
        Self(Arc::new(Discrete))
    }

    /// Create a Binned scale type
    pub fn binned() -> Self {
        Self(Arc::new(Binned))
    }

    /// Create a Date scale type
    pub fn date() -> Self {
        Self(Arc::new(Date))
    }

    /// Create a DateTime scale type
    pub fn datetime() -> Self {
        Self(Arc::new(DateTime))
    }

    /// Create a Time scale type
    pub fn time() -> Self {
        Self(Arc::new(Time))
    }

    /// Create an Identity scale type
    pub fn identity() -> Self {
        Self(Arc::new(Identity))
    }

    /// Infer scale type from a Polars data type.
    ///
    /// Maps data types to appropriate scale types:
    /// - Numeric types (Int*, UInt*, Float*) → Continuous
    /// - Date → Date
    /// - Datetime → DateTime
    /// - Time → Time
    /// - Boolean, String, other → Discrete
    pub fn infer(dtype: &DataType) -> Self {
        match dtype {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64 => Self::continuous(),
            DataType::Date => Self::date(),
            DataType::Datetime(_, _) => Self::datetime(),
            DataType::Time => Self::time(),
            DataType::Boolean | DataType::String => Self::discrete(),
            _ => Self::discrete(),
        }
    }

    /// Create a ScaleType from a ScaleTypeKind
    pub fn from_kind(kind: ScaleTypeKind) -> Self {
        match kind {
            ScaleTypeKind::Continuous => Self::continuous(),
            ScaleTypeKind::Discrete => Self::discrete(),
            ScaleTypeKind::Binned => Self::binned(),
            ScaleTypeKind::Date => Self::date(),
            ScaleTypeKind::DateTime => Self::datetime(),
            ScaleTypeKind::Time => Self::time(),
            ScaleTypeKind::Identity => Self::identity(),
        }
    }

    /// Get the scale type kind (for pattern matching)
    pub fn scale_type_kind(&self) -> ScaleTypeKind {
        self.0.scale_type_kind()
    }

    /// Get the canonical name
    pub fn name(&self) -> &'static str {
        self.0.name()
    }

    /// Check if this scale type represents discrete/categorical data
    pub fn is_discrete(&self) -> bool {
        self.0.is_discrete()
    }

    /// Check if this scale type accepts the given data type
    pub fn allows_data_type(&self, dtype: &DataType) -> bool {
        self.0.allows_data_type(dtype)
    }

    /// Validate that all columns have compatible data types for this scale
    pub fn validate_columns(&self, columns: &[&Column]) -> Result<(), String> {
        self.0.validate_columns(columns)
    }

    /// Resolve input range from user-provided range and data columns.
    ///
    /// Delegates to the underlying scale type implementation.
    pub fn resolve_input_range(
        &self,
        user_range: Option<&[ArrayElement]>,
        columns: &[&Column],
        properties: &HashMap<String, ParameterValue>,
    ) -> Result<Option<Vec<ArrayElement>>, String> {
        self.0.resolve_input_range(user_range, columns, properties)
    }

    /// Get default output range for an aesthetic.
    ///
    /// Delegates to the underlying scale type implementation.
    pub fn default_output_range(
        &self,
        aesthetic: &str,
        input_range: Option<&[ArrayElement]>,
    ) -> Result<Option<Vec<ArrayElement>>, String> {
        self.0.default_output_range(aesthetic, input_range)
    }

    /// Resolve and validate properties.
    ///
    /// Validates all user-provided properties are allowed for this scale type,
    /// and fills in default values for missing properties.
    pub fn resolve_properties(
        &self,
        aesthetic: &str,
        properties: &HashMap<String, ParameterValue>,
    ) -> Result<HashMap<String, ParameterValue>, String> {
        self.0.resolve_properties(aesthetic, properties)
    }
}

impl std::fmt::Debug for ScaleType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ScaleType::{:?}", self.scale_type_kind())
    }
}

impl std::fmt::Display for ScaleType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl PartialEq for ScaleType {
    fn eq(&self, other: &Self) -> bool {
        self.scale_type_kind() == other.scale_type_kind()
    }
}

impl Eq for ScaleType {}

impl Serialize for ScaleType {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.scale_type_kind().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ScaleType {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let kind = ScaleTypeKind::deserialize(deserializer)?;
        Ok(ScaleType::from_kind(kind))
    }
}

// =============================================================================
// Shared helpers for input range resolution
// =============================================================================

/// Check if an aesthetic is a positional aesthetic (x, y, and variants).
/// Positional aesthetics support properties like `expand`.
pub(super) fn is_positional_aesthetic(aesthetic: &str) -> bool {
    matches!(
        aesthetic,
        "x" | "y" | "xmin" | "xmax" | "ymin" | "ymax" | "xend" | "yend"
    )
}

/// Check if input range contains any Null placeholders
pub(super) fn input_range_has_nulls(range: &[ArrayElement]) -> bool {
    range.iter().any(|e| matches!(e, ArrayElement::Null))
}

/// Merge explicit range with inferred values (replace nulls with inferred)
pub(super) fn merge_with_inferred(
    explicit: &[ArrayElement],
    inferred: &[ArrayElement],
) -> Vec<ArrayElement> {
    explicit
        .iter()
        .enumerate()
        .map(|(i, exp)| {
            if matches!(exp, ArrayElement::Null) {
                inferred.get(i).cloned().unwrap_or(ArrayElement::Null)
            } else {
                exp.clone()
            }
        })
        .collect()
}

// =============================================================================
// Expansion helpers for continuous/temporal scales
// =============================================================================

/// Default multiplicative expansion factor for continuous/temporal scales.
pub(super) const DEFAULT_EXPAND_MULT: f64 = 0.05;

/// Default additive expansion factor for continuous/temporal scales.
pub(super) const DEFAULT_EXPAND_ADD: f64 = 0.0;

/// Parse expand parameter value into (mult, add) factors.
/// Returns None if value is invalid.
///
/// Syntax:
/// - Single number: `expand => 0.05` → (0.05, 0.0)
/// - Two numbers: `expand => [0.05, 10]` → (0.05, 10.0)
pub(super) fn parse_expand_value(expand: &ParameterValue) -> Option<(f64, f64)> {
    match expand {
        ParameterValue::Number(m) => Some((*m, 0.0)),
        ParameterValue::Array(arr) if arr.len() >= 2 => {
            let mult = match &arr[0] {
                ArrayElement::Number(n) => *n,
                _ => return None,
            };
            let add = match &arr[1] {
                ArrayElement::Number(n) => *n,
                _ => return None,
            };
            Some((mult, add))
        }
        _ => None,
    }
}

/// Apply expansion to a numeric [min, max] range.
/// Returns expanded [min, max] as ArrayElements.
///
/// Formula: [min - range*mult - add, max + range*mult + add]
pub(super) fn expand_numeric_range(
    range: &[ArrayElement],
    mult: f64,
    add: f64,
) -> Vec<ArrayElement> {
    if range.len() < 2 {
        return range.to_vec();
    }

    let min = match &range[0] {
        ArrayElement::Number(n) => *n,
        _ => return range.to_vec(),
    };
    let max = match &range[1] {
        ArrayElement::Number(n) => *n,
        _ => return range.to_vec(),
    };

    let span = max - min;
    let expanded_min = min - span * mult - add;
    let expanded_max = max + span * mult + add;

    vec![
        ArrayElement::Number(expanded_min),
        ArrayElement::Number(expanded_max),
    ]
}

/// Get expand factors from properties, using defaults for continuous/temporal scales.
pub(super) fn get_expand_factors(
    properties: &HashMap<String, ParameterValue>,
) -> (f64, f64) {
    properties
        .get("expand")
        .and_then(parse_expand_value)
        .unwrap_or((DEFAULT_EXPAND_MULT, DEFAULT_EXPAND_ADD))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scale_type_creation() {
        let continuous = ScaleType::continuous();
        assert_eq!(continuous.scale_type_kind(), ScaleTypeKind::Continuous);

        let discrete = ScaleType::discrete();
        assert_eq!(discrete.scale_type_kind(), ScaleTypeKind::Discrete);

        let date = ScaleType::date();
        assert_eq!(date.scale_type_kind(), ScaleTypeKind::Date);
    }

    #[test]
    fn test_scale_type_equality() {
        let c1 = ScaleType::continuous();
        let c2 = ScaleType::continuous();
        let d1 = ScaleType::discrete();

        assert_eq!(c1, c2);
        assert_ne!(c1, d1);
    }

    #[test]
    fn test_scale_type_display() {
        assert_eq!(format!("{}", ScaleType::continuous()), "continuous");
        assert_eq!(format!("{}", ScaleType::datetime()), "datetime");
    }

    #[test]
    fn test_scale_type_kind_display() {
        assert_eq!(format!("{}", ScaleTypeKind::Continuous), "continuous");
        assert_eq!(format!("{}", ScaleTypeKind::Identity), "identity");
    }

    #[test]
    fn test_scale_type_from_kind() {
        let scale_type = ScaleType::from_kind(ScaleTypeKind::Binned);
        assert_eq!(scale_type.scale_type_kind(), ScaleTypeKind::Binned);
    }

    #[test]
    fn test_scale_type_name() {
        assert_eq!(ScaleType::continuous().name(), "continuous");
        assert_eq!(ScaleType::date().name(), "date");
        assert_eq!(ScaleType::identity().name(), "identity");
    }

    #[test]
    fn test_scale_type_serialization() {
        let continuous = ScaleType::continuous();
        let json = serde_json::to_string(&continuous).unwrap();
        assert_eq!(json, "\"continuous\"");

        let deserialized: ScaleType = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.scale_type_kind(), ScaleTypeKind::Continuous);
    }

    #[test]
    fn test_scale_type_is_discrete() {
        // Default is true for most scale types
        assert!(ScaleType::continuous().is_discrete());
        assert!(ScaleType::binned().is_discrete());
        assert!(ScaleType::date().is_discrete());
        assert!(ScaleType::datetime().is_discrete());
        assert!(ScaleType::time().is_discrete());

        // Discrete and Identity return false
        assert!(!ScaleType::discrete().is_discrete());
        assert!(!ScaleType::identity().is_discrete());
    }

    #[test]
    fn test_allows_data_type() {
        // Continuous allows numeric types
        assert!(ScaleType::continuous().allows_data_type(&DataType::Float64));
        assert!(ScaleType::continuous().allows_data_type(&DataType::Int32));
        assert!(ScaleType::continuous().allows_data_type(&DataType::UInt64));
        assert!(!ScaleType::continuous().allows_data_type(&DataType::String));
        assert!(!ScaleType::continuous().allows_data_type(&DataType::Date));

        // Binned allows numeric types (same as continuous)
        assert!(ScaleType::binned().allows_data_type(&DataType::Float64));
        assert!(ScaleType::binned().allows_data_type(&DataType::Int32));
        assert!(!ScaleType::binned().allows_data_type(&DataType::String));

        // Discrete allows string/boolean
        assert!(ScaleType::discrete().allows_data_type(&DataType::String));
        assert!(ScaleType::discrete().allows_data_type(&DataType::Boolean));
        assert!(!ScaleType::discrete().allows_data_type(&DataType::Float64));
        assert!(!ScaleType::discrete().allows_data_type(&DataType::Int32));

        // Date allows only Date
        assert!(ScaleType::date().allows_data_type(&DataType::Date));
        assert!(!ScaleType::date().allows_data_type(&DataType::String));
        assert!(!ScaleType::date().allows_data_type(&DataType::Float64));

        // DateTime allows Datetime with any time unit
        use polars::prelude::TimeUnit;
        assert!(ScaleType::datetime()
            .allows_data_type(&DataType::Datetime(TimeUnit::Milliseconds, None)));
        assert!(ScaleType::datetime()
            .allows_data_type(&DataType::Datetime(TimeUnit::Microseconds, None)));
        assert!(!ScaleType::datetime().allows_data_type(&DataType::Date));
        assert!(!ScaleType::datetime().allows_data_type(&DataType::Time));

        // Time allows only Time
        assert!(ScaleType::time().allows_data_type(&DataType::Time));
        assert!(!ScaleType::time().allows_data_type(&DataType::Date));
        assert!(!ScaleType::time().allows_data_type(&DataType::String));

        // Identity allows everything
        assert!(ScaleType::identity().allows_data_type(&DataType::String));
        assert!(ScaleType::identity().allows_data_type(&DataType::Float64));
        assert!(ScaleType::identity().allows_data_type(&DataType::Date));
        assert!(ScaleType::identity().allows_data_type(&DataType::Time));
    }

    #[test]
    fn test_validate_columns() {
        use polars::prelude::*;

        let float_col: Column = Series::new("x".into(), &[1.0f64, 2.0, 3.0]).into();
        let string_col: Column = Series::new("y".into(), &["a", "b", "c"]).into();
        let int_col: Column = Series::new("z".into(), &[1i32, 2, 3]).into();

        // Continuous should accept numeric columns
        assert!(ScaleType::continuous()
            .validate_columns(&[&float_col])
            .is_ok());
        assert!(ScaleType::continuous()
            .validate_columns(&[&int_col])
            .is_ok());
        assert!(ScaleType::continuous()
            .validate_columns(&[&float_col, &int_col])
            .is_ok());

        // Continuous should reject string column
        let result = ScaleType::continuous().validate_columns(&[&string_col]);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Column 'y'"));

        // Discrete should accept string column
        assert!(ScaleType::discrete()
            .validate_columns(&[&string_col])
            .is_ok());

        // Discrete should reject numeric column
        let result = ScaleType::discrete().validate_columns(&[&float_col]);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Column 'x'"));

        // Identity should accept any column
        assert!(ScaleType::identity()
            .validate_columns(&[&float_col])
            .is_ok());
        assert!(ScaleType::identity()
            .validate_columns(&[&string_col])
            .is_ok());
        assert!(ScaleType::identity()
            .validate_columns(&[&float_col, &string_col, &int_col])
            .is_ok());
    }

    #[test]
    fn test_resolve_input_range_continuous_no_expand() {
        use polars::prelude::*;
        let col: Column = Series::new("x".into(), &[1.0f64, 5.0, 10.0]).into();

        // Disable expansion for predictable test values
        let mut props = HashMap::new();
        props.insert("expand".to_string(), ParameterValue::Number(0.0));

        // No user range -> compute from data
        let result = ScaleType::continuous()
            .resolve_input_range(None, &[&col], &props)
            .unwrap();
        assert_eq!(
            result,
            Some(vec![ArrayElement::Number(1.0), ArrayElement::Number(10.0)])
        );

        // User range with nulls -> merge
        let user = vec![ArrayElement::Null, ArrayElement::Number(100.0)];
        let result = ScaleType::continuous()
            .resolve_input_range(Some(&user), &[&col], &props)
            .unwrap();
        assert_eq!(
            result,
            Some(vec![ArrayElement::Number(1.0), ArrayElement::Number(100.0)])
        );

        // User range without nulls -> keep as-is
        let user = vec![ArrayElement::Number(0.0), ArrayElement::Number(50.0)];
        let result = ScaleType::continuous()
            .resolve_input_range(Some(&user), &[&col], &props)
            .unwrap();
        assert_eq!(
            result,
            Some(vec![ArrayElement::Number(0.0), ArrayElement::Number(50.0)])
        );
    }

    #[test]
    fn test_resolve_input_range_discrete() {
        use polars::prelude::*;
        let col: Column = Series::new("x".into(), &["b", "a", "c"]).into();
        let props = HashMap::new();

        // No user range -> compute unique sorted values
        let result = ScaleType::discrete()
            .resolve_input_range(None, &[&col], &props)
            .unwrap();
        assert_eq!(
            result,
            Some(vec![
                ArrayElement::String("a".into()),
                ArrayElement::String("b".into()),
                ArrayElement::String("c".into()),
            ])
        );

        // User range without nulls -> keep as-is
        let user = vec![
            ArrayElement::String("x".into()),
            ArrayElement::String("y".into()),
        ];
        let result = ScaleType::discrete()
            .resolve_input_range(Some(&user), &[&col], &props)
            .unwrap();
        assert_eq!(
            result,
            Some(vec![
                ArrayElement::String("x".into()),
                ArrayElement::String("y".into()),
            ])
        );
    }

    #[test]
    fn test_resolve_input_range_discrete_rejects_nulls() {
        use polars::prelude::*;
        let col: Column = Series::new("x".into(), &["a", "b"]).into();
        let user = vec![ArrayElement::Null, ArrayElement::String("c".into())];
        let props = HashMap::new();

        let result = ScaleType::discrete().resolve_input_range(Some(&user), &[&col], &props);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("null placeholder"));
    }

    #[test]
    fn test_resolve_input_range_identity_rejects_range() {
        use polars::prelude::*;
        let col: Column = Series::new("x".into(), &[1.0f64]).into();
        let user = vec![ArrayElement::Number(0.0), ArrayElement::Number(10.0)];
        let props = HashMap::new();

        let result = ScaleType::identity().resolve_input_range(Some(&user), &[&col], &props);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("does not support input range"));
    }

    #[test]
    fn test_resolve_input_range_identity_no_range() {
        use polars::prelude::*;
        let col: Column = Series::new("x".into(), &[1.0f64]).into();
        let props = HashMap::new();

        let result = ScaleType::identity()
            .resolve_input_range(None, &[&col], &props)
            .unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_resolve_input_range_binned_no_expand() {
        use polars::prelude::*;
        let col: Column = Series::new("x".into(), &[1i32, 5, 10]).into();

        // Disable expansion for predictable test values
        let mut props = HashMap::new();
        props.insert("expand".to_string(), ParameterValue::Number(0.0));

        // No user range -> compute from data
        let result = ScaleType::binned()
            .resolve_input_range(None, &[&col], &props)
            .unwrap();
        assert_eq!(
            result,
            Some(vec![ArrayElement::Number(1.0), ArrayElement::Number(10.0)])
        );
    }

    #[test]
    fn test_scale_type_infer() {
        use polars::prelude::TimeUnit;

        // Numeric → Continuous
        assert_eq!(ScaleType::infer(&DataType::Int32), ScaleType::continuous());
        assert_eq!(ScaleType::infer(&DataType::Int64), ScaleType::continuous());
        assert_eq!(
            ScaleType::infer(&DataType::Float64),
            ScaleType::continuous()
        );
        assert_eq!(ScaleType::infer(&DataType::UInt16), ScaleType::continuous());

        // Temporal
        assert_eq!(ScaleType::infer(&DataType::Date), ScaleType::date());
        assert_eq!(
            ScaleType::infer(&DataType::Datetime(TimeUnit::Microseconds, None)),
            ScaleType::datetime()
        );
        assert_eq!(ScaleType::infer(&DataType::Time), ScaleType::time());

        // Discrete
        assert_eq!(ScaleType::infer(&DataType::String), ScaleType::discrete());
        assert_eq!(ScaleType::infer(&DataType::Boolean), ScaleType::discrete());
    }

    // =========================================================================
    // Expansion Tests
    // =========================================================================

    #[test]
    fn test_parse_expand_value_number() {
        let val = ParameterValue::Number(0.1);
        let (mult, add) = parse_expand_value(&val).unwrap();
        assert!((mult - 0.1).abs() < 1e-10);
        assert!((add - 0.0).abs() < 1e-10);
    }

    #[test]
    fn test_parse_expand_value_array() {
        let val = ParameterValue::Array(vec![
            ArrayElement::Number(0.05),
            ArrayElement::Number(10.0),
        ]);
        let (mult, add) = parse_expand_value(&val).unwrap();
        assert!((mult - 0.05).abs() < 1e-10);
        assert!((add - 10.0).abs() < 1e-10);
    }

    #[test]
    fn test_parse_expand_value_invalid() {
        let val = ParameterValue::String("invalid".to_string());
        assert!(parse_expand_value(&val).is_none());

        let val = ParameterValue::Array(vec![ArrayElement::Number(0.05)]);
        assert!(parse_expand_value(&val).is_none()); // Too few elements
    }

    #[test]
    fn test_expand_numeric_range_mult_only() {
        let range = vec![ArrayElement::Number(0.0), ArrayElement::Number(100.0)];
        let expanded = expand_numeric_range(&range, 0.05, 0.0);
        // span = 100, expanded = [-5, 105]
        assert_eq!(expanded[0], ArrayElement::Number(-5.0));
        assert_eq!(expanded[1], ArrayElement::Number(105.0));
    }

    #[test]
    fn test_expand_numeric_range_with_add() {
        let range = vec![ArrayElement::Number(0.0), ArrayElement::Number(100.0)];
        let expanded = expand_numeric_range(&range, 0.05, 10.0);
        // span = 100, mult_expansion = 5, add_expansion = 10
        // expanded = [0 - 5 - 10, 100 + 5 + 10] = [-15, 115]
        assert_eq!(expanded[0], ArrayElement::Number(-15.0));
        assert_eq!(expanded[1], ArrayElement::Number(115.0));
    }

    #[test]
    fn test_expand_numeric_range_zero_disables() {
        let range = vec![ArrayElement::Number(0.0), ArrayElement::Number(100.0)];
        let expanded = expand_numeric_range(&range, 0.0, 0.0);
        // No expansion
        assert_eq!(expanded[0], ArrayElement::Number(0.0));
        assert_eq!(expanded[1], ArrayElement::Number(100.0));
    }

    #[test]
    fn test_expand_default_applied() {
        use polars::prelude::*;
        let col: Column = Series::new("x".into(), &[0.0f64, 100.0]).into();

        // Default properties (no expand key) should use DEFAULT_EXPAND_MULT = 0.05
        let props = HashMap::new();

        let result = ScaleType::continuous()
            .resolve_input_range(None, &[&col], &props)
            .unwrap()
            .unwrap();

        // span = 100, 5% expansion = 5 on each side
        // expected: [-5, 105]
        assert_eq!(result[0], ArrayElement::Number(-5.0));
        assert_eq!(result[1], ArrayElement::Number(105.0));
    }

    #[test]
    fn test_expand_custom_value() {
        use polars::prelude::*;
        let col: Column = Series::new("x".into(), &[0.0f64, 100.0]).into();

        let mut props = HashMap::new();
        props.insert("expand".to_string(), ParameterValue::Number(0.1));

        let result = ScaleType::continuous()
            .resolve_input_range(None, &[&col], &props)
            .unwrap()
            .unwrap();

        // span = 100, 10% expansion = 10 on each side
        // expected: [-10, 110]
        assert_eq!(result[0], ArrayElement::Number(-10.0));
        assert_eq!(result[1], ArrayElement::Number(110.0));
    }

    #[test]
    fn test_expand_with_additive() {
        use polars::prelude::*;
        let col: Column = Series::new("x".into(), &[0.0f64, 100.0]).into();

        let mut props = HashMap::new();
        props.insert(
            "expand".to_string(),
            ParameterValue::Array(vec![
                ArrayElement::Number(0.05),
                ArrayElement::Number(10.0),
            ]),
        );

        let result = ScaleType::continuous()
            .resolve_input_range(None, &[&col], &props)
            .unwrap()
            .unwrap();

        // span = 100, 5% expansion = 5, additive = 10
        // expected: [-15, 115]
        assert_eq!(result[0], ArrayElement::Number(-15.0));
        assert_eq!(result[1], ArrayElement::Number(115.0));
    }

    #[test]
    fn test_expand_applied_to_user_range() {
        use polars::prelude::*;
        let col: Column = Series::new("x".into(), &[50.0f64]).into();

        let mut props = HashMap::new();
        props.insert("expand".to_string(), ParameterValue::Number(0.05));

        // User provides explicit range
        let user_range = vec![ArrayElement::Number(0.0), ArrayElement::Number(100.0)];

        let result = ScaleType::continuous()
            .resolve_input_range(Some(&user_range), &[&col], &props)
            .unwrap()
            .unwrap();

        // span = 100, 5% expansion = 5 on each side
        // expected: [-5, 105]
        assert_eq!(result[0], ArrayElement::Number(-5.0));
        assert_eq!(result[1], ArrayElement::Number(105.0));
    }

    #[test]
    fn test_expand_zero_disables() {
        use polars::prelude::*;
        let col: Column = Series::new("x".into(), &[0.0f64, 100.0]).into();

        let mut props = HashMap::new();
        props.insert("expand".to_string(), ParameterValue::Number(0.0));

        let result = ScaleType::continuous()
            .resolve_input_range(None, &[&col], &props)
            .unwrap()
            .unwrap();

        // No expansion
        assert_eq!(result[0], ArrayElement::Number(0.0));
        assert_eq!(result[1], ArrayElement::Number(100.0));
    }

    // =========================================================================
    // resolve_properties Tests
    // =========================================================================

    #[test]
    fn test_resolve_properties_unknown_rejected() {
        let mut props = HashMap::new();
        props.insert("unknown".to_string(), ParameterValue::Number(1.0));

        let result = ScaleType::continuous().resolve_properties("x", &props);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("unknown"));
        assert!(err.contains("expand")); // Should suggest allowed properties
    }

    #[test]
    fn test_resolve_properties_default_expand() {
        let props = HashMap::new();
        let resolved = ScaleType::continuous().resolve_properties("x", &props).unwrap();

        assert!(resolved.contains_key("expand"));
        match resolved.get("expand") {
            Some(ParameterValue::Number(n)) => {
                assert!((n - DEFAULT_EXPAND_MULT).abs() < 1e-10);
            }
            _ => panic!("Expected Number"),
        }
    }

    #[test]
    fn test_resolve_properties_preserves_user_value() {
        let mut props = HashMap::new();
        props.insert("expand".to_string(), ParameterValue::Number(0.1));

        let resolved = ScaleType::continuous().resolve_properties("x", &props).unwrap();

        match resolved.get("expand") {
            Some(ParameterValue::Number(n)) => assert!((n - 0.1).abs() < 1e-10),
            _ => panic!("Expected Number"),
        }
    }

    #[test]
    fn test_discrete_rejects_properties() {
        let mut props = HashMap::new();
        props.insert("expand".to_string(), ParameterValue::Number(0.1));

        let result = ScaleType::discrete().resolve_properties("color", &props);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("discrete"));
        assert!(err.contains("does not support any SETTING"));
    }

    #[test]
    fn test_identity_rejects_properties() {
        let mut props = HashMap::new();
        props.insert("expand".to_string(), ParameterValue::Number(0.1));

        let result = ScaleType::identity().resolve_properties("x", &props);
        assert!(result.is_err());
    }

    #[test]
    fn test_resolve_properties_binned_supports_expand() {
        let mut props = HashMap::new();
        props.insert("expand".to_string(), ParameterValue::Number(0.2));

        let resolved = ScaleType::binned().resolve_properties("x", &props).unwrap();
        match resolved.get("expand") {
            Some(ParameterValue::Number(n)) => assert!((n - 0.2).abs() < 1e-10),
            _ => panic!("Expected Number"),
        }
    }

    #[test]
    fn test_resolve_properties_date_supports_expand() {
        let props = HashMap::new();
        let resolved = ScaleType::date().resolve_properties("x", &props).unwrap();

        // Should have default expand
        assert!(resolved.contains_key("expand"));
    }

    #[test]
    fn test_resolve_properties_empty_allowed_for_discrete() {
        // Empty properties should be allowed for discrete
        let props = HashMap::new();
        let result = ScaleType::discrete().resolve_properties("color", &props);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_expand_only_for_positional_aesthetics() {
        let mut props = HashMap::new();
        props.insert("expand".to_string(), ParameterValue::Number(0.1));

        // Positional aesthetics should allow expand
        assert!(ScaleType::continuous().resolve_properties("x", &props).is_ok());
        assert!(ScaleType::continuous().resolve_properties("y", &props).is_ok());
        assert!(ScaleType::continuous().resolve_properties("xmin", &props).is_ok());
        assert!(ScaleType::continuous().resolve_properties("ymax", &props).is_ok());

        // Non-positional aesthetics should reject expand
        let result = ScaleType::continuous().resolve_properties("color", &props);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("does not support any SETTING"));

        let result = ScaleType::continuous().resolve_properties("size", &props);
        assert!(result.is_err());

        let result = ScaleType::continuous().resolve_properties("opacity", &props);
        assert!(result.is_err());
    }

    #[test]
    fn test_no_default_expand_for_non_positional() {
        // Non-positional aesthetics should not get default expand
        let props = HashMap::new();
        let resolved = ScaleType::continuous().resolve_properties("color", &props).unwrap();
        assert!(!resolved.contains_key("expand"));
        assert!(resolved.is_empty());

        // Positional aesthetics should get default expand
        let resolved = ScaleType::continuous().resolve_properties("x", &props).unwrap();
        assert!(resolved.contains_key("expand"));
    }
}
