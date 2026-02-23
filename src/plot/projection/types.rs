//! Projection types for ggsql visualization specifications
//!
//! This module defines projection configuration and types.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::coord::Coord;
use crate::plot::ParameterValue;

/// Projection (from PROJECT clause)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Projection {
    /// Coordinate system type
    pub coord: Coord,
    /// Projection-specific options
    pub properties: HashMap<String, ParameterValue>,
}
