//! Projection types for ggsql visualization specifications
//!
//! This module defines projection configuration and types.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::super::types::ParameterValue;

/// Projection (from PROJECT clause)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Project {
    /// Projection type
    pub project_type: ProjectType,
    /// Projection-specific options
    pub properties: HashMap<String, ParameterValue>,
}

/// Projection types
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ProjectType {
    Cartesian,
    Polar,
    Flip,
    Fixed,
    Trans,
    Map,
    QuickMap,
}
