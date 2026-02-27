//! Projection types for ggsql visualization specifications
//!
//! This module defines projection configuration and types.

pub mod coord;
mod types;

pub use coord::{Coord, CoordKind, CoordTrait};
pub use types::Projection;
