//! High-level ggsql API.
//!
//! Validation and query inspection without SQL execution.

use crate::parser;
use crate::Result;

// ============================================================================
// Core Types
// ============================================================================

/// Result of `validate()` - query inspection and validation without SQL execution.
pub struct Validated {
    sql: String,
    visual: String,
    has_visual: bool,
    tree: Option<tree_sitter::Tree>,
    valid: bool,
    errors: Vec<ValidationError>,
    warnings: Vec<ValidationWarning>,
}

impl Validated {
    /// Whether the query contains a VISUALISE clause.
    pub fn has_visual(&self) -> bool {
        self.has_visual
    }

    /// The SQL portion (before VISUALISE).
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// The VISUALISE portion (raw text).
    pub fn visual(&self) -> &str {
        &self.visual
    }

    /// CST for advanced inspection.
    pub fn tree(&self) -> Option<&tree_sitter::Tree> {
        self.tree.as_ref()
    }

    /// Whether the query is valid (no errors).
    pub fn valid(&self) -> bool {
        self.valid
    }

    /// Validation errors.
    pub fn errors(&self) -> &[ValidationError] {
        &self.errors
    }

    /// Validation warnings.
    pub fn warnings(&self) -> &[ValidationWarning] {
        &self.warnings
    }
}

/// A validation error (fatal).
#[derive(Debug, Clone)]
pub struct ValidationError {
    pub message: String,
    pub location: Option<Location>,
}

/// A validation warning (non-fatal).
#[derive(Debug, Clone)]
pub struct ValidationWarning {
    pub message: String,
    pub location: Option<Location>,
}

/// Location within a query string (0-based).
#[derive(Debug, Clone)]
pub struct Location {
    pub line: usize,
    pub column: usize,
}

// ============================================================================
// High-Level API Functions
// ============================================================================

/// Validate query syntax and semantics without executing SQL.
pub fn validate(query: &str) -> Result<Validated> {
    let mut errors = Vec::new();
    let warnings = Vec::new();

    // Split to determine if there's a viz portion
    let (sql_part, viz_part) = match parser::split_query(query) {
        Ok((sql, viz)) => (sql, viz),
        Err(e) => {
            // Split error - return as validation error
            errors.push(ValidationError {
                message: e.to_string(),
                location: None,
            });
            return Ok(Validated {
                sql: String::new(),
                visual: String::new(),
                has_visual: false,
                tree: None,
                valid: false,
                errors,
                warnings,
            });
        }
    };

    let has_visual = !viz_part.trim().is_empty();

    // Parse the full query to get the CST
    let tree = if has_visual {
        let mut ts_parser = tree_sitter::Parser::new();
        ts_parser
            .set_language(&tree_sitter_ggsql::language())
            .map_err(|e| {
                crate::GgsqlError::InternalError(format!("Failed to set language: {}", e))
            })?;
        ts_parser.parse(query, None)
    } else {
        None
    };

    // If no visualization, just syntax check passed
    if !has_visual {
        return Ok(Validated {
            sql: sql_part,
            visual: viz_part,
            has_visual,
            tree,
            valid: true,
            errors,
            warnings,
        });
    }

    // Parse to get plot specifications for validation
    let plots = match parser::parse_query(query) {
        Ok(p) => p,
        Err(e) => {
            errors.push(ValidationError {
                message: e.to_string(),
                location: None,
            });
            return Ok(Validated {
                sql: sql_part,
                visual: viz_part,
                has_visual,
                tree,
                valid: false,
                errors,
                warnings,
            });
        }
    };

    // Validate the single plot (we only support one VISUALISE statement)
    if let Some(plot) = plots.first() {
        // Validate each layer
        for (layer_idx, layer) in plot.layers.iter().enumerate() {
            let context = format!("Layer {}", layer_idx + 1);

            // Check required aesthetics
            // Note: Without schema data, we can only check if mappings exist,
            // not if the columns are valid. We skip this check for wildcards.
            if !layer.mappings.wildcard {
                if let Err(e) = layer.validate_required_aesthetics() {
                    errors.push(ValidationError {
                        message: format!("{}: {}", context, e),
                        location: None,
                    });
                }
            }

            // Validate SETTING parameters
            if let Err(e) = layer.validate_settings() {
                errors.push(ValidationError {
                    message: format!("{}: {}", context, e),
                    location: None,
                });
            }
        }
    }

    Ok(Validated {
        sql: sql_part,
        visual: viz_part,
        has_visual,
        tree,
        valid: errors.is_empty(),
        errors,
        warnings,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_with_visual() {
        let validated =
            validate("SELECT 1 as x, 2 as y VISUALISE DRAW point MAPPING x AS x, y AS y").unwrap();
        assert!(validated.has_visual());
        assert_eq!(validated.sql(), "SELECT 1 as x, 2 as y");
        assert!(validated.visual().starts_with("VISUALISE"));
        assert!(validated.tree().is_some());
        assert!(validated.valid());
    }

    #[test]
    fn test_validate_without_visual() {
        let validated = validate("SELECT 1 as x, 2 as y").unwrap();
        assert!(!validated.has_visual());
        assert_eq!(validated.sql(), "SELECT 1 as x, 2 as y");
        assert!(validated.visual().is_empty());
        assert!(validated.tree().is_none());
        assert!(validated.valid());
    }

    #[test]
    fn test_validate_valid_query() {
        let validated =
            validate("SELECT 1 as x, 2 as y VISUALISE DRAW point MAPPING x AS x, y AS y").unwrap();
        assert!(
            validated.valid(),
            "Expected valid query: {:?}",
            validated.errors()
        );
        assert!(validated.errors().is_empty());
    }

    #[test]
    fn test_validate_missing_required_aesthetic() {
        // Point requires x and y, but we only provide x
        let validated =
            validate("SELECT 1 as x, 2 as y VISUALISE DRAW point MAPPING x AS x").unwrap();
        assert!(!validated.valid());
        assert!(!validated.errors().is_empty());
        assert!(validated.errors()[0].message.contains("y"));
    }

    #[test]
    fn test_validate_syntax_error() {
        let validated = validate("SELECT 1 VISUALISE DRAW invalidgeom").unwrap();
        assert!(!validated.valid());
        assert!(!validated.errors().is_empty());
    }

    #[cfg(all(feature = "duckdb", feature = "vegalite"))]
    #[test]
    fn test_execute_and_render() {
        use crate::reader::{DuckDBReader, Reader};
        use crate::writer::VegaLiteWriter;

        let reader = DuckDBReader::from_connection_string("duckdb://memory").unwrap();
        let spec = reader.execute("SELECT 1 as x, 2 as y VISUALISE x, y DRAW point").unwrap();

        assert_eq!(spec.plot().layers.len(), 1);
        assert_eq!(spec.metadata().layer_count, 1);
        assert!(spec.data().is_some());

        let writer = VegaLiteWriter::new();
        let result = spec.render(&writer).unwrap();
        assert!(result.contains("point"));
    }

    #[cfg(all(feature = "duckdb", feature = "vegalite"))]
    #[test]
    fn test_execute_metadata() {
        use crate::reader::{DuckDBReader, Reader};

        let reader = DuckDBReader::from_connection_string("duckdb://memory").unwrap();
        let spec = reader.execute(
            "SELECT * FROM (VALUES (1, 10), (2, 20), (3, 30)) AS t(x, y) VISUALISE x, y DRAW point",
        )
        .unwrap();

        let metadata = spec.metadata();
        assert_eq!(metadata.rows, 3);
        assert_eq!(metadata.columns.len(), 2);
        assert!(metadata.columns.contains(&"x".to_string()));
        assert!(metadata.columns.contains(&"y".to_string()));
        assert_eq!(metadata.layer_count, 1);
    }

    #[cfg(all(feature = "duckdb", feature = "vegalite"))]
    #[test]
    fn test_execute_with_cte() {
        use crate::reader::{DuckDBReader, Reader};

        let reader = DuckDBReader::from_connection_string("duckdb://memory").unwrap();
        let query = r#"
            WITH data AS (
                SELECT * FROM (VALUES (1, 10), (2, 20)) AS t(x, y)
            )
            SELECT * FROM data
            VISUALISE x, y DRAW point
        "#;

        let spec = reader.execute(query).unwrap();

        assert_eq!(spec.plot().layers.len(), 1);
        assert!(spec.data().is_some());
        let df = spec.data().unwrap();
        assert_eq!(df.height(), 2);
    }

    #[cfg(all(feature = "duckdb", feature = "vegalite"))]
    #[test]
    fn test_render_multi_layer() {
        use crate::reader::{DuckDBReader, Reader};
        use crate::writer::VegaLiteWriter;

        let reader = DuckDBReader::from_connection_string("duckdb://memory").unwrap();
        let query = r#"
            SELECT * FROM (VALUES (1, 10), (2, 20), (3, 30)) AS t(x, y)
            VISUALISE
            DRAW point MAPPING x AS x, y AS y
            DRAW line MAPPING x AS x, y AS y
        "#;

        let spec = reader.execute(query).unwrap();
        let writer = VegaLiteWriter::new();
        let result = spec.render(&writer).unwrap();

        assert!(result.contains("layer"));
    }

    #[cfg(all(feature = "duckdb", feature = "vegalite"))]
    #[test]
    fn test_register_and_query() {
        use crate::reader::{DuckDBReader, Reader};
        use crate::writer::VegaLiteWriter;
        use polars::prelude::*;

        let mut reader = DuckDBReader::from_connection_string("duckdb://memory").unwrap();

        let df = df! {
            "x" => [1i32, 2, 3],
            "y" => [10i32, 20, 30],
        }
        .unwrap();

        reader.register("my_data", df).unwrap();

        let query = "SELECT * FROM my_data VISUALISE x, y DRAW point";
        let spec = reader.execute(query).unwrap();

        assert_eq!(spec.metadata().rows, 3);
        assert!(spec.metadata().columns.contains(&"x".to_string()));

        let writer = VegaLiteWriter::new();
        let result = spec.render(&writer).unwrap();
        assert!(result.contains("point"));
    }

    #[cfg(all(feature = "duckdb", feature = "vegalite"))]
    #[test]
    fn test_register_and_join() {
        use crate::reader::{DuckDBReader, Reader};
        use polars::prelude::*;

        let mut reader = DuckDBReader::from_connection_string("duckdb://memory").unwrap();

        let sales = df! {
            "id" => [1i32, 2, 3],
            "amount" => [100i32, 200, 300],
            "product_id" => [1i32, 1, 2],
        }
        .unwrap();

        let products = df! {
            "id" => [1i32, 2],
            "name" => ["Widget", "Gadget"],
        }
        .unwrap();

        reader.register("sales", sales).unwrap();
        reader.register("products", products).unwrap();

        let query = r#"
            SELECT s.id, s.amount, p.name
            FROM sales s
            JOIN products p ON s.product_id = p.id
            VISUALISE id AS x, amount AS y
            DRAW bar
        "#;

        let spec = reader.execute(query).unwrap();
        assert_eq!(spec.metadata().rows, 3);
    }

    #[cfg(feature = "duckdb")]
    #[test]
    fn test_execute_no_viz_fails() {
        use crate::reader::{DuckDBReader, Reader};

        let reader = DuckDBReader::from_connection_string("duckdb://memory").unwrap();
        let query = "SELECT 1 as x, 2 as y";

        let result = reader.execute(query);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_sql_and_visual_content() {
        let query = "SELECT 1 as x, 2 as y VISUALISE DRAW point MAPPING x AS x, y AS y DRAW line MAPPING x AS x, y AS y";
        let validated = validate(query).unwrap();

        assert!(validated.has_visual());
        assert_eq!(validated.sql(), "SELECT 1 as x, 2 as y");
        assert!(validated.visual().contains("DRAW point"));
        assert!(validated.visual().contains("DRAW line"));
        assert!(validated.valid());
    }

    #[test]
    fn test_validate_sql_only() {
        let query = "SELECT 1 as x, 2 as y";
        let validated = validate(query).unwrap();

        // SQL-only queries should be valid (just syntax check)
        assert!(validated.valid());
        assert!(validated.errors().is_empty());
    }
}
