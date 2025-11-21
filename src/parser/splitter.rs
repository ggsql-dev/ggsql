//! Query splitter using tree-sitter
//!
//! Splits vvSQL queries into SQL and visualization portions, and injects
//! SELECT * FROM <source> when VISUALISE FROM is used.

use crate::{VvsqlError, Result};
use tree_sitter::{Parser, Node};

/// Split a vvSQL query into SQL and visualization portions
///
/// Returns (sql_part, viz_part) where:
/// - sql_part: SQL to execute (may be injected with SELECT * FROM if VISUALISE FROM is present)
/// - viz_part: Everything from first "VISUALISE/VISUALIZE AS" onwards (may contain multiple VISUALISE statements)
///
/// If VISUALISE FROM <source> is used, this function will inject "SELECT * FROM <source>"
/// into the SQL portion, handling semicolons correctly.
pub fn split_query(query: &str) -> Result<(String, String)> {
    let query = query.trim();

    // Parse the full query with tree-sitter to understand its structure
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_vvsql::language())
        .map_err(|e| VvsqlError::InternalError(format!("Failed to set language: {}", e)))?;

    let tree = parser
        .parse(query, None)
        .ok_or_else(|| VvsqlError::ParseError("Failed to parse query".to_string()))?;

    let root = tree.root_node();

    // If there's no VISUALISE statement, treat entire query as SQL
    // Note: We don't check for parse errors here because the SQL portion might have errors
    // (complex SQL we don't fully parse), but we still want to extract VISUALISE statements
    if root.children(&mut root.walk()).all(|n| n.kind() != "visualise_statement") {
        return Ok((query.to_string(), String::new()));
    }

    // Find the first VISUALISE statement to determine split point
    // Use byte offset instead of node boundaries to handle parse errors in SQL portion
    let mut first_viz_start: Option<usize> = None;
    let mut cursor = root.walk();
    for child in root.children(&mut cursor) {
        if child.kind() == "visualise_statement" {
            first_viz_start = Some(child.start_byte());
            break;
        }
    }

    let (sql_text, viz_text) = if let Some(viz_start) = first_viz_start {
        // Split at the first VISUALISE keyword
        let sql_part = &query[..viz_start];
        let viz_part = &query[viz_start..];
        (sql_part.trim().to_string(), viz_part.trim().to_string())
    } else {
        // No VISUALISE statement found (shouldn't happen due to earlier check)
        (query.to_string(), String::new())
    };

    // Check if any VISUALISE statement has FROM clause and inject SELECT if needed
    let mut modified_sql = sql_text.clone();

    for child in root.children(&mut root.walk()) {
        if child.kind() == "visualise_statement" {
            // Look for FROM identifier in this visualise_statement
            if let Some(from_identifier) = extract_from_identifier(&child, query) {
                // Inject SELECT * FROM <source>
                if modified_sql.trim().is_empty() {
                    // No SQL yet - just add SELECT
                    modified_sql = format!("SELECT * FROM {}", from_identifier);
                } else {
                    // Check if last statement is WITH - if so, no semicolon needed
                    let trimmed = modified_sql.trim();
                    let last_is_with = trimmed.to_uppercase().starts_with("WITH");

                    if last_is_with {
                        // WITH followed by SELECT - no semicolon
                        modified_sql = format!("{} SELECT * FROM {}", trimmed, from_identifier);
                    } else if trimmed.ends_with(';') {
                        // Already has semicolon - add SELECT after it
                        modified_sql = format!("{} SELECT * FROM {}", trimmed, from_identifier);
                    } else {
                        // No semicolon - add one before SELECT
                        modified_sql = format!("{}; SELECT * FROM {}", trimmed, from_identifier);
                    }
                }
                // Only inject once (first VISUALISE FROM found)
                break;
            }
        }
    }

    Ok((modified_sql, viz_text))
}

/// Extract FROM identifier or string from a visualise_statement node
fn extract_from_identifier(node: &Node, source: &str) -> Option<String> {
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.kind() == "identifier" {
            // Identifier: table name or CTE name
            return Some(get_node_text(&child, source).to_string());
        }
        if child.kind() == "string" {
            // String literal: file path (e.g., 'mtcars.csv')
            // Return as-is with quotes - DuckDB handles it
            return Some(get_node_text(&child, source).to_string());
        }
        if child.kind() == "viz_type" {
            // If we hit viz_type without finding identifier/string, there's no FROM
            return None;
        }
    }
    None
}

/// Get text content of a node
fn get_node_text<'a>(node: &Node, source: &'a str) -> &'a str {
    &source[node.start_byte()..node.end_byte()]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_split() {
        let query = "SELECT * FROM data VISUALISE AS PLOT WITH point USING x = x, y = y";
        let (sql, viz) = split_query(query).unwrap();

        assert_eq!(sql, "SELECT * FROM data");
        assert!(viz.starts_with("VISUALISE AS PLOT"));
        assert!(viz.contains("WITH point"));
    }

    #[test]
    fn test_case_insensitive() {
        let query = "SELECT * FROM data visualise as plot WITH point USING x = x, y = y";
        let (sql, viz) = split_query(query).unwrap();

        assert_eq!(sql, "SELECT * FROM data");
        assert!(viz.starts_with("visualise as plot"));
    }

    #[test]
    fn test_no_visualise() {
        let query = "SELECT * FROM data WHERE x > 5";
        let (sql, viz) = split_query(query).unwrap();

        assert_eq!(sql, query);
        assert!(viz.is_empty());
    }

    #[test]
    fn test_visualise_from_no_sql() {
        let query = "VISUALISE FROM mtcars AS PLOT WITH point USING x = mpg, y = hp";
        let (sql, viz) = split_query(query).unwrap();

        // Should inject SELECT * FROM mtcars
        assert_eq!(sql, "SELECT * FROM mtcars");
        assert!(viz.starts_with("VISUALISE FROM mtcars"));
    }

    #[test]
    fn test_visualise_from_with_cte() {
        let query = "WITH cte AS (SELECT * FROM x) VISUALISE FROM cte AS PLOT WITH point USING x = a, y = b";
        let (sql, viz) = split_query(query).unwrap();

        // Should inject SELECT * FROM cte after the WITH
        assert!(sql.contains("WITH cte AS (SELECT * FROM x)"));
        assert!(sql.contains("SELECT * FROM cte"));
        assert!(viz.starts_with("VISUALISE FROM cte"));
    }

    #[test]
    fn test_visualise_from_with_semicolon() {
        let query = "CREATE TABLE x AS SELECT 1; WITH cte AS (SELECT * FROM x) VISUALISE FROM cte AS PLOT";
        let (sql, viz) = split_query(query).unwrap();

        // Should have CREATE; WITH...; SELECT * FROM cte
        assert!(sql.contains("CREATE TABLE x AS SELECT 1"));
        assert!(sql.contains("WITH cte"));
        assert!(sql.contains("SELECT * FROM cte"));
    }

    #[test]
    fn test_visualise_as_no_injection() {
        let query = "SELECT * FROM x VISUALISE AS PLOT WITH point USING x = a, y = b";
        let (sql, viz) = split_query(query).unwrap();

        // Should NOT inject anything - just split normally
        assert_eq!(sql, "SELECT * FROM x");
        assert!(!sql.contains("SELECT * FROM SELECT")); // Make sure we didn't double-inject
    }

    #[test]
    fn test_visualise_from_file_path() {
        let query = "VISUALISE FROM 'mtcars.csv' AS PLOT WITH point USING x = mpg, y = hp";
        let (sql, viz) = split_query(query).unwrap();

        // Should inject SELECT * FROM 'mtcars.csv' with quotes preserved
        assert_eq!(sql, "SELECT * FROM 'mtcars.csv'");
        assert!(viz.starts_with("VISUALISE FROM 'mtcars.csv'"));
    }

    #[test]
    fn test_visualise_from_file_path_double_quotes() {
        let query = r#"VISUALISE FROM "data/sales.parquet" AS PLOT WITH bar USING x = region, y = total"#;
        let (sql, viz) = split_query(query).unwrap();

        // Should inject SELECT * FROM "data/sales.parquet" with quotes preserved
        assert_eq!(sql, r#"SELECT * FROM "data/sales.parquet""#);
        assert!(viz.starts_with(r#"VISUALISE FROM "data/sales.parquet""#));
    }

    #[test]
    fn test_visualise_from_file_path_with_cte() {
        let query = "WITH prep AS (SELECT * FROM 'raw.csv' WHERE year = 2024) VISUALISE FROM prep AS PLOT WITH line USING x = date, y = value";
        let (sql, _viz) = split_query(query).unwrap();

        // Should inject SELECT * FROM prep after WITH
        assert!(sql.contains("WITH prep AS"));
        assert!(sql.contains("SELECT * FROM prep"));
        // The file path inside the CTE should remain as-is (part of the WITH clause)
        assert!(sql.contains("'raw.csv'"));
    }
}