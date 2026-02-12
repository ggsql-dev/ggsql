//! Tree-sitter source tree wrapper with declarative query support.
//!
//! The `SourceTree` struct wraps a tree-sitter parse tree along with the source text
//! and language, providing high-level query operations for tree traversal and text extraction.

use crate::{GgsqlError, Result};
use tree_sitter::{Language, Node, Parser, Query, QueryCursor, StreamingIterator, Tree};

/// The source tree - holds a parsed syntax tree, source text, and language together.
/// Like Yggdrasil, it connects all parsing operations with a single root.
#[derive(Debug)]
pub struct SourceTree<'a> {
    pub tree: Tree,
    pub source: &'a str,
    pub language: Language,
}

impl<'a> SourceTree<'a> {
    /// Parse source and create a new SourceTree
    pub fn new(source: &'a str) -> Result<Self> {
        let language = tree_sitter_ggsql::language();

        let mut parser = Parser::new();
        parser
            .set_language(&language)
            .map_err(|e| GgsqlError::InternalError(format!("Failed to set language: {}", e)))?;

        let tree = parser
            .parse(source, None)
            .ok_or_else(|| GgsqlError::ParseError("Failed to parse query".to_string()))?;

        Ok(Self {
            tree,
            source,
            language,
        })
    }

    /// Validate that the parse tree has no errors
    pub fn validate(&self) -> Result<()> {
        if self.tree.root_node().has_error() {
            return Err(GgsqlError::ParseError(
                "Parse tree contains errors".to_string(),
            ));
        }
        Ok(())
    }

    /// Get the root node
    pub fn root(&self) -> Node<'_> {
        self.tree.root_node()
    }

    /// Extract text from a node
    pub fn get_text(&self, node: &Node) -> String {
        self.source[node.start_byte()..node.end_byte()].to_string()
    }

    /// Find all nodes matching a tree-sitter query
    pub fn find_nodes<'b>(&self, node: &Node<'b>, query_source: &str) -> Vec<Node<'b>> {
        let query = match Query::new(&self.language, query_source) {
            Ok(q) => q,
            Err(_) => return Vec::new(),
        };

        let mut cursor = QueryCursor::new();
        let mut matches = cursor.matches(&query, *node, self.source.as_bytes());

        let mut results = Vec::new();
        while let Some(match_result) = matches.next() {
            for capture in match_result.captures {
                results.push(capture.node);
            }
        }
        results
    }

    /// Find first node matching query
    pub fn find_node<'b>(&self, node: &Node<'b>, query_source: &str) -> Option<Node<'b>> {
        let query = match Query::new(&self.language, query_source) {
            Ok(q) => q,
            Err(_) => return None,
        };

        let mut cursor = QueryCursor::new();
        let mut matches = cursor.matches(&query, *node, self.source.as_bytes());

        // Return the first capture immediately without collecting all results
        if let Some(match_result) = matches.next() {
            if let Some(capture) = match_result.captures.first() {
                return Some(capture.node);
            }
        }
        None
    }

    /// Find first node text matching query
    pub fn find_text(&self, node: &Node, query: &str) -> Option<String> {
        self.find_node(node, query).map(|n| self.get_text(&n))
    }

    /// Find all node texts matching query
    pub fn find_texts(&self, node: &Node, query: &str) -> Vec<String> {
        self.find_nodes(node, query)
            .iter()
            .map(|n| self.get_text(n))
            .collect()
    }

    /// Extract the SQL portion of the query (before VISUALISE)
    ///
    /// If VISUALISE FROM is used, this injects "SELECT * FROM <source>"
    /// Returns None if there's no SQL portion and no VISUALISE FROM injection needed
    pub fn extract_sql(&self) -> Option<String> {
        let root = self.root();

        // Check if there's any VISUALISE statement
        if self.find_node(&root, "(visualise_statement) @viz").is_none() {
            // No VISUALISE at all - return entire source as SQL
            return Some(self.source.to_string());
        }

        // Find sql_portion node and extract its text
        let sql_text = self
            .find_node(&root, "(sql_portion) @sql")
            .map(|node| self.get_text(&node))
            .unwrap_or_default();

        // Check if any VISUALISE statement has FROM clause
        let from_query = r#"
            (visualise_statement
              (from_clause
                (table_ref) @table))
        "#;

        if let Some(from_identifier) = self.find_text(&root, from_query) {
            // Inject SELECT * FROM <source>
            let result = if sql_text.trim().is_empty() {
                format!("SELECT * FROM {}", from_identifier)
            } else {
                format!("{} SELECT * FROM {}", sql_text.trim(), from_identifier)
            };
            Some(result)
        } else {
            // No injection needed - return SQL if not empty
            let trimmed = sql_text.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        }
    }

    /// Extract the VISUALISE portion of the query (from first VISUALISE onwards)
    ///
    /// Returns the raw text of all VISUALISE statements
    pub fn extract_visualise(&self) -> Option<String> {
        let root = self.root();

        // Find byte offset of first VISUALISE
        let viz_start = self
            .find_node(&root, "(visualise_statement) @viz")
            .map(|node| node.start_byte())?;

        // Extract viz text from first VISUALISE onwards
        let viz_text = &self.source[viz_start..];
        Some(viz_text.trim().to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_sql_simple() {
        let query = "SELECT * FROM data VISUALISE  DRAW point MAPPING x AS x, y AS y";
        let tree = SourceTree::new(query).unwrap();

        let sql = tree.extract_sql().unwrap();
        assert_eq!(sql, "SELECT * FROM data");

        let viz = tree.extract_visualise().unwrap();
        assert!(viz.starts_with("VISUALISE"));
        assert!(viz.contains("DRAW point"));
    }

    #[test]
    fn test_extract_sql_case_insensitive() {
        let query = "SELECT * FROM data visualise x, y DRAW point";
        let tree = SourceTree::new(query).unwrap();

        let sql = tree.extract_sql().unwrap();
        assert_eq!(sql, "SELECT * FROM data");

        let viz = tree.extract_visualise().unwrap();
        assert!(viz.starts_with("visualise"));
    }

    #[test]
    fn test_extract_sql_no_visualise() {
        let query = "SELECT * FROM data WHERE x > 5";
        let tree = SourceTree::new(query).unwrap();

        let sql = tree.extract_sql().unwrap();
        assert_eq!(sql, query);

        let viz = tree.extract_visualise();
        assert!(viz.is_none());
    }

    #[test]
    fn test_extract_sql_visualise_from_no_sql() {
        let query = "VISUALISE FROM mtcars  DRAW point MAPPING mpg AS x, hp AS y";
        let tree = SourceTree::new(query).unwrap();

        let sql = tree.extract_sql().unwrap();
        // Should inject SELECT * FROM mtcars
        assert_eq!(sql, "SELECT * FROM mtcars");

        let viz = tree.extract_visualise().unwrap();
        assert!(viz.starts_with("VISUALISE FROM mtcars"));
    }

    #[test]
    fn test_extract_sql_visualise_from_with_cte() {
        let query = "WITH cte AS (SELECT * FROM x) VISUALISE FROM cte DRAW point MAPPING a AS x, b AS y";
        let tree = SourceTree::new(query).unwrap();

        let sql = tree.extract_sql().unwrap();
        // Should inject SELECT * FROM cte after the WITH
        assert!(sql.contains("WITH cte AS (SELECT * FROM x)"));
        assert!(sql.contains("SELECT * FROM cte"));

        let viz = tree.extract_visualise().unwrap();
        assert!(viz.starts_with("VISUALISE FROM cte"));
    }

    #[test]
    fn test_extract_sql_visualise_from_after_create() {
        let query = "CREATE TABLE x AS SELECT 1; VISUALISE FROM x";
        let tree = SourceTree::new(query).unwrap();

        let sql = tree.extract_sql().unwrap();
        assert!(sql.contains("CREATE TABLE x AS SELECT 1;"));
        assert!(sql.contains("SELECT * FROM x"));

        let viz = tree.extract_visualise().unwrap();
        assert!(viz.starts_with("VISUALISE FROM x"));

        // Without semicolon, the visualise statement should also be recognised
        let query2 = "CREATE TABLE x AS SELECT 1 VISUALISE FROM x";
        let tree2 = SourceTree::new(query2).unwrap();

        let sql2 = tree2.extract_sql().unwrap();
        assert!(sql2.contains("CREATE TABLE x AS SELECT 1"));
        assert!(sql2.contains("SELECT * FROM x"));

        let viz2 = tree2.extract_visualise().unwrap();
        assert!(viz2.starts_with("VISUALISE FROM x"));
    }

    #[test]
    fn test_extract_sql_visualise_from_after_insert() {
        let query = "INSERT INTO x VALUES (1) VISUALISE FROM x DRAW";
        let tree = SourceTree::new(query).unwrap();

        let sql = tree.extract_sql().unwrap();
        assert!(sql.contains("INSERT"));

        let viz = tree.extract_visualise().unwrap();
        assert!(viz.contains("DRAW"));
    }

    #[test]
    fn test_extract_sql_no_injection_with_select() {
        let query = "SELECT * FROM x VISUALISE DRAW point MAPPING a AS x, b AS y";
        let tree = SourceTree::new(query).unwrap();

        let sql = tree.extract_sql().unwrap();
        // Should NOT inject anything - just extract SQL normally
        assert_eq!(sql, "SELECT * FROM x");
        assert!(!sql.contains("SELECT * FROM SELECT")); // Make sure we didn't double-inject
    }

    #[test]
    fn test_extract_sql_visualise_from_file_path_single_quotes() {
        let query = "VISUALISE FROM 'mtcars.csv'  DRAW point MAPPING mpg AS x, hp AS y";
        let tree = SourceTree::new(query).unwrap();

        let sql = tree.extract_sql().unwrap();
        // Should inject SELECT * FROM 'mtcars.csv' with quotes preserved
        assert_eq!(sql, "SELECT * FROM 'mtcars.csv'");

        let viz = tree.extract_visualise().unwrap();
        assert!(viz.starts_with("VISUALISE FROM 'mtcars.csv'"));
    }

    #[test]
    fn test_extract_sql_visualise_from_file_path_double_quotes() {
        let query = r#"VISUALISE FROM "data/sales.parquet"  DRAW bar MAPPING region AS x, total AS y"#;
        let tree = SourceTree::new(query).unwrap();

        let sql = tree.extract_sql().unwrap();
        // Should inject SELECT * FROM "data/sales.parquet" with quotes preserved
        assert_eq!(sql, r#"SELECT * FROM "data/sales.parquet""#);

        let viz = tree.extract_visualise().unwrap();
        assert!(viz.starts_with(r#"VISUALISE FROM "data/sales.parquet""#));
    }

    #[test]
    fn test_extract_sql_visualise_from_file_path_with_cte() {
        let query = "WITH prep AS (SELECT * FROM 'raw.csv' WHERE year = 2024) VISUALISE FROM prep  DRAW line MAPPING date AS x, value AS y";
        let tree = SourceTree::new(query).unwrap();

        let sql = tree.extract_sql().unwrap();
        // Should inject SELECT * FROM prep after WITH
        assert!(sql.contains("WITH prep AS"));
        assert!(sql.contains("SELECT * FROM prep"));
        // The file path inside the CTE should remain as-is (part of the WITH clause)
        assert!(sql.contains("'raw.csv'"));
    }
}
