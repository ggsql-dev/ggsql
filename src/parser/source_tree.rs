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
    pub fn find_node<'b>(&self, node: &Node<'b>, query: &str) -> Option<Node<'b>> {
        self.find_nodes(node, query).into_iter().next()
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
}
