/*!
Rust bindings for tree-sitter-vvsql grammar

This crate provides the tree-sitter language definition for vvSQL,
a SQL extension for declarative data visualization.
*/

use tree_sitter::Language;

extern "C" {
    fn tree_sitter_vvsql() -> Language;
}

/// Returns the tree-sitter language for vvSQL
pub fn language() -> Language {
    unsafe { tree_sitter_vvsql() }
}

/// The node types and field names used by the vvSQL grammar
pub const NODE_TYPES: &str = include_str!("../../src/node-types.json");

/// The highlighting queries for vvSQL syntax
pub const HIGHLIGHTS_QUERY: &str = include_str!("../../queries/highlights.scm");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_language() {
        let language = language();
        assert!(language.abi_version() <= tree_sitter::LANGUAGE_VERSION);
        assert!(language.abi_version() >= tree_sitter::MIN_COMPATIBLE_LANGUAGE_VERSION);
    }
}
