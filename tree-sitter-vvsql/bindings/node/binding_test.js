/**
 * Test file for tree-sitter-vvsql Node.js bindings
 */

const Parser = require('tree-sitter');

try {
  const vvSQL = require('./index.js');
  console.log('✅ Successfully loaded tree-sitter-vvsql bindings');
  console.log('Language name:', vvSQL.name);

  // Create a parser
  const parser = new Parser();
  parser.setLanguage(vvSQL.language);

  // Test parsing a simple vvSQL query
  const sourceCode = `
  VISUALISE AS PLOT
  WITH point USING
      x = date,
      y = revenue
  `;

  const tree = parser.parse(sourceCode);

  if (tree.rootNode.hasError()) {
    console.log('❌ Parse error in test query');
    console.log(tree.rootNode.toString());
  } else {
    console.log('✅ Successfully parsed test vvSQL query');
    console.log('Root node type:', tree.rootNode.type);
    console.log('Child count:', tree.rootNode.childCount);
  }

  // Test a more complex query
  const complexQuery = `
  VISUALISE AS PLOT
  WITH line USING
      x = date,
      y = revenue,
      color = region
  WITH point USING
      x = date,
      y = revenue,
      color = region,
      size = 3
  SCALE x USING
      type = 'date'
  LABEL title = 'Revenue Analysis'
  THEME minimal
  `;

  const complexTree = parser.parse(complexQuery);

  if (complexTree.rootNode.hasError()) {
    console.log('❌ Parse error in complex query');
  } else {
    console.log('✅ Successfully parsed complex vvSQL query');
    console.log('Complex query child count:', complexTree.rootNode.childCount);
  }

} catch (error) {
  console.error('❌ Failed to load tree-sitter-vvsql bindings:', error.message);
  process.exit(1);
}