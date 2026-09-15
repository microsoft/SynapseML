const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const {matchPath} = require('react-router');
const sidebars = require('../sidebars');

const documentId =
  'Explore Algorithms/LightGBM/LightGBM - Quantile Regression for Drug Discovery (Scala)';
const markdown = fs.readFileSync(
  path.join(__dirname, '..', '..', 'docs', `${documentId}.md`),
  'utf8',
);

test('the Scala quantile tutorial route matches its published URL', () => {
  const slug =
    markdown.match(/^slug:\s*(.+)$/m)?.[1] ?? path.posix.basename(documentId);
  const route = `/SynapseML/docs/${path.posix.dirname(documentId)}/${slug}/`;

  assert.ok(matchPath(route, {path: route, exact: true}));
});

test('the LightGBM sidebar includes the Scala quantile tutorial', () => {
  const algorithms = sidebars.docs.find(
    (item) => item.label === 'Explore Algorithms',
  );
  const lightgbm = algorithms.items.find((item) => item.label === 'LightGBM');

  assert.ok(lightgbm.items.includes(documentId));
});

test('coverage excludes every row flagged by the quantile-crossing diagnostic', () => {
  assert.match(
    markdown,
    /val validRows\s*=\s*predictionsWithInterval\.filter\(!\$"is_crossed"\)/,
  );
  assert.doesNotMatch(
    markdown,
    /val validRows\s*=\s*predictionsWithInterval\.filter\(\$"uncertainty_width" >= 0\)/,
  );
});
