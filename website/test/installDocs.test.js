const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const repoRoot = path.resolve(__dirname, '..', '..');
const config = fs.readFileSync(
  path.join(repoRoot, 'website', 'docusaurus.config.js'),
  'utf8',
);
const currentVersion = config.match(/let version = "([0-9.]+)"/)?.[1];
const publishedVersions = JSON.parse(
  fs.readFileSync(path.join(repoRoot, 'website', 'versions.json'), 'utf8'),
);

assert.ok(currentVersion, 'expected a current SynapseML documentation version');
assert.equal(publishedVersions[0], currentVersion);

const installGuides = [
  path.join(repoRoot, 'README.md'),
  path.join(repoRoot, 'docs', 'Get Started', 'Install SynapseML.md'),
  path.join(
    repoRoot,
    'website',
    'versioned_docs',
    `version-${currentVersion}`,
    'Get Started',
    'Install SynapseML.md',
  ),
];

for (const guide of installGuides) {
  test(`Spark runtime matrix is complete in ${path.relative(repoRoot, guide)}`, () => {
    const markdown = fs.readFileSync(guide, 'utf8');

    assert.ok(
      markdown.includes(`SYNAPSEML_VERSION="${currentVersion}"`),
      'the release bump mechanism must own the documented base version',
    );
    assert.ok(
      markdown.includes(
        'com.microsoft.azure:synapseml_2.12:${SYNAPSEML_VERSION}',
      ),
    );
    assert.ok(
      markdown.includes(
        'com.microsoft.azure:synapseml_2.13:${SYNAPSEML_VERSION}-spark4.0',
      ),
    );
    assert.ok(
      markdown.includes(
        'com.microsoft.azure:synapseml_2.13:${SYNAPSEML_VERSION}-spark4.1',
      ),
    );
    assert.ok(
      markdown.includes(
        '| Spark 4.0.x | 2.13 | Python 3.12 | ' +
          '`v${SYNAPSEML_VERSION}-spark4.0` |',
      ),
    );
    assert.ok(
      markdown.includes(
        '| Spark 4.1.x | 2.13 | Python 3.13 | ' +
          '`v${SYNAPSEML_VERSION}-spark4.1` |',
      ),
    );
    assert.match(markdown, /does \*\*not\*\* add the\s+JVM artifacts/);
    assert.match(markdown, /LightGBMClassifier does not exist in the JVM/);
    assert.match(markdown, /pyspark>=4\.1,<4\.2/);
    assert.match(markdown, /--repositories "\$SYNAPSEML_REPOSITORY"/);
    assert.match(
      markdown,
      /https:\/\/mmlspark\.blob\.core\.windows\.net\/maven/,
    );
    assert.doesNotMatch(markdown, /mmlspark\.azureedge\.net/);
  });
}

test('specialized install guides defer to the runtime matrix', () => {
  const overview = fs.readFileSync(
    path.join(repoRoot, 'docs', 'Overview.md'),
    'utf8',
  );
  const deepLearning = fs.readFileSync(
    path.join(
      repoRoot,
      'docs',
      'Explore Algorithms',
      'Deep Learning',
      'Getting Started.md',
    ),
    'utf8',
  );
  const onnx = fs.readFileSync(
    path.join(
      repoRoot,
      'docs',
      'Explore Algorithms',
      'Deep Learning',
      'ONNX.md',
    ),
    'utf8',
  );
  const rSetup = fs.readFileSync(
    path.join(repoRoot, 'docs', 'Reference', 'R Setup.md'),
    'utf8',
  );
  const isolationForest = fs.readFileSync(
    path.join(
      repoRoot,
      'docs',
      'Explore Algorithms',
      'Anomaly Detection',
      'Quickstart - Isolation Forests.ipynb',
    ),
    'utf8',
  );

  assert.doesNotMatch(overview, /requires Scala 2\.12/);
  assert.match(overview, /Spark 4\.0 and 4\.1 use Scala 2\.13/);
  assert.match(deepLearning, /Python wheel supplies wrappers/);
  assert.match(deepLearning, /installation matrix/);
  assert.match(onnx, /<synapseml-coordinate>/);
  assert.doesNotMatch(onnx, /synapseml_2\.12:<version>/);
  assert.match(rSetup, /examples below use Spark 3\.5 \/ Scala 2\.12/);
  assert.match(rSetup, /installation matrix/);
  assert.match(isolationForest, /COORDINATE_FROM_THE_INSTALL_MATRIX/);
  assert.doesNotMatch(isolationForest, /THE_SYNAPSEML_VERSION_YOU_WANT/);
});
