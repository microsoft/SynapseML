const assert = require('node:assert/strict');
const childProcess = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const repoRoot = path.resolve(__dirname, '..', '..');
const installArtifacts = require('../src/installArtifacts');
const publishedPorts = JSON.parse(
  fs.readFileSync(
    path.join(__dirname, 'published-spark-ports.lock'),
    'utf8',
  ),
);
const publishedVersions = JSON.parse(
  fs.readFileSync(path.join(repoRoot, 'website', 'versions.json'), 'utf8'),
);
const currentVersion = publishedVersions[0];

assert.match(
  currentVersion,
  /^\d+\.\d+\.\d+$/,
  'expected the first published documentation version to be current',
);

const portCases = [
  {
    key: 'spark4.0',
    websiteKey: 'spark40',
    sparkVersionPattern: /val sparkVersion = "4\.0\.[0-9]+"/,
  },
  {
    key: 'spark4.1',
    websiteKey: 'spark41',
    sparkVersionPattern: /val sparkVersion = "4\.1\.[0-9]+"/,
  },
];

function git(...args) {
  return childProcess.execFileSync('git', args, {
    cwd: repoRoot,
    encoding: 'utf8',
  }).trim();
}

function expandDocumentedVersion(markdown) {
  return markdown.replaceAll('${SYNAPSEML_VERSION}', currentVersion);
}

test('published Spark port metadata matches tagged source', () => {
  for (const portCase of portCases) {
    const locked = publishedPorts[portCase.key];
    const websiteArtifact = installArtifacts[portCase.websiteKey];

    assert.ok(locked, `missing locked metadata for ${portCase.key}`);
    assert.equal(git('tag', '--list', locked.releaseTag), locked.releaseTag);
    assert.equal(
      locked.artifactVersion,
      locked.releaseTag.replace(/^v/, ''),
    );
    assert.ok(locked.coordinate.endsWith(`:${locked.artifactVersion}`));
    assert.equal(websiteArtifact.coordinate, locked.coordinate);
    assert.equal(websiteArtifact.pythonBaseline, locked.pythonBaseline);
    assert.equal(websiteArtifact.pythonPackage, locked.pythonPackage);
    assert.equal(websiteArtifact.pysparkSpec, locked.pysparkSpec);
    assert.equal(
      websiteArtifact.scalaBinaryVersion,
      locked.scalaBinaryVersion,
    );

    const build = git('show', `${locked.releaseTag}:build.sbt`);
    const environment = git('show', `${locked.releaseTag}:environment.yml`);
    assert.match(build, portCase.sparkVersionPattern);
    assert.match(
      build,
      new RegExp(
        `ThisBuild / scalaVersion := "${locked.scalaBinaryVersion.replace(
          '.',
          '\\.',
        )}\\.[0-9]+"`,
      ),
    );
    assert.match(
      environment,
      new RegExp(`^  - python=${locked.pythonBaseline}(?:\\.[0-9]+)?$`, 'm'),
    );
  }
});

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
    const expanded = expandDocumentedVersion(markdown);

    assert.ok(
      markdown.includes(`SYNAPSEML_VERSION="${currentVersion}"`),
      'the release bump mechanism must own the documented base version',
    );
    assert.ok(expanded.includes(installArtifacts.spark35.coordinate));
    for (const portCase of portCases) {
      const locked = publishedPorts[portCase.key];
      assert.ok(expanded.includes(locked.coordinate));
      assert.ok(expanded.includes(locked.releaseTag));
    }
    assert.match(markdown, /does \*\*not\*\* add the\s+JVM artifacts/);
    assert.match(markdown, /LightGBMClassifier does not exist in the JVM/);
    assert.match(markdown, /Spark 4\.1 \/ Python 3\.13/);
    assert.match(markdown, /pyspark>=4\.1,<4\.2/);
    assert.match(markdown, /Spark 4\.0 \/ Python 3\.12/);
    assert.match(markdown, /pyspark>=4\.0,<4\.1/);
    assert.match(markdown, /Spark 3\.5 \/ Python 3\.11/);
    assert.match(markdown, /pyspark>=3\.5,<3\.6/);
    assert.match(markdown, /Choose exactly one complete runtime variant/);
    for (const [runtime, pysparkSpec] of [
      ['Spark 4.1 / Python 3.13', 'pyspark>=4.1,<4.2'],
      ['Spark 4.0 / Python 3.12', 'pyspark>=4.0,<4.1'],
      ['Spark 3.5 / Python 3.11', 'pyspark>=3.5,<3.6'],
    ]) {
      const escapedRuntime = runtime.replaceAll('.', '\\.');
      const escapedSpec = pysparkSpec.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      assert.match(
        markdown,
        new RegExp(
          `\\*\\*${escapedRuntime}\\*\\*\\s+\\x60\\x60\\x60bash\\s+` +
            `python -m pip install [^\\n]+${escapedSpec}[^\\n]+\\s+\\x60\\x60\\x60`,
        ),
      );
    }
    assert.match(
      markdown,
      /synapseml_2\.12:\$\{SYNAPSEML_VERSION\}/,
    );
    assert.match(markdown, /--repositories "\$SYNAPSEML_REPOSITORY"/);
    assert.ok(markdown.includes(installArtifacts.repository));
    assert.doesNotMatch(markdown, /mmlspark\.azureedge\.net/);
  });
}

test('website landing page uses every supported runtime variant', () => {
  const index = fs.readFileSync(
    path.join(repoRoot, 'website', 'src', 'pages', 'index.js'),
    'utf8',
  );

  assert.match(index, /import installArtifacts from "@site\/src\/installArtifacts"/);
  for (const key of ['spark35', 'spark40', 'spark41']) {
    assert.match(index, new RegExp(`${key}\\.coordinate`));
    assert.match(index, new RegExp(`${key}\\.pythonBaseline`));
    assert.match(index, new RegExp(`${key}\\.pysparkSpec`));
  }
  assert.doesNotMatch(index, /THE_SYNAPSEML_VERSION_YOU_WANT/);
  assert.doesNotMatch(index, /only Spark 3|any Spark 3 infrastructure/);
  assert.match(index, /Choose exactly one Python\/PySpark runtime variant/);
  assert.match(index, /lang="scala"/);
  assert.doesNotMatch(index, /lang="jsx"/);
  assert.doesNotMatch(
    index,
    /com\.microsoft\.azure:synapseml_2\.12:1\.1\.3/,
  );
  assert.doesNotMatch(index, /<p>\s*<p>/);
  assert.doesNotMatch(index, /<p>\{description\}<\/p>/);
});

test('specialized install guides state their runtime scope', () => {
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
  assert.match(deepLearning, /targets Spark 3\.5 \/ Scala 2\.12/);
  assert.doesNotMatch(deepLearning, /sample uses a Spark 3 \/ Scala 2\.12/);
  const expandedOnnx = expandDocumentedVersion(onnx);
  assert.ok(
    onnx.includes(`SYNAPSEML_VERSION="${currentVersion}"`),
    'the release bump mechanism must own the ONNX module version',
  );
  assert.match(onnx, /<synapseml-deep-learning-coordinate>/);
  assert.match(onnx, /<synapseml-deep-learning-version>/);
  assert.doesNotMatch(onnx, /<synapseml-coordinate>/);
  assert.doesNotMatch(onnx, /<synapseml-artifact-version>/);
  assert.doesNotMatch(onnx, /--packages\s+<synapseml/);
  assert.match(
    onnx,
    /--packages "\$\{SYNAPSEML_DEEP_LEARNING_COORDINATE\},/,
  );
  for (const key of ['spark35', 'spark40', 'spark41']) {
    assert.ok(
      expandedOnnx.includes(
        installArtifacts[key].coordinate.replace(
          ':synapseml_',
          ':synapseml-deep-learning_',
        ),
      ),
    );
  }
  assert.ok(onnx.includes(installArtifacts.repository));
  assert.match(rSetup, /examples below use Spark 3\.5 \/ Scala 2\.12/);
  assert.match(rSetup, /installation matrix/);
  assert.match(isolationForest, /scoped to Spark 3\.5 \/ Scala 2\.12/);
  assert.match(isolationForest, /not a Python 3\.12\/3\.13 setup/);
  assert.match(
    isolationForest,
    /com\.microsoft\.azure:synapseml_2\.12:1\.1\.3/,
  );
  assert.doesNotMatch(isolationForest, /synapseml_2\.13/);
  assert.doesNotMatch(isolationForest, /COORDINATE_FROM_THE_INSTALL_MATRIX/);
});
