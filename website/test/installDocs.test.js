const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");

const repoRoot = path.resolve(__dirname, "..", "..");
const installArtifacts = require("../src/installArtifacts");
const publishedPorts = JSON.parse(
  fs.readFileSync(
    path.join(__dirname, "published-spark-ports.lock"),
    "utf8",
  ),
);
const publishedVersions = JSON.parse(
  fs.readFileSync(path.join(repoRoot, "website", "versions.json"), "utf8"),
);
const currentVersion = installArtifacts.version;
const artifacts = [
  installArtifacts.spark35,
  installArtifacts.spark40,
  installArtifacts.spark41,
];

function read(...segments) {
  return fs.readFileSync(path.join(repoRoot, ...segments), "utf8");
}

assert.match(
  currentVersion,
  /^\d+\.\d+\.\d+$/,
  "expected an explicit current SynapseML version",
);
assert.equal(
  publishedVersions[0],
  currentVersion,
  "website versions.json must start with the current SynapseML version",
);

test("published Spark port versions are explicitly locked", () => {
  for (const [port, artifact] of [
    ["spark4.0", installArtifacts.spark40],
    ["spark4.1", installArtifacts.spark41],
  ]) {
    const expectedVersion = `${currentVersion}-${port}`;
    assert.equal(
      publishedPorts[port],
      expectedVersion,
      `update published-spark-ports.lock only after ${expectedVersion} is published`,
    );
    assert.equal(
      artifact.coordinate,
      `com.microsoft.azure:synapseml_2.13:${expectedVersion}`,
    );
    assert.equal(artifact.releaseTag, `v${expectedVersion}`);
  }
});

test("runtime metadata identifies the maintained code lines", () => {
  assert.deepEqual(
    artifacts.map((artifact) => artifact.branch),
    ["master", "spark4.0", "spark4.1"],
  );
  assert.equal(installArtifacts.spark35.sparkRuntime, "3.5.x");
  assert.equal(installArtifacts.spark40.sparkRuntime, "4.0.1+ (<4.1)");
  assert.equal(installArtifacts.spark41.sparkRuntime, "4.1.x");
  assert.equal(installArtifacts.spark40.pysparkSpec, ">=4.0.1,<4.1");
  for (const artifact of artifacts) {
    assert.equal(artifact.pythonPackage, `synapseml==${currentVersion}`);
  }
});

const installGuides = [
  {
    path: ["README.md"],
    hasMasterSnapshot: true,
  },
  {
    path: ["docs", "Get Started", "Install SynapseML.md"],
    hasMasterSnapshot: true,
  },
  {
    path: [
      "website",
      "versioned_docs",
      `version-${currentVersion}`,
      "Get Started",
      "Install SynapseML.md",
    ],
    hasMasterSnapshot: false,
  },
];

for (const guide of installGuides) {
  const relativePath = guide.path.join("/");
  test(`installation examples are concrete in ${relativePath}`, () => {
    const markdown = read(...guide.path);

    assert.match(markdown, /does \*\*not\*\* add the\s+JVM artifacts/);
    assert.match(markdown, /LightGBMClassifier does not exist in the JVM/);
    assert.match(markdown, /choose exactly one complete runtime variant/i);
    assert.ok(markdown.includes(installArtifacts.repository));

    for (const artifact of artifacts) {
      assert.ok(markdown.includes(artifact.coordinate));
      assert.ok(markdown.includes(artifact.releaseTag));
      assert.ok(markdown.includes(artifact.pythonPackage));
      assert.ok(markdown.includes(`pyspark${artifact.pysparkSpec}`));
    }

    assert.doesNotMatch(markdown, /\$\{SYNAPSEML_VERSION\}/);
    assert.doesNotMatch(markdown, /COORDINATE_FROM_THE_MATRIX_ABOVE/);
    assert.doesNotMatch(markdown, /SCALA_BINARY_VERSION/);
    assert.doesNotMatch(markdown, /THE_SYNAPSEML_VERSION_YOU_WANT/);
    assert.doesNotMatch(markdown, /mmlspark\.azureedge\.net/);
    assert.doesNotMatch(markdown, /For Spark ?3\.[34] [Pp]ools/);
    assert.doesNotMatch(markdown, /synapseml_2\.12:0\.11\.4-spark3\.3/);
    assert.doesNotMatch(markdown, /synapseml_2\.12:1\.0\.15/);

    if (guide.hasMasterSnapshot) {
      assert.match(markdown, /^#{2,3} Latest master snapshot/m);
      assert.ok(markdown.includes("master_version3.svg"));
      assert.match(markdown, /MASTER_VERSION=/);
      assert.match(markdown, /spark-shell/);
    } else {
      assert.doesNotMatch(markdown, /master_version3\.svg/);
    }
  });
}

test("website landing page exposes only maintained runtime installs", () => {
  const index = read("website", "src", "pages", "index.js");

  assert.match(
    index,
    /import installArtifacts from "@site\/src\/installArtifacts"/,
  );
  for (const key of ["spark35", "spark40", "spark41"]) {
    for (const field of [
      "branch",
      "coordinate",
      "pythonBaseline",
      "pysparkSpec",
      "releaseTag",
      "sparkRuntime",
    ]) {
      assert.match(index, new RegExp(`${key}\\.${field}`));
    }
  }
  assert.match(index, /latest successful/);
  assert.match(
    index,
    /docs\/next\/Get%20Started\/Install%20SynapseML#latest-master-snapshot/,
  );
  assert.match(index, /Choose exactly one Python\/PySpark runtime variant/);
  assert.match(index, /lang="scala"/);
  assert.doesNotMatch(index, /lang="jsx"/);
  assert.doesNotMatch(index, /Spark3\.4|Spark 3\.4|Spark3\.3|Spark 3\.3/);
  assert.doesNotMatch(index, /synapseml_2\.12:1\.0\.15/);
  assert.doesNotMatch(index, /THE_SYNAPSEML_VERSION_YOU_WANT/);
  assert.doesNotMatch(index, /<p>\s*<p>/);
  assert.doesNotMatch(index, /<p>\{description\}<\/p>/);
});

test("specialized install guides use concrete maintained coordinates", () => {
  const overview = read("docs", "Overview.md");
  const deepLearning = read(
    "docs",
    "Explore Algorithms",
    "Deep Learning",
    "Getting Started.md",
  );
  const versionedDeepLearning = read(
    "website",
    "versioned_docs",
    `version-${currentVersion}`,
    "Explore Algorithms",
    "Deep Learning",
    "Getting Started.md",
  );
  const onnx = read(
    "docs",
    "Explore Algorithms",
    "Deep Learning",
    "ONNX.md",
  );
  const rSetup = read("docs", "Reference", "R Setup.md");
  const versionedRSetup = read(
    "website",
    "versioned_docs",
    `version-${currentVersion}`,
    "Reference",
    "R Setup.md",
  );
  const isolationForest = read(
    "docs",
    "Explore Algorithms",
    "Anomaly Detection",
    "Quickstart - Isolation Forests.ipynb",
  );

  assert.doesNotMatch(overview, /requires Scala 2\.12/);
  assert.match(overview, /Spark 4\.0 and 4\.1 use Scala 2\.13/);

  for (const guide of [deepLearning, versionedDeepLearning]) {
    assert.match(guide, /Python wheel supplies wrappers/);
    assert.ok(guide.includes(installArtifacts.spark40.coordinate));
    assert.ok(guide.includes(installArtifacts.spark41.coordinate));
  }

  for (const artifact of artifacts) {
    assert.ok(
      onnx.includes(
        artifact.coordinate.replace(
          ":synapseml_",
          ":synapseml-deep-learning_",
        ),
      ),
    );
  }
  assert.ok(onnx.includes(installArtifacts.repository));
  assert.doesNotMatch(onnx, /SYNAPSEML_VERSION/);
  assert.doesNotMatch(onnx, /SCALA_BINARY_VERSION/);
  assert.doesNotMatch(onnx, /SYNAPSEML_DEEP_LEARNING_VERSION/);
  assert.doesNotMatch(onnx, /<synapseml-deep-learning/);
  assert.match(onnx, /resolvers \+= "SynapseML"/);
  assert.match(onnx, /<id>SynapseML<\/id>/);
  assert.match(onnx, /"repo": "https:\/\/mmlspark\.blob\.core\.windows\.net\/maven"/);

  for (const guide of [rSetup, versionedRSetup]) {
    assert.ok(guide.includes(installArtifacts.spark35.coordinate));
    assert.ok(guide.includes(installArtifacts.spark40.coordinate));
    assert.ok(guide.includes(installArtifacts.spark41.coordinate));
    assert.doesNotMatch(guide, /spark-3\.3\./);
  }

  assert.match(isolationForest, /scoped to Spark 3\.5 \/ Scala 2\.12/);
  assert.match(isolationForest, /not a Python 3\.12\/3\.13 setup/);
  assert.ok(isolationForest.includes(installArtifacts.spark35.coordinate));
  assert.doesNotMatch(isolationForest, /synapseml_2\.13/);
});
