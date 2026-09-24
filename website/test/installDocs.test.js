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
const spark40Version = installArtifacts.spark40.releaseTag
  .replace(/^v/, "")
  .replace(/-spark4\.0$/, "");
const previewSetting = process.env.SYNAPSEML_DOCS_PREVIEW;
assert.ok(
  [undefined, "true", "false"].includes(previewSetting),
  "SYNAPSEML_DOCS_PREVIEW must be true or false",
);
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

function validatePublicationLock(
  version, lock, versions, preview, optionalVersion = version,
) {
  assert.equal(typeof preview, "boolean");
  assert.equal(versions[0], version);
  assert.ok(
    versions.includes(optionalVersion),
    "unknown Spark 4.0 documentation version",
  );
  for (const port of ["spark4.0", "spark4.1"]) {
    const selectedVersion = port === "spark4.0" ? optionalVersion : version;
    const allowed = (preview ? versions : [selectedVersion]).map(
      (item) => `${item}-${port}`,
    );
    assert.ok(
      allowed.includes(lock[port]),
      `update published-spark-ports.lock only after ${selectedVersion}-${port} is published`,
    );
  }
}

function validateSpark40References(markdown, version) {
  const references = [...markdown.matchAll(
    /(?<![\d.])(\d+\.\d+\.\d+)-spark4\.0(?!\w|\.\d)/g,
  )];
  assert.ok(references.length, "missing Spark 4.0 artifact references");
  for (const reference of references) {
    assert.equal(reference[1], version, "mixed Spark 4.0 artifact versions");
  }
  for (const line of markdown.split(/\r?\n/)) {
    if (!line.includes("| [`spark4.0`]") && !line.includes("pyspark>=4.0")) {
      continue;
    }
    const pins = [...line.matchAll(/synapseml==([^\s"'`|]+)/g)];
    assert.ok(pins.length, "missing Spark 4.0 Python package pin");
    for (const pin of pins) {
      assert.equal(pin[1], version, "mixed Spark 4.0 Python package versions");
    }
  }
}

test("partial optional-runtime edits cannot pass beside matching references", () => {
  const readme = read("README.md");
  const next = "999.8.7";
  const partial = readme.split("\n").map((line) =>
    line.includes("| [`spark4.0`]") || line.includes("Spark 4.0 notebooks")
      ? line.replaceAll(spark40Version, next) : line,
  ).join("\n");
  assert.throws(() => validateSpark40References(partial, next), /mixed Spark 4.0/);
  const artifactOnly = readme.replaceAll(
    `${spark40Version}-spark4.0`, `${next}-spark4.0`,
  );
  assert.throws(
    () => validateSpark40References(artifactOnly, next),
    /mixed Spark 4.0 Python/,
  );
});

test("a complete optional-runtime update or restoration uses one version", () => {
  const readme = read("README.md");
  const next = "999.8.7";
  const updated = readme.split("\n").map((line) => {
    if (line.includes("| [`spark4.0`]") || line.includes("pyspark>=4.0")) {
      return line.replaceAll(spark40Version, next);
    }
    return line.replaceAll(`${spark40Version}-spark4.0`, `${next}-spark4.0`);
  }).join("\n");
  validateSpark40References(updated, next);
  validateSpark40References(updated.replaceAll(next, spark40Version), spark40Version);
});

test("optional-runtime references include sentence endings and jar names", () => {
  for (const suffix of [".", ".jar"]) {
    validateSpark40References(`${spark40Version}-spark4.0${suffix}`, spark40Version);
    assert.throws(
      () => validateSpark40References(
        `${spark40Version}-spark4.0 and 999.8.7-spark4.0${suffix}`, spark40Version,
      ),
      /mixed Spark 4.0 artifact versions/,
    );
  }
});

test("published Spark port versions are explicitly locked", () => {
  validatePublicationLock(
    currentVersion,
    publishedPorts,
    publishedVersions,
    previewSetting === "true",
    spark40Version,
  );
  for (const [port, artifact] of [
    ["spark4.0", installArtifacts.spark40],
    ["spark4.1", installArtifacts.spark41],
  ]) {
    const selectedVersion = port === "spark4.0" ? spark40Version : currentVersion;
    const expectedVersion = `${selectedVersion}-${port}`;
    assert.equal(
      artifact.coordinate,
      `com.microsoft.azure:synapseml_2.13:${expectedVersion}`,
    );
    assert.equal(artifact.releaseTag, `v${expectedVersion}`);
  }
});

test("unpublished documentation can be previewed but cannot be deployed", () => {
  const version = "2.0.0";
  const versions = [version, "1.0.0"];
  const lock = {
    "spark4.0": "1.0.0-spark4.0",
    "spark4.1": "1.0.0-spark4.1",
  };
  validatePublicationLock(version, lock, versions, true);
  assert.throws(() => validatePublicationLock(version, lock, versions, false));
  assert.throws(() => validatePublicationLock(version, lock, versions, "true"));
  for (const invalid of [undefined, "9.0.0-spark4.0", "1.0.0-spark4.1"]) {
    assert.throws(() =>
      validatePublicationLock(
        version,
        { ...lock, "spark4.0": invalid },
        versions,
        true,
      ),
    );
  }
  const released = {
    "spark4.0": "2.0.0-spark4.0",
    "spark4.1": "2.0.0-spark4.1",
  };
  validatePublicationLock(version, released, versions, false);
  const defaultRelease = { ...released, "spark4.0": lock["spark4.0"] };
  validatePublicationLock(version, defaultRelease, versions, false, "1.0.0");
  assert.throws(() =>
    validatePublicationLock(version, defaultRelease, versions, false),
  );
  assert.throws(() =>
    validatePublicationLock(version, released, versions, false, "1.0.0"),
  );
  assert.throws(() =>
    validatePublicationLock(version, defaultRelease, versions, false, "0.9.0"),
  );
});

test("new release guidance only links artifacts produced by the public release", () => {
  const sourceInstall = read("docs", "Get Started", "Install SynapseML.md");
  const readme = read("README.md");
  const index = read("website", "src", "pages", "index.js");
  for (const guide of [readme, sourceInstall]) {
    assert.doesNotMatch(guide, /SynapseMLExamplesv[0-9.]+\.dbc/);
    for (const artifact of artifacts) {
      assert.ok(
        guide.includes(
          `https://github.com/microsoft/SynapseML/tree/${artifact.releaseTag}/docs`,
        ),
      );
    }
  }
  assert.doesNotMatch(index, /SynapseMLExamplesv[0-9.]+\.dbc/);
  assert.ok(index.includes("${artifact.releaseTag}/docs"));
  const sourceR = read("docs", "Reference", "R Setup.md");
  assert.match(sourceR, /^r_installation: source$/m);
  assert.doesNotMatch(sourceR, /blob\.core\.windows\.net\/rrr\//);
  const versionedR = read(
    "website",
    "versioned_docs",
    `version-${currentVersion}`,
    "Reference",
    "R Setup.md",
  );
  const sourceBuiltR = /^r_installation: source$/m.test(versionedR);
  if (publishedPorts["spark4.0"] !== `${currentVersion}-spark4.0`) {
    assert.ok(sourceBuiltR, "new releases must not invent R archive downloads");
  }
  if (sourceBuiltR) {
    const versionedInstall = read(
      "website",
      "versioned_docs",
      `version-${currentVersion}`,
      "Get Started",
      "Install SynapseML.md",
    );
    assert.doesNotMatch(versionedInstall, /SynapseMLExamplesv[0-9.]+\.dbc/);
    for (const artifact of artifacts) {
      assert.ok(versionedInstall.includes(`/tree/${artifact.releaseTag}/docs`));
    }
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
    const expectedVersion = artifact.branch === "spark4.0"
      ? spark40Version : currentVersion;
    assert.equal(artifact.pythonPackage, `synapseml==${expectedVersion}`);
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
    validateSpark40References(markdown, spark40Version);

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
  const versionedOnnx = read(
    "website",
    "versioned_docs",
    `version-${currentVersion}`,
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
  for (const guide of [
    deepLearning, versionedDeepLearning, onnx, rSetup, versionedRSetup,
  ]) {
    validateSpark40References(guide, spark40Version);
  }
  // Older published ONNX snapshots predate the Spark 4.0 examples.
  if (versionedOnnx.includes("-spark4.0") ||
      /^r_installation: source$/m.test(versionedRSetup)) {
    validateSpark40References(versionedOnnx, spark40Version);
  }

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
