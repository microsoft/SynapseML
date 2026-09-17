const version = "1.1.3";
const pythonPackage = `synapseml==${version}`;
const repository = "https://mmlspark.blob.core.windows.net/maven";

const installArtifacts = Object.freeze({
  version,
  repository,
  spark35: Object.freeze({
    branch: "master",
    coordinate: `com.microsoft.azure:synapseml_2.12:${version}`,
    pythonBaseline: "3.11",
    pythonPackage,
    pysparkSpec: ">=3.5,<3.6",
    releaseTag: `v${version}`,
    sparkRuntime: "3.5.x",
    scalaBinaryVersion: "2.12",
  }),
  spark40: Object.freeze({
    branch: "spark4.0",
    coordinate: `com.microsoft.azure:synapseml_2.13:${version}-spark4.0`,
    pythonBaseline: "3.12",
    pythonPackage,
    pysparkSpec: ">=4.0.1,<4.1",
    releaseTag: `v${version}-spark4.0`,
    sparkRuntime: "4.0.1+ (<4.1)",
    scalaBinaryVersion: "2.13",
  }),
  spark41: Object.freeze({
    branch: "spark4.1",
    coordinate: `com.microsoft.azure:synapseml_2.13:${version}-spark4.1`,
    pythonBaseline: "3.13",
    pythonPackage,
    pysparkSpec: ">=4.1,<4.2",
    releaseTag: `v${version}-spark4.1`,
    sparkRuntime: "4.1.x",
    scalaBinaryVersion: "2.13",
  }),
});

module.exports = installArtifacts;
