const pythonPackage = "synapseml==1.1.3";
const pythonPackagePrefix = "synapseml==";
if (
  !pythonPackage.startsWith(pythonPackagePrefix) ||
  pythonPackage.length === pythonPackagePrefix.length
) {
  throw new Error(`Invalid SynapseML Python package pin: ${pythonPackage}`);
}
const version = pythonPackage.slice(pythonPackagePrefix.length);
const repository = "https://mmlspark.blob.core.windows.net/maven";

const installArtifacts = Object.freeze({
  version,
  repository,
  spark35: Object.freeze({
    coordinate: `com.microsoft.azure:synapseml_2.12:${version}`,
    pythonBaseline: "3.11",
    pythonPackage,
    pysparkSpec: ">=3.5,<3.6",
    scalaBinaryVersion: "2.12",
  }),
  spark40: Object.freeze({
    coordinate: `com.microsoft.azure:synapseml_2.13:${version}-spark4.0`,
    pythonBaseline: "3.12",
    pythonPackage,
    pysparkSpec: ">=4.0.1,<4.1",
    scalaBinaryVersion: "2.13",
  }),
  spark41: Object.freeze({
    coordinate: `com.microsoft.azure:synapseml_2.13:${version}-spark4.1`,
    pythonBaseline: "3.13",
    pythonPackage,
    pysparkSpec: ">=4.1,<4.2",
    scalaBinaryVersion: "2.13",
  }),
});

module.exports = installArtifacts;
