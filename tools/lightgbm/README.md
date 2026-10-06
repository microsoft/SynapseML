# Preparing the LightGBM native package

SynapseML uses the Maven dependency
`com.microsoft.ml.lightgbm:lightgbmlib:4.7.0`. It contains the Java bridge and
matched CPU native libraries for Linux, macOS, and Windows x86-64.
The package version is distinct from a SynapseML release version.

**Publication gate:** the new Maven version must be published and independently
resolved from Maven Central before this dependency change can ship. Preparing
the local repository below does not publish anything, supply signing authority,
or make the upgrade merge-ready. Do not change CI to consume a developer's cache.

## Prepare and verify

Requirements: Python 3.8 or newer, a JDK with `javap`, and HTTPS access to the
official LightGBM GitHub release. No Python dependencies are needed.

From the SynapseML checkout, select output and cache directories outside the
source tree:

```powershell
python tools\lightgbm\prepare_native_package.py `
  --output C:\scratch\lightgbm470\repository `
  --cache C:\scratch\lightgbm470\cache
```

On Linux, use the same script and options with Linux paths. `--javap` can select
a specific JDK executable.

The script:

- Verifies the hashes of all three official platform JARs, the release
  `commit.txt`, and the source license against `native-package.json`.
- Verifies that every platform contains its matched native pair and the same
  Java classes and method/field declarations.
- Uses the official Linux Java 8 wrappers for every platform. The upstream
  macOS JAR's wrappers require Java 21, despite having matching declarations.
  Reusing that JAR's classes would break the supported older JVMs.
- Preserves each native binary byte-for-byte. No native code is rebuilt, and
  binaries from different releases are never mixed.
- Produces a deterministic JAR, POM, and SHA-1/SHA-256/SHA-512 checksum files in
  a Maven-layout repository. The JAR embeds the source commit, input and entry
  hashes, package script hash normalized to LF line endings, and MIT license.

A bad cached hash, missing library, different Java/JNI declarations, or existing
different output fails explicitly. The script does not overwrite an existing
different version directory. Use a fresh output directory when changing the
packaging script; the embedded script hash intentionally changes the package.
Platform API parity does not prove native runtime compatibility.

## Validate before publication

Add the isolated repository only to the current SBT session. For example:

```text
set ThisBuild / resolvers += "lightgbm-candidate" at file("""C:\scratch\lightgbm470\repository""").toURI.toString
lightgbm/compile
lightgbm/Test/compile
lightgbm/scalastyle
lightgbm/Test/scalastyle
lightgbm/testOnly com.microsoft.azure.synapse.ml.lightgbm.split1.LightGBMNativeCompatibilitySuite
```

Use the repository's local-setup skill to select the correct JDK and platform
path. Check the resolved dependency and loaded native hashes, not just the
Python package version. Restart the Spark application when switching natives.

The compatibility suite drives two native workers through public bulk `fit`
with a 101-category feature whose first split selects 30 categories at
`maxCatThreshold=32`. It checks predictions, actual participating training tasks,
repeated fits, persistence, classifier probabilities, and dense/sparse
single-worker streaming with reusable references. Run it with at least two
concurrent Spark task slots.

The checked-in `lightgbm/src/test/resources/native-3.3.510.json` fixture contains
a synthetic native model, serialized reference dataset, expected predictions,
producer package hash, and training parameters from the previously published
engine. The suite loads the old model and reuses the old reference in both dense
and sparse streaming fits.

To establish the original failure, run the distributed test in a disposable
forked JVM with `3.3.510`; the expected result is a native process crash, not an
ordinary assertion failure. Never run that baseline in a shared test JVM.
Local Spark workers do not substitute for a multi-executor Fabric run.

Package-tool tests:

```powershell
python -m pytest tools\lightgbm\tests -q
python -m black --check tools\lightgbm
```

## Publisher handoff

The authorized owner of `com.microsoft.ml.lightgbm` must review the prepared
artifacts and complete the native publication process. This script prepares
the binary and POM only; the publisher must also produce and verify the required
source/Javadoc artifacts, signatures, and repository metadata under the
approved publishing process. It does not retrieve credentials or upload files.
Do not replace the already-published `3.3.510` artifact.

Before publishing and releasing:

- Run the upgraded package on supported Windows, macOS, Linux, Spark/Scala,
  and Fabric runtimes. Check both native libraries on driver and executors.
- Cover distributed dense/sparse streaming and bulk, ranker, validation and
  early stopping, old saved models, reference data, continued training, and
  representative performance. Keep CUDA unsupported until separately validated.
- Verify native artifact contents, license notices, source/build provenance,
  and reproducibility before signing.
- Resolve the published package from an empty cache, rerun the relevant
  SynapseML checks, and then release compatible SynapseML/runtime updates.

The upstream fix is
[LightGBM#6738](https://github.com/lightgbm-org/LightGBM/pull/6738).
Delivery is tracked in
[microsoft/SynapseML#2697](https://github.com/microsoft/SynapseML/issues/2697).
Do not claim this also resolves every connection-refused or streaming-ingestion
report.
