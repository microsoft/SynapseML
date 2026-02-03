# Handoff Document: R Test Fixes for Spark 4.0 Upgrade

**Branch:** `brwals/rtestfix`
**Date:** 2026-02-03
**Author:** Brendan Walsh

---

## Summary

This branch fixes R test generation and execution after the SynapseML upgrade to Spark 4.0, Java 17, and Scala 2.13. The R codegen pipeline was broken due to multiple issues related to the Java/Scala version changes.

---

## Branch State

```
Local commits (top = most recent):
  8a892df511  ignore test for broken service
  d295d39167  Fix R Tests
  2dd0a65392  [chore] Upgrade to Spark4.0, Java 17, Scala 2.13, Python 3.12.11
```

**Warning:** Local branch has diverged from `origin/brwals/rtestfix`. The remote has an older fix (`f2b2c872 r fix`) that should be replaced. To sync:

```bash
git push --force-with-lease
```

---

## Problems Fixed

### 1. Java Module System Issues (Java 17)

**Problem:** Java 17's module system restricts access to `java.sql` classes needed by Scala reflection during codegen.

**Fix:** `project/CodegenPlugin.scala` - Fork Java process with explicit module access:
```scala
val javaOpts = (Test / javaOptions).value ++ Seq(
  "--add-modules=java.sql",
  "--add-opens=java.sql/java.sql=ALL-UNNAMED"
)
```

### 2. JSON Config Argument Quoting

**Problem:** Long JSON config strings passed as command-line arguments break due to shell quoting/escaping issues.

**Fix:** Write config to temp file and pass `@filename` syntax:
- `project/CodegenPlugin.scala` - writes to temp file, passes `@/path/to/file`
- `core/src/main/scala/.../RCodegen.scala` - reads `@filename` args
- `core/src/test/scala/.../RTestGen.scala` - reads `@filename` args

### 3. Stage Project Detection (Java 17)

**Problem:** In Java 17, `getCodeSource().getLocation()` returns a classes directory path instead of a JAR file path. The old code extracted project name from JAR filename.

**Fix:** `core/src/test/scala/.../Fuzzing.scala` - Extract project name from path by finding the directory before `/target/`:
```scala
val pathParts = location.split("/")
val targetIdx = pathParts.indexOf("target")
pathParts(targetIdx - 1)  // e.g., "core" or "cognitive"
```

### 4. Local JAR Loading in R Tests

**Problem:** R tests tried to load `synapseml-core` via Maven coordinates, but SNAPSHOT versions may not be published.

**Fix:** `core/src/test/scala/.../RTestGen.scala` - Load freshly built JAR directly via `spark.jars` config:
```scala
val localCoreJar: Option[String] = conf.jarName.map { jar =>
  new File(conf.targetDir, jar).getAbsolutePath
}
// Use spark.jars for local JAR, spark.jars.packages only for Avro
```

### 5. Hardcoded JAVA_HOME Path

**Problem:** `tools/tests/run_r_tests.R` had a hardcoded macOS JDK path that doesn't work on Linux/CI.

**Fix:** Auto-detect JAVA_HOME from environment or PATH:
```r
java_home <- Sys.getenv("JAVA_HOME", unset = NA)
if (is.na(java_home) || !nzchar(java_home)) {
  java_bin <- Sys.which("java")
  # ... infer from java binary location
}
```

### 6. Retired Azure Maps API

**Problem:** Azure Maps Spatial API (Point in Polygon) was retired on September 30, 2025. Tests fail because the service no longer exists.

**Fix:** `cognitive/src/test/scala/.../AzureMapsSuite.scala` - Added `@Ignore` annotation to `AzMapsPointInPolygonSuite`, and wrapped `beforeAll()`/`afterAll()` lifecycle methods in try-catch blocks to prevent R test generation from failing when the retired API calls throw exceptions.

---

## Files Changed

| File | Change |
|------|--------|
| `project/CodegenPlugin.scala` | Fork process with JVM opts, temp file for config |
| `core/src/main/scala/.../RCodegen.scala` | Support `@filename` argument syntax |
| `core/src/test/scala/.../RTestGen.scala` | Support `@filename`, local JAR loading |
| `core/src/test/scala/.../Fuzzing.scala` | Fix stageProject extraction for Java 17 |
| `tools/tests/run_r_tests.R` | Auto-detect JAVA_HOME |
| `cognitive/.../AzureMapsSuite.scala` | `@Ignore` retired API test |
| `core/.../HTTPTransformerSuite.scala` | Minor test fix |

---

## Verification Commands

```bash
# Compile and run R codegen
sbt rCodegen

# Generate R tests
sbt rTestGen

# Run R tests (requires R + sparklyr installed)
sbt rTest

# Run Azure Maps tests (should skip ignored test)
sbt "cognitive/testOnly *AzureMaps*"
```

---

## Known Issues

### Azure Maps SearchAddress Malformed Response

The Azure Maps geocoding service occasionally returns malformed GeoJSON responses with empty coordinate arrays (`coordinates: [[[]]]`). This is an **external service issue**, not a code bug. The test may be flaky depending on which addresses are geocoded.

If this becomes a problem, options:
1. Add `@Ignore` to `AzMapsSearchAddressSuite`
2. Add defensive parsing in `AzureMapsJsonProtocol`
3. Mock the service responses in tests

---

## Next Steps

1. **Push changes:** `git push --force-with-lease`
2. **Verify CI:** Ensure R tests pass in CI pipeline
3. **Merge to spark4.0 branch:** Once verified, merge to the main Spark 4.0 integration branch

---

## Architecture Notes

### R Codegen Flow

```
sbt rCodegen
    └─> CodegenPlugin.scala (forks Java process)
        └─> RCodegen.main()
            └─> Generates R wrapper functions in core/target/.../r/

sbt rTestGen
    └─> CodegenPlugin.scala (forks Java process)
        └─> RTestGen.main()
            └─> Generates R test files using RTestFuzzing trait

sbt rTest
    └─> Runs tools/tests/run_r_tests.R
        └─> Loads sparklyr, connects to Spark
        └─> Runs generated testthat tests
```

### Key Config Object

`CodegenConfig` (defined in `Codegen.scala`) carries paths and settings through the pipeline:
- `rSrcDir` - where R source files go
- `rTestDir` - where R tests go
- `jarName` - the built JAR to load
- `targetDir` - build output directory

---

## Contact

For questions about this work, check git blame or the PR discussion on GitHub.
