# Spark 3.5 branch context

This reference intentionally serves both `master` and `spark3.5`.

## Branch mapping

- `master` is the canonical development branch and currently uses the Spark 3.5
  runtime family. Ordinary features and fixes target `master`.
- `spark3.5` is the shared Spark 3.5 release branch. It can differ from
  `master` despite using the same Spark generation.
- Always verify exact Spark/Scala versions and dependency pins in the target
  branch's live `build.sbt` and `environment.yml`.

## Sync policy

- PRs targeting `master` rebase onto latest `master` and push with lease.
- Synchronize the shared `spark3.5` branch by merging `master`; never rebase or
  force-push that shared ref.
- Cross-version work lands on `master` first unless it exists only to preserve
  the release branch.

## Validation

- Master validation covers the primary Spark 3.5 runtime, so compatibility
  replay can intentionally omit a duplicate Spark 3.5 leg. This does not waive
  direct CI for PRs targeting the `spark3.5` branch.
- Inspect workflows and Azure triggers on the actual target branch; historical
  GitHub coverage for `spark3.5` differed from `master`.
- Confirm the affected suites were selected and executed. Green matrices can
  omit an unclaimed package or explicit test class.
- Recheck target movement immediately before readiness.

## Fabric LightGBM baseline

- At the 2026-08-26 baseline, Fabric Runtime 1.3 supplies Python `lightgbm`
  4.3.0, but its JVM/SWIG classes load from
  `com.microsoft.ml.lightgbm:lightgbmlib:3.3.510`.
- The managed JAR is byte-for-byte identical to the Maven Central artifact
  (SHA-256 `f2b1b13172699832594303ab4c04f3bc8fc2d24737e3e8c11d98d69a88c09272`).
- Do not infer the Maven dependency version from the Python package version.
  Changing the JNI/SWIG artifact is a separate compatibility change.
