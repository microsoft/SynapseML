# Round 1 review

## Review summary

- Round: 1
- Theme: Broad correctness, security, and spec conformance
- Mode: Sequential, direct review
- Model: `gpt-6-astra`
- Artifact: `reviews\task-java8-codegen-20260917-attempt-1-review-1-gpt-6-astra.md`
- Issues found: 0
- Verdict: **CLEAN**, static review only

Reviewed the two uncommitted changes against local HEAD `1d12c2b8`, matching
the supplied head for public PR #2719 targeting `master`. Read `AGENTS.md`
and relevant local context. No concrete correctness, security, or spec
defect was found in these changes.

## Evidence checklist

- [x] `core\src\test\scala\com\microsoft\azure\synapse\ml\codegen\CodegenDiscoverySuite.scala:21-30`
  replaces the Java 9-only API with `ClassLoader.getSystemClassLoader.getParent`.
  Both methods are available on Java 8. With the standard application loader,
  this selects the extension loader on Java 8 and the platform loader on
  JDK 11, keeping application classes in the isolated `URLClassLoader`.
  The new reference-identity requirement rejects a parent-loaded probe before
  invocation. Classpath construction and the fixed reflective target are unchanged.
- [x] `core\src\test\scala\com\microsoft\azure\synapse\ml\codegen\CodegenDiscoverySuite.scala:27-35`
  retains the `try`/`finally` around context-loader installation, probe loading,
  the new requirement, and invocation. The existing cleanup still restores
  the original context loader before closing the isolated loader, including
  when the new requirement fails.
- [x] `core\src\test\scala\com\microsoft\azure\synapse\ml\codegen\CodegenDiscoverySuite.scala:39-76`
  retains the packaged-fixture check, production Python/R output checks,
  test-fixture exclusion, scoped and unscoped discovery checks, and
  invalid-constructor failure check. The production discovery implementation
  in `core\src\main\scala\com\microsoft\azure\synapse\ml\core\utils\JarLoadingUtils.scala`
  remains unchanged. This is source-level coverage evidence, not a test result.
- [x] `.github\skills\synapseml-branches\references\branch-spark3p5.md:32-38`
  distinguishes Java 8 CI from the local JDK 11 default, consistent with the
  retained CI logs and
  `.github\skills\synapseml-local-setup\scripts\synapseml-sbt.sh:16`.
  The guide requires checking the actual CI startup log rather than treating
  a local pass as Java 8 compatibility evidence.
- [x] `.github\skills\synapseml-branches\references\branch-spark3p5.md:44-47`
  preserves overlapping release-patch conflicts as a merge-order gate and
  requires separate validation of the resolved sync. This agrees with
  `pipeline.yaml`, which replays onto the fetched release tip and exits on
  patch-application failure. The change does not bypass that gate.
- [ ] New Java 8 compilation, suite execution, and lint results are not
  available from this review. The driver owns JDK installation and validation.

## Evidence limits

For CI build `236308415`, as identified in the request, the retained session
artifacts `master-current-log948.json` and `master-current-log1566.json` both
show Temurin Java `1.8.0_504`, the error
`value getPlatformClassLoader is not a member of object ClassLoader`, and
`core / Test / compileIncremental` failure. They establish the failure before
this fix, not a successful build of the uncommitted changes. The reported
earlier JDK 11 validation does not establish Java 8 compatibility.

No tests, builds, or linters were run. No network calls, agents, factories,
private-worktree inspection, code fixes, or commits were performed. The only
file written is this review. This clean round-1 verdict does not establish
green CI or merge readiness; Java 8 validation and the release replay gate
remain outstanding.
