# CI fixture regression review

Reviewer: GPT-6 Astra. Mode: direct review of the three-line fixture correction.

## Failure evidence

[Azure build 236598594](https://dev.azure.com/msdata/A365/_build/results?buildId=236598594)
failed in three independent jobs:

| Job | Evidence | Cause |
| --- | --- | --- |
| UnitTests core | Unit Test log 1070, `PipelineTestCoverageSuite` | The new concrete nested `NotebookFixture` was classified as an unclaimed CI suite. Both attempts failed this one test; 296 other core tests passed per attempt. |
| UnitTests language | Unit Test log 908 | The AzureCLI task received a certificate for `*.azureedge.net` when connecting to `msdata.visualstudio.com`. Both attempts failed before SBT or the test script ran. |
| Release Branch Compatibility Check spark4.1 | Apply PR changes log 139 | Three-way replay conflicts in `FabricNotebookTests.scala`. The release branch lacks the immediate artifact cleanup from microsoft/SynapseML#2725, including `FabricTestArtifactTracker.withArtifact`. |

The core failure is a regression introduced by the preflight follow-up.
The earlier targeted validation omitted the repository-wide suite-discovery gate.
Running that exact gate locally on the published source reproduced the same
unclaimed `NotebookFixture` failure.

## Correction

Declare the helper `private abstract class NotebookFixture` and instantiate its
two previously concrete uses as anonymous subclasses. Its other two uses already
create anonymous subclasses. The helper is not a standalone CI suite; its parent
suite executes the fixture explicitly.

`PipelineTestCoverageSuite.parseDeclarations` already distinguishes abstract
classes from concrete classes while retaining abstract classes in its ancestry
graph. No scanner exclusion, matrix entry, skipped test, or pipeline edit is needed.

## Direct review

| Theme | Finding |
| --- | --- |
| Correctness | The named helper is abstract; all four actual uses remain concrete, runnable fixtures. |
| Architecture | The fixture stays private to its owning suite. No production or public API changes. |
| Robustness | Constructor laziness, interruption caching, cleanup, and executor behavior are unchanged. |
| Detailed correctness | Only the declaration modifier and two anonymous-subclass bodies change. Existing fixture overrides remain intact. |
| Test coverage | Run the exact failed matrix gate together with the tracker and artifact-name suites. Do not substitute the helper tests alone for the discovery check. |
| Polish and safety | No TLS bypass, weakened replay checks, broad CI selector, or unrelated refactor. |

No additional defects found in the correction.

## Remaining CI constraints

The language failure is an agent/service TLS failure, not a failing language test.
A new agent attempt can establish whether it was transient; certificate
verification must remain enabled.

Spark 4.1 tip `b4ca894139a93b59f9da9c38cd9627d5199cc1ff` does not contain master
commit `1f33e376535970724906ce43cc8935b147c94413`. File comparisons independently
confirm that the required cleanup methods and call sites are absent. Retrying
unchanged branch content cannot resolve this replay conflict. The release branch
needs the existing master prerequisite merged before this PR can be reported
compatible. This correction does not change shared release branches.

## Validation

- Before the correction, `core/testOnly
  com.microsoft.azure.synapse.ml.core.test.pipeline.PipelineTestCoverageSuite`
  failed with the same unclaimed fixture reported by Azure.
- After the correction, that exact gate passed alongside
  `FabricTestArtifactTrackerSuite` and `FabricArtifactNamesSuite`: 44 tests passed,
  none failed, and none were skipped.
- All-module `scalastyle`, `Test/scalastyle`, `compile`, and `Test/compile` passed
  using the repository's JDK 11 wrapper.
- `git diff --check` passed. The implementation diff contains only the three
  fixture declaration/instantiation lines. Existing cleanup and pipeline
  configuration are unchanged.
