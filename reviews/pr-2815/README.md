# Review evidence for PR #2815

## Current validation and cumulative inspection

Reduced Azure build
[246576831](https://dev.azure.com/msdata/A365/_build/results?buildId=246576831)
passed all 10 active jobs at `fcd5b237741c789bd8d1796cca36dc171d279047`.
The ordinary cognitive Python suite passed 258 tests with 18 skips. The semantic
notebook artifact verifies matching Python/JVM artifacts, the expected supplier,
amount and delivery date, confidence/source grounding, and saved-handle recovery
with the same operation and fields. CI-owned analyzer cleanup returned HTTP 204.

The HTTP smoke test now uses a loopback server rather than the public dog.ceo
endpoint, preserving real Spark/JVM HTTP execution and asserting all 20 responses.
The captured external-call stall established a response-header wait, not whether
the public service or an intermediary caused it.

Direct cumulative inspection covered the CU production implementation, shared-core
changes, pipeline wiring, live/diagnostic runners, notebook and guide, with
targeted regression-assertion review against Rana's five requests. It found stale
guide examples missing explicit analyzer model roles and stale CI cleanup text.
This follow-up aligns the guide with the working notebook and adds an assertion
on the actual creation payload. That assertion failed before the guide fix;
afterward all three runnable-guide tests and four subtests passed. The Python
formatting and whitespace checks passed.

No new runtime blocker was identified by this inspection. This is a direct
inspection, not a new independent or automated multi-model signoff; the cancelled
review orchestration was not restarted. Prior full build 246564982 passed the CU
Scala and shared-core jobs but failed the external HTTP smoke test and lost a form
test agent. Full PR validation, including coverage upload, still needs a clean run
after these fixes. The guide/test follow-up changes no runtime or pipeline code.

## Historical Rana follow-up and mainline integration

The current follow-up removes the generic JVM-backed `isSet`, `hasDefault`,
`getOrDefault`, and `extractParamMap` overrides to prevent automatic credential
logging. Explicit typed getters, execution, copying, and persistence remain
supported. The earlier case-class inspection correction below is historical,
not the current generic-inspection contract.

The follow-up also adds opt-in cancellable analysis throttling recovery,
optional input truncation, saved-result examples, and the semantic extraction
notebook. The [stage-11 driver record](task-5837660-attempt-6-stage-attempt-11-review-1-claude-opus-5.5.md)
retains the six-lens outcomes and subsequent explicitly approved test-only
interrupt-wait fix. Earlier findings, failed transports, and their resolutions
remain in the numbered reports; they are not additional clean signoffs.

Before publication, the complete change set was rebased onto master
`33200c265adf16d10272e6abcac00f6aa9dccb32`. Conflict resolutions preserve
upstream method binding, parameter-default handling, assigned Spark sessions,
and notebook-selection coverage alongside the CU changes. The two distinct
persistence-test fixtures now have distinct names, and the codegen test remains
within the existing file-length limit. CU implementation and CU Scala test
contents are unchanged by this integration.

Local validation of the rebased tree on 2026-11-12:

- Core and cognitive compilation, test compilation, Scala style, and regenerated
  core/OpenCV/cognitive Python bindings passed.
- All 146 selected shared-core Scala tests and 208 CU Scala tests passed.
- The affected generated-Python, credential, and notebook-cell regressions
  passed 81 tests and 213 subtests.
- CI runner and pipeline wiring checks passed 34 tests with 9 platform skips.
- Changed Python formatting, sidebar syntax, and whitespace checks passed.

No new automated review was launched after the user cancelled the next round.
These results do not turn historical reviews into review of the rebased tree.

The cognitive Python CI lane now runs the live CU suite separately using
`synapseml-openai-3` and `mmlspark-build-keys/openai-api-key-3`, under the existing
approved-fork build policy. No shared defaults or model deployments were changed.
Live CU endpoint/analyzer readiness, packaged live behavior, and the complete
Azure pipeline remain unverified until the requested build runs.

### Semantic notebook CI follow-up

The same cognitive lane now includes a separate step executing the semantic
notebook's actual code cells, followed by recovery from saved handles in a fresh
namespace. Before service requests it verifies installed wheel contents and
loaded packaged JARs against the build outputs. Its redacted evidence records
commit/artifact/runtime provenance, actual semantic values and grounding, and
the recovery outcome. This notebook creates its own uniquely named analyzer;
it does not modify shared model defaults or retry the whole workload.

Offline validation passed 170 CI helper tests with 9 platform skips, then 47
runner/wiring tests with 9 platform skips after final hardening. The actual
notebook regressions passed 4 tests and 22 subtests; the changed runner/runtime
reflection path subsequently passed its focused Spark regression. The loopback
runner observed exactly one submission and one analyzer creation across initial
execution and recovery. Changed Python formatting and whitespace checks passed.
These are offline results, not live semantic-extraction evidence. No additional
automated review round was launched.

## Historical case-class parameter inspection correction

The follow-up to commit `8ff7754af5425d393ef706cc1312cb10ab5f4580` makes
`getOrDefault` and `extractParamMap` reuse the typed getter's parameter-aware
conversion. Case-class sequences become JSON-serializable dictionaries while
the existing raw-byte guard is preserved. The [focused correction review](task-5837660-attempt-7-review-0-harness.md)
covers this small follow-up, not a repeat of the full connector review.

The new regression failed before the fix for both APIs in configured and
restored wrappers. After the fix, all 35 selected Python tests passed, including
the complete ServiceParam bridge suite and affected CU scalar, map, sequence,
column-binding and raw-byte paths. Core code generation and pinned formatting
also passed. No live service was called for this local validation.

## Earlier three-model source review

The three retained reports are the final targeted detailed-correctness pass,
following the six-stage High review and five targeted fix/review iterations.
All three returned CLEAN for the correction scope; this is not a claim that
all six stages were rerun on the final source revision.

| Reviewer | Report |
|---|---|
| GPT-6.1 Sol | [Final report](task-5837660-attempt-4-stage-attempt-5-review-4-gpt-6.1-sol.md) |
| Grok 4.7 | [Final report](task-5837660-attempt-4-stage-attempt-5-review-4-grok-4.7.md) |
| Claude Opus 5.5 | [Final report](task-5837660-attempt-4-stage-attempt-5-review-4-claude-opus-5.5.md) |

The reviewed source was published in commit
[`1bab6fb4aef3d00b616f09b023a03062abc9695e`](https://github.com/BoraElkin/SynapseML/commit/1bab6fb4aef3d00b616f09b023a03062abc9695e),
against integrated master `d3f3af6bd3e4b7d191f51c9eb29f9aeffe788193`.
The 14-path correction patch reviewed in the final pass has SHA-256
`b0640b9e490dccf25d2345ac44668583f9155c40a4f0c83d4400b58f7642f347`.
The final input, runtime-identity and scoped-tool audits passed without
warnings or violations. No confirmed finding remained open in that scope.

At that revision, local validation covered 174 offline CU Scala tests and four
affected Python public paths, including cancellation, after affected style and
compilation checks. Earlier full Python/shared-HTTP suites retain their original
snapshot attribution. Live CU, full Azure pipeline execution, packaged-runtime
and performance validation remain outstanding; these reports are not merge
approval.

## Historical findings and resolutions

The [complete 53-report archive](https://github.com/BoraElkin/SynapseML/tree/bd7ed4d84fe85ea16379000d7100eede66945870/reviews/pr-2815)
is retained byte-for-byte in the rebased branch's history. Its
[driver record](https://github.com/BoraElkin/SynapseML/blob/bd7ed4d84fe85ea16379000d7100eede66945870/reviews/pr-2815/task-5837660-attempt-3-stage-attempt-2-review-1-gpt-6.1-sol.md)
preserves the review decisions, original findings, dispositions, fix/regression
history and final loop closure. Historical status statements describe their own
checkpoints, not the current PR state.

All 53 paths, blob hashes and file modes were verified against the original
archive after rebasing onto `6c2699defcfb24d5bff5ba55f2252ba2d8cbea24`.
The original review-revision attribution above is unchanged; historical reports
do not establish review of the newly integrated master fixes.
The three retained source reports are unchanged.
Superseded, failed and scope-invalid results remain in the archive with their
original verdicts; they are not counted as clean signoffs. This artifact-only
cleanup changes no implementation, test, build or configuration file.
