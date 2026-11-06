---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 22
round: 1
timestamp: "2026-11-05T14:39:20Z"
outcome: "issues_found"
reviewer_model: "gpt-6.1-sol"
input_sha256: "c0f3c440cbd39042b5a4bc9068bce1cbcf56145c9ab352eb1f2c6dbd0a8162e5"
---

---
producer: review-code
schema_version: 1
tier: Max
contract: direct
execution_backend: copilot-cli
attempt: 1
stage_attempt: 22
round: 1
timestamp: "2026-11-05T14:28:51.823Z"
outcome: issues_found
reviewer_model: gpt-6.1-sol
input_sha256: 89c7ee46006dcdf0bf8f804eed43db4f55cc34dcb738731dee17aa7e36a4a237
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: gpt-6.1-sol
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-22-review-1-gpt-6.1-sol.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Reviewed the supplied changed paths, including CU protocol, transport, DNS, parsing bounds, management, and durable-writer removals.
- [x] Traced partition handling and planning through `CognitiveServiceBase.scala`, `SimpleHTTPTransformer.scala`, `HTTPTransformer.scala`, and `DatasetExtensions.scala`.
- [x] Reviewed Python parameter/code-generation changes in `Utils.py`, `Wrappable.scala`, and `ContentUnderstandingParams.scala`; traced persistence through `ComplexParamsSerializer.scala`.
- [x] Inspected the supplied Scala/Python tests, documentation changes, CI selector, and caller-supplied validation evidence.
- [ ] Tests were not executed during this read-only review; live-service and external CI evidence remain unavailable.

## Issues

### Issue 1: Save/load bypasses the explicit analyzer-ID guard
- **Severity**: Medium
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstanding.scala
- **Line(s)**: 440–442
- **Description**: `isSet(analyzerId)` does not establish explicit user selection after persistence. The inherited writer serializes `instance.extractParamMap()`—including defaults—into metadata `paramMap` (`core\src\main\scala\org\apache\spark\ml\ComplexParamsSerializer.scala:111–126`). Loading applies those entries as settings through `metadata.getAndSetParams(instance)` at line 161. Consequently, saving and loading a stage whose analyzer ID was unset or cleared makes this guard pass. `createAnalyzer("{}", allowReplace = true)` can then send a PUT to the default `prebuilt-read` ID without any explicit `setAnalyzerId` call. The added guard tests do not cover reloaded stages.
- **Risk**: Round-tripping an analysis-only stage silently removes the required explicit-target safeguard for administrative creation/replacement calls.
- **Suggested Fix**: Preserve explicit analyzer-ID provenance across persistence rather than relying on defaults materialized as settings. Do not blacklist the default ID. Add Scala/Python regressions proving saved-and-loaded unset/cleared IDs reject both replacement modes before HTTP, while explicitly configured IDs remain supported.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending.
- **Why**: Explicit provisioning intent must survive save/load.
- **How verified**: Static writer → reader → guard trace; regression execution pending.

### Driver resolution - attempt 23
- **Status**: Fixed for newly saved CU stages; legacy compatibility policy explicitly selected by the user. Original finding/verdict retained.
- **Cause**: The new `isSet(analyzerId)` guard was incomplete against the unchanged SynapseML writer, which materialized defaults as explicit settings. Spark 3.5 `copyValues` was independently inspected and already preserves the distinction; it is unchanged.
- **Change**: `ComplexParamsWritable` has a protected opt-in controlling whether non-complex defaults also appear in the explicit metadata `paramMap`. The default preserves every existing writer's behavior. Only CU opts out; it writes explicit settings separately from `defaultParamMap` through the existing encoding/complex-parameter machinery. The reader, management signatures, parameter shapes and normal copy logic are unchanged. No provenance flag, custom loader or ID blacklist.
- **User decision**: Preserve old saved models' stored settings. Their historical selection intent cannot be recovered, and the docs state this limitation rather than silently reclassifying those settings or disabling management. Newly saved unset/cleared IDs stay implicit; explicitly selecting the default ID remains allowed.
- **Regression evidence**: Scala and Python unset/cleared save/load regressions failed before the fix; explicit scalar/column and legacy controls passed. Fixed coverage includes clear/copy/save/load/resave, generic Scala/Python setters, explicit default/custom IDs, both replacement modes, default analysis/lookup, PipelineModel persistence, and legacy load/resave. Core coverage checks both writer modes and preservation of complex values and defaults.

## Driver scrutiny and decision - stage 1 attempt 23
- **Pre-review gate**: User-required scrutiny completed before dispatch. All three findings were collected/audited before source edits. Each changed contract was traced across callers/lifecycle transitions, red tests were executed, unchanged-behavior controls were included, and the final production diff was inspected. No unresolved concern was knowingly deferred to the next reviewer.
- **Creation-state scrutiny**: Accepted PUT state is decoded only after validating its handle. Initial and final pre-dispatch expiry preserve known remote state; existing diagnostic precedence is retained. Final retrieval now exposes the existing internal wire metadata until classification, without a new public result/exception type. A previous or newer dispatched GET failure still wins. No PUT replay, retry-policy change, unsafe-handle request, or automatic provisioning was introduced.
- **Validation**: Core/cognitive main/test scalastyle and compilation passed. Core serializer suite 10/10; all four CU suites 134/134; offline Python 28/28 including Pipeline and legacy resave; both codegen/publication tasks passed. Black205 unchanged and whitespace clean. Built/published/actually loaded JAR hashes match: core `4890b2366afa1c796f2826358912142bce434d4fa2bfe60c9e65c00f67b15b6a`; cognitive `631d6993b373b267ccdc91c9906ff5cea4de36e08057c0608b1d1653ecb45a4a`.
- **Evidence**: Session files `cu-attempt23-scala-red.log` (four meaningful creation failures; separate test-placement mistake), `cu-attempt23-scala-persistence-red.log` (corrected registration: implicit-ID failure and explicit/column control pass), `cu-attempt23-python-red.log` (two failures and legacy control pass), `cu-attempt23-validation.log`, `cu-attempt23-python-green.log`, `cu-attempt23-black.log`. The test-placement error was corrected and is not counted as production red evidence.
- **Tier/source**: Max/direct and all-clean gate from the human; Grok-low and unavailable-Grok exception retained. Remaining model policy from review-code. Broad snapshot: 36 paths,436020 bytes; seven changed paths since attempt22. High interaction complexity across transport recovery, Spark planning, persistence and codegen.
- **Primary/reviewers**: Primary `gpt-6-astra`. Fresh current task-catalog selections: `gpt-6.1-sol`/max, `grok-4.7`/low (human override), `claude-opus-5.5`/max. Three concurrent independent Execute invocations on the same full source-only snapshot; repeat the broad sweep, not only the fixes.
- **Snapshot/gate**: `cu-stage1-attempt23.patch`, SHA-256 `67f7c6c9ca722c7ba6e465327ac25b1ad2e2880635209e6d85ed7e6b25608271`, HEAD `f3a7963c217677e9b6564cc6736495e0f1bcdce4`. Source frozen through collection. Advance only when every required reviewer is clean. No new timeout/credit/cycle bound, permission bypass or review-scope reduction.
- **Monitoring/limits**: Scheduled monitoring was stopped at the user's request and will not be restarted. User monitors runtimes; completion notifications own result collection. Attempt22 GPT636.293s/65 calls, Grok712.856s/36 calls, Opus1533.529s/65 calls; all independence and provenance audits passed. No commit/push/live CU/Azure CI/ADO edits authorized.