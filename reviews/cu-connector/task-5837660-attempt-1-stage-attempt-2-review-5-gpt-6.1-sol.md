---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 2
round: 5
timestamp: "2026-11-05T20:58:56Z"
outcome: "issues_found"
reviewer_model: "gpt-6.1-sol"
input_sha256: "884cf6898b0099f7842e02343e319971618ee3a4ac179471e7410ba6f9fde635"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: gpt-6.1-sol
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-2-review-5-gpt-6.1-sol.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Reviewed the supplied contribution against the CU Scala suites, including `ContentUnderstandingSubmissionSuite.scala` acceptance, credential-provenance, concurrent-sibling, cancellation, and Retry-After cases.
- [x] Compared deadline, DNS, parsing, and polling implementations with `ContentUnderstandingAdmissionSuite.scala`, `ContentUnderstandingConcurrencySuite.scala`, and `ContentUnderstandingRecoverySuite.scala`.
- [x] Inspected `test_ContentUnderstanding.py`, `Utils.py`, `Wrappable.scala`, and serializer tests for pending values, column bindings, binary safety, defaults, and persistence.
- [x] Inspected `ContentUnderstandingStub.scala` and canonical HTTP dependencies to assess what transport failures the fixtures actually exercise.
- [x] Independently checked `SimpleHTTPTransformerSuite.scala` and scoped HTTP-test searches for coverage of the shared temporary/error-column correction.
- [x] Assessed caller-supplied validation separately: the reported 144-test baseline and seven new tests plus one strengthened test are not a newly executed combined suite; earlier Python evidence belongs to its stated snapshot.
- [ ] Tests and validation logs were not independently executed or opened in this read-only invocation. Live-service and current-target integration remain unestablished.

## Issues

### Issue 1: Partial-write classification has no regression test
- **Severity**: Medium
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingTransport.scala
- **Line(s)**: 14–20
- **Description**: The safety distinction depends on setting the write-start marker *before* `super.doSendRequest` completes. Existing disconnect tests do not verify that ordering: `ContentUnderstandingStub.scala:101–116` consumes the entire request body before disconnecting. Connection-refusal and TLS tests cover failures before HTTP writing. None exercises an exception during request-header or entity writing. Moving the marker after the superclass call could therefore preserve existing test results while violating the explicit partial-write contract.
- **Risk**: A regression could classify a partially transmitted submission as definitely unsent (`Rejected`/`RequestNotSent`), encouraging unsafe resubmission of potentially accepted work.
- **Suggested Fix**: Add a deterministic failure during HTTP request writing, using a controlled mid-upload reset or a connection double executing the real request executor. Verify the resulting submission is `Unknown`, not `Rejected`/`RequestNotSent`, and is not replayed. The test should fail if the marker is moved after `super.doSendRequest`.

### Issue 2: Shared error-column reservation correction is not exercised
- **Severity**: Low
- **File**: core\src\main\scala\com\microsoft\azure\synapse\ml\io\http\SimpleHTTPTransformer.scala
- **Line(s)**: 115–121
- **Description**: Adding `getErrorCol` to reserved temporary names fixes a distinct shared-transformer collision, but no inspected test configures the error column as `parsedInput` or `unparsedOutput`. CU resolver tests preserve similarly named *input* columns; CU itself supplies uniquely generated internal error names. Those tests would not detect removal of the new error-column reservation.
- **Risk**: This compatibility correction can regress unnoticed, causing valid error-column settings to collide with intermediate request/response columns, produce schema errors, or lose the requested error column.
- **Suggested Fix**: Add focused cases to `SimpleHTTPTransformerSuite.scala` using `setErrorCol("parsedInput")` and `setErrorCol("unparsedOutput")`, including case variants under case-insensitive resolution. Materialize results and verify output, error schema/value, and preservation of the requested columns.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Unresolved coverage recommendation.
- **What changed**: No further source change at the second-attempt gate.
- **Why**: A deterministic mid-write regression is useful, but the current production marker is already set before `super.doSendRequest`. The finding establishes missing coverage, not an observed misclassification. Preserve it explicitly rather than claim it fixed or start a third coverage attempt.
- **How verified**: Driver confirmed the ordering in `ContentUnderstandingTransport.scala:14-20` and that the loopback stub consumes the full body before disconnecting. No mid-write experiment was run.

### Issue 2
- **Status**: Unresolved coverage recommendation; duplicate of Opus issue 3.
- **What changed**: No new core test in this attempt.
- **Why**: A direct shared-transformer regression would pin the error-column reservation better than CU-only tests.
- **How verified**: Driver inspected `SimpleHTTPTransformer.scala:115-121` and the actual test file `core/src/test/scala/com/microsoft/azure/synapse/ml/io/split1/SimpleHTTPTransformerSuite.scala`; the configured error-column collision case is absent there. No production regression was demonstrated.

## Driver: Stage 5 final outcome and stage 6 plan

- **Advanced-with-unresolved**, not clean. Two valid High coverage attempts are complete; no third coverage attempt.
- Attempt 2: GPT two findings (112.391s), Opus five findings (138.831s), independent Grok retry clean (229.517s). GPT/Opus input hashes matched and source-only tool arguments were audited. The Grok retry likewise exposed no feedback; its malformed/missing-path and out-of-range lookups were ordinary recovered tool errors.
- The original Grok clean report remains retained but uncounted because its broad search returned prior review filenames. The valid replacement is under `stage5-attempt2-grok-retry`.
- Six distinct second-attempt coverage topics remain: partial-write classification, shared HTTP column planning, deterministic deep-decode/fallback cases, Retry-After 20-digit/Unicode cases, dispatched-timeout diagnostics, and mixed scalar-analyzer/column-model deployments. Keep prior exact-depth and timing-relaxation suggestions visible as well.
- The timeout recommendation is qualified: current dispatched timeout returns OperationTimedOut, while definitely unsent requests use RequestNotSent. A test must reflect that contract rather than assume OperationTimedOut means unsent or silently require different production wording.
- Previously added seven tests and the strengthened deep-error test passed; no executable edits followed. Earlier eight architecture recommendations and the creation-vocabulary recommendation remain unresolved.
- Start stage 6 (polish and hardening), attempt 1, using unchanged snapshot `cu-stage5-attempt2.patch`, SHA256 `c482545bef4965c427a0b9c897002a0695ab97bd385056b92800039ca426adae`, 37 paths/463746 bytes. Source is frozen during collection.
- Continue human-selected High/direct, GPT/Opus high reasoning and Grok low; current-catalog IDs `gpt-6.1-sol`, `claude-opus-5.5`, `grok-4.7`; primary `gpt-6-astra`. Broad change and state/concurrency/lifecycle complexity justify the three reviewers. No new bounds or permissions.
- Each independent invocation gets the same scope, contract and qualified validation, plus explicit source-search boundaries, never prior/sibling findings. No commit, push, ADO edit, live CU, target integration or CI claim.