---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 1
round: 5
timestamp: "2026-11-05T20:38:24Z"
outcome: "issues_found"
reviewer_model: "gpt-6.1-sol"
input_sha256: "234f44b745652cff50cc274b2a09f3d7d22c5b5116daf91ebededf8178b3cc1e"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: gpt-6.1-sol
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-1-review-5-gpt-6.1-sol.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Reviewed the supplied contribution and six CU Scala suites under `cognitive\src\test\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding`, including HTTP fixtures, deadlines, admission classification, parsing bounds, persistence, and schema validation.
- [x] Compared credential-provenance assertions in `ContentUnderstandingRecoverySuite.scala:457–468` with `ContentUnderstandingProtocol.scala:488–515`.
- [x] Compared concurrent integration coverage in `ContentUnderstandingConcurrencySuite.scala:27–42` and `ContentUnderstandingRecoverySuite.scala:557–581` with the partition handler and canonical `core\src\main\scala\com\microsoft\azure\synapse\ml\io\http\Clients.scala`; inspected `VerifyClients.scala` context-association coverage.
- [x] Inspected generated-wrapper lifecycle tests in `cognitive\src\test\python\synapsemltest\services\test_ContentUnderstanding.py`, shared Python parameter handling in `core\src\main\python\synapse\ml\core\schema\Utils.py`, and serializer/codegen regression tests.
- [x] Inspected live-suite credential skipping, documented test selection, and the approved CU package selector in `pipeline.yaml:818–823`.
- [ ] No tests or commands executed. Reported Scala/Python successes are caller-supplied evidence, not independently reproduced; live service and external CI remain unverified.

## Issues

### Issue 1: Equal-valued credential provenance test cannot detect partition poisoning
- **Severity**: Medium
- **File**: cognitive\src\test\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingRecoverySuite.scala
- **Line(s)**: 457–468
- **Description**: The test claims to prove that a column credential matching a lower-priority scalar credential remains row-specific, but checks only `Rejected` and one request for one input row. A shared authentication failure also retains `Rejected` and sends exactly one request. Removing the `rowCredentialWasSelected` protection from `isSharedAuthFailure` would therefore still satisfy these assertions.
- **Risk**: A regression that classifies equal-valued column credentials as scalar could silently stop valid subsequent rows while this dedicated regression test remains green.
- **Suggested Fix**: Assert that the first row retains its service diagnostic without `SharedConfigurationFailure`. Add a subsequent row with a different valid column credential, configure concurrency one, and assert that it is submitted and succeeds. Check both POSTs and their effective credentials; cover 401 and 403.

### Issue 2: Shared-stop coverage never exercises an in-flight CU sibling
- **Severity**: Medium
- **File**: cognitive\src\test\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingConcurrencySuite.scala
- **Line(s)**: 27–42
- **Description**: The multi-row concurrency test uses identical operation handles and checks sorted statuses, so it cannot detect response-to-input misassociation. The dedicated shared-failure sibling test in `ContentUnderstandingRecoverySuite.scala:557–581` explicitly uses concurrency one: its accepted sibling finishes before rejection occurs. No public-transformer test combines overlapping CU requests, a definitive shared rejection, and a sibling completing afterward. Generic `VerifyClients` context tests do not exercise this new stop state.
- **Risk**: Cancellation/discarding of already-started siblings, lost handles, or incorrect row association around a shared stop could escape the offline suite.
- **Suggested Fix**: Add a latch-controlled loopback test with concurrency two and distinct document/operation identifiers. Establish that a sibling request is already dispatched, deliver a definitive shared rejection, then release the sibling response. Assert its original row, handle, and raw body survive; later unsent rows are rejected locally; and no accepted request is replayed. Use controlled ordering rather than timing-only sleeps.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Addressed; locally verified.
- **What changed**: Added a two-row public-transformer case in `ContentUnderstandingSubmissionSuite` for both 401 and 403, with a column AAD value equal to the lower-priority scalar custom credential, followed by a different valid column value.
- **Why**: The new assertions distinguish service rejection from shared-stop poisoning: original service code, no SharedConfigurationFailure, successful later row, two POSTs, exact effective credentials and no on-wire provenance marker.
- **How verified**: The new seven-test suite passed in `cu-stage5-coverage-validation-retry2.log`; production credential classification was unchanged.

### Issue 2
- **Status**: Addressed; locally verified.
- **What changed**: Added a concurrency-two public-transformer test with distinct row/document/operation identifiers. The rejection waits until the sibling is dispatched; the sibling response waits until a downstream UDF observes the connector's shared rejection. Later rows remain unsent.
- **Why**: Stub response order alone would not prove connector-side stop ordering. Latches establish overlap and rejection observation without sleeps. Assertions cover row association, sibling id/handle/raw body, later local rejection, exactly two POSTs and no replay.
- **How verified**: The latch-controlled case passed in `cu-stage5-coverage-validation-retry2.log`. Latches are released and the loopback server closes in finally.

## Driver: Stage 5 attempt 1 collection and attempt 2 plan

- All three reviews persisted; input hashes and actual source-only tool arguments were audited. No previous/sibling feedback access. GPT corrected one missing-path lookup; Opus and Grok had no failed calls.
- GPT: two Medium coverage findings. Opus: one Medium and four Low findings. Grok: two Medium and one Low finding; future HTTP-date coverage duplicates Opus's finding.
- Test-only changes add one submission suite because the existing recovery/concurrency/general suites are near the 800-line cap. No production, wrapper, pipeline or dependency changes.
- Main/test style and compilation passed across the retained validation runs. `cu-stage5-coverage-validation-retry2.log` passes seven new submission tests and the strengthened existing deep-error test. The initial naming and Dataset helper compilation failures remain in the prior logs; both were corrected without weakening checks.
- The prior 144-test six-suite run remains evidence for the unchanged production source. This is not a claim of a new combined 151-test run. Python 29/29 and publication hashes remain qualified as snapshot25 evidence.
- Opus's exact-depth-1000 acceptance suggestion and timing-bound relaxation remain unimplemented: stack-overflow fallback is intentional, and deadline assertions were not loosened without evidence. See its resolution notes.
- Attempt 2: same human High/direct policy, `gpt-6.1-sol/high`, `grok-4.7/low`, `claude-opus-5.5/high`, primary `gpt-6-astra`, resolved from the current task catalog. No third attempt; retain any unresolved second-attempt findings. No new bounds or permissions.
- Snapshot `cu-stage5-attempt2.patch`: 37 paths, 463746 bytes, SHA256 `c482545bef4965c427a0b9c897002a0695ab97bd385056b92800039ca426adae`. Broad contribution and substantial state/concurrency/lifecycle complexity justify the review breadth; the explicit human tier/reasoning override remains controlling. Source frozen; reviewers receive contract and qualified validation, not feedback.
- Earlier eight architecture recommendations and the stage-4 creation-vocabulary recommendation remain unresolved. No commit, push, ADO edit, live CU call or target-integration/CI claim.