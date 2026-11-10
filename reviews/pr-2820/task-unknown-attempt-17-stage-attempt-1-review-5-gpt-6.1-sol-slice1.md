---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 17
stage_attempt: 1
round: 5
timestamp: "2026-11-09T12:10:36Z"
outcome: "issues_found"
reviewer_model: "gpt-6.1-sol"
input_sha256: "f846adee90044efa80212e61b4b0df62ba344cb4e5521f79b1994a5e3b42e953"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: gpt-6.1-sol
- **Artifact**: reviews/pr-2820/task-unknown-attempt-17-stage-attempt-1-review-5-gpt-6.1-sol-slice1.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read all 667 lines of the assigned frozen diff.
- [x] Compared interruption and timeout classification in `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala` with every scenario in `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/AttachmentReadFailureFileSystem.scala`.
- [x] Reviewed the complete `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptMultimodalRequestSuite.scala`, including sanitization, restored interrupt flags, fatal-error identity, Spark cancellation, and both API variants.
- [x] Inspected the UDF exception contract in `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala` and the replaced resource-management implementation.
- [ ] Tests were not executed under this read-only review. The caller reports 89 passing tests with zero skips, compilation, and both scalastyle gates passing; those results were not independently verified.

## Issues

### Issue 1: Cycle tests do not exercise the timeout classifier’s cycle guard
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala
- **Line(s)**: 56–67
- **Description**: The added cyclic tests exercise `isInterruptedRead`, but not the visited-node exit in `isTimeoutFailure`. `cyclicExceptionGraph` uses ordinary `IOException` nodes and appends an acyclic `InterruptedIOException` leaf. The separate `timeout-cause-cycle.png` scenario reaches a `SocketTimeoutException`, so timeout recognition short-circuits before following its cyclic causes.
- **Risk**: A regression in this separate cycle guard could make classification loop indefinitely for a plain `InterruptedIOException` with cyclic, non-timeout causes without being detected by the existing cyclic cases.
- **Suggested Fix**: Add a focused `readAttachment` test with a cause cycle such as `InterruptedIOException → IOException → original InterruptedIOException`, containing no typed timeout. Verify bounded completion, the sanitized `InterruptedException`, and restoration of the thread’s interrupt flag.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Addressed
- **What changed**: Both existing cyclic `readAttachment` cases now include an `InterruptedIOException -> IOException -> original InterruptedIOException` cause cycle with no typed timeout. The fixture bounds cause accesses so a broken guard fails deterministically instead of hanging.
- **Why**: The assertions now reach the separate timeout-classifier visited-node exit while still checking the sanitized `InterruptedException`, restored interrupt flag, and cleanup of that flag.
- **How verified**: With only the timeout-classifier cycle guard temporarily removed, `cognitive/testOnly com.microsoft.azure.synapse.ml.services.openai.OpenAIPromptMultimodalRequestSuite -- -z "attachment reads detect interruption in cyclic exception graphs"` failed both cases as expected with the fixture's `AssertionError` (2 failed, 0 errors/skips; no hang). After restoring the production file byte-for-byte, `cognitive/compile`, `cognitive/Test/compile`, `cognitive/testOnly com.microsoft.azure.synapse.ml.services.openai.OpenAIPromptMultimodalRequestSuite`, `cognitive/scalastyle`, and `cognitive/Test/scalastyle` passed: all 89 cases, 0 failures/errors/skips, and 0 style errors/warnings. Tests used `set cognitive / Test / fork := false`. Only two test files remain changed; no production change is included.