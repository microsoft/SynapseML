---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 18
stage_attempt: 1
round: 1
timestamp: "2026-11-09T12:55:48Z"
outcome: "issues_found"
reviewer_model: "grok-4.7"
input_sha256: "6585872ac45b29f73bbd640ca22f51240eec17400e3eb0d0662ec0d07898658a"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: reviews/pr-2820/task-unknown-attempt-18-stage-attempt-1-review-1-grok-4.7-slice1.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read the assigned frozen diff in full across the required ranges and compared it with `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala`.
- [x] Checked interruption, timeout, cancellation, and fatal classification against `AttachmentReadFailureFileSystem.scala` and `OpenAIPromptMultimodalRequestSuite.scala`. Timeout-owned causes stay row-local; suppressed cleanup interruptions and cleared thread flags propagate.
- [x] Confirmed the UDF contract in `OpenAIPrompt.scala` (`createMessagesUDF`, lines 484–498): `Try` does not catch `InterruptedException`, rethrows `TaskKilledException`, and stores other failures as sanitized row errors.
- [x] Compared the new `Using.resource` read (`OpenAIAttachmentUtils.scala` lines 316–318) with `BinaryFileReader.readSingleFileBytes` and `StreamUtilities.using` (lines 27–35). The fresh Hadoop `Configuration` matches the previous reader and is not a regression.
- [ ] Did not execute tests. Caller-supplied compile, style, and suite results were treated as evidence only.

## Issues

### Issue 1: Fatal close failures are discarded when the read also fails
- **Severity**: Medium
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala
- **Line(s)**: 70–88, 96–104, 316–318
- **Description**: `Using.resource` keeps the body failure as the primary exception and only suppresses a failure from `close()`. `readAttachment` then replaces that primary exception. `interruptedRead()` and `new AttachmentReadException` do not copy suppressed throwables. A fatal close error (`LinkageError`, `VirtualMachineError`, including `OutOfMemoryError`) is therefore dropped. This happens both when close’s fatal is suppressed on an ordinary read failure and when it is suppressed on an interruption, because a primary `InterruptedException` returns at `isInterruptedRead` line 71 without inspecting suppressed exceptions. The previous `StreamUtilities.using` `finally` close let a fatal from `close()` escape unchanged. The new fatal test only throws a primary `LinkageError` from the by-name body, so it does not cover this cleanup path.
- **Risk**: A broken or out-of-memory stream close becomes `Unable to read attachment '...'`. `createMessagesUDF` (`OpenAIPrompt.scala` lines 496–497) records that as a row-local error and the job continues. That violates the required fatal-error behavior and can hide an executor-level failure behind a successful stage.
- **Suggested Fix**: Before replacing any caught failure, walk its cause and suppressed graph with the existing cycle guard and rethrow the first fatal that is not an `InterruptedException` you intentionally sanitize. Prefer a fatal thrown by `close()` over the body failure, while still attaching the body failure as suppressed. Add a read-failure plus `close()`-throws-`LinkageError` case and assert that the fatal escapes `readAttachment` and is not stored as an error row.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Rejected
- **What changed**: No shipped source changed. The original reviewer feedback and verdict above are preserved.
- **Why**: The premise about Scala `Using.resource` is incorrect: fatal cleanup failures take priority over both ordinary IO failures and interruption. The actual `prepareFile` path rethrows the exact fatal close object with the original read error suppressed, rather than converting it into a row-local error or sanitized interruption.
- **How verified**: An isolated probe compiled against the already-qualified production classes injected a synthetic Hadoop filesystem through the same shared cache used by the existing suite. All four combinations of `IOException`/`InterruptedException` during read and `LinkageError`/synthetic `OutOfMemoryError` during close passed exact-object identity, suppressed-body identity, and exactly-once read/close assertions. The native-JDK compile and probe commands passed; production sources and class bytes remained unchanged. No real memory exhaustion or live storage was attempted.