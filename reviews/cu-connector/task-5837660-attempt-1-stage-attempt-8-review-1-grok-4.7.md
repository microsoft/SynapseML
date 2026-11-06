---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 8
round: 1
timestamp: "2026-11-04T07:28:23Z"
outcome: "issues_found"
reviewer_model: "grok-4.7"
input_sha256: "a2dcea9b8cef633d005c1213b91ab3e8de6e749abfef217d788c07ba0f778a03"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-8-review-1-grok-4.7.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Reviewed frozen CU protocol, DNS resolver, header validation, and transformer pipeline in `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala`, `ContentUnderstandingDnsResolver.scala`, and `ContentUnderstanding.scala`.
- [x] Checked shared auth/header changes in `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/CognitiveServiceBase.scala` and null-request handling in `core/src/main/scala/com/microsoft/azure/synapse/ml/io/http/HTTPTransformer.scala`.
- [x] Compared poll, deadline, and shared-failure behavior with `ContentUnderstandingRecoverySuite.scala` and the updated Content Understanding docs.
- [ ] Did not execute tests; offline validation claims were not re-run in this review.

## Issues

### Issue 1: Stalled DNS lookups permanently exhaust the shared resolver
- **Severity**: High
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingDnsResolver.scala
- **Line(s)**: 23-42, 64-75
- **Description**: DNS resolution runs on a process-wide pool of 32 threads with a `SynchronousQueue` and `AbortPolicy`. On budget expiry the future is cancelled, but `InetAddress` resolution does not respond to interruption, so the worker stays occupied until the platform lookup returns. Those threads are never replaced. After 32 hung lookups, later submits fail immediately with "DNS resolution capacity is exhausted."
- **Risk**: One executor JVM can lose all CU DNS capacity for the rest of the process. POSTs then surface as transport/`Unknown` failures. Callers who resubmit can repeat billable work, and unrelated partitions fail even when their own budget has not expired.
- **Suggested Fix**: Do not cap a pool of non-interruptible lookups with abort-on-reject. Abandon timed-out workers and create replacements, or resolve with an API that has a real timeout and does not permanently occupy a shared worker. Bound queueing without turning a few stalls into a process-wide outage.

### Issue 2: Budget expiry overwrites an already actionable poll error
- **Severity**: Medium
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala
- **Line(s)**: 611-614
- **Description**: Before sleeping for the next poll, if the remaining budget cannot hold `delay`, the loop returns the previous response with `error` replaced by `OperationTimedOut`. That replacement is unconditional. A retained `Running` handle whose error is already `ResponseTooLarge`, `InvalidResponse`, or a service poll error loses that diagnostic when the next delay simply does not fit.
- **Risk**: Callers resume or raise limits based on `error`. Replacing `ResponseTooLarge` with `OperationTimedOut` hides the reason the body was unusable and can lead to repeated polls of a response that will never fit the current cap.
- **Suggested Fix**: Keep an existing error when skipping the next poll. Set `OperationTimedOut` only if the retained response has no error. Add a case where a size-limit poll is followed by a `Retry-After` that exceeds the remaining budget and assert the size-limit code survives.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 2
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

## Driver Decision

Tier: Max
Source: Human request to continue the Max review-fix loop until clean.
Change size: Broad cross-module CU connector work spanning Scala implementation and tests, Python tests, docs, shared code generation/schema helpers, and pipeline selection (25 diff sections; frozen patch 247,781 bytes).
Cyclomatic complexity: High; request, polling, deadline, DNS, authentication, analyzer provisioning/retrieval, persistence/codegen, and failure paths interact, with concurrency and recovery cases.
Rationale: Max is explicitly requested and justified by broad change surface and interacting asynchronous/network failure paths.
Primary model: Copilot CLI configured identity not relied on for review assignment; reviewers are explicit.
Reviewers: gpt-6.1-sol (latest available GPT flagship in current task catalog), grok-4.7 (latest Grok frontier), claude-opus-5.5 (latest Opus). Pairwise distinct.
Reasoning: xhigh for GPT and Opus; low for Grok per the user's prior explicit lower-reasoning preference.
Override source: Human instruction: continue review-fix loop until clean; use Grok 4.7 with lower reasoning; if Grok is not connecting, consider review clean when GPT and Opus are clean. The Grok availability exception applies only to a demonstrated connection failure; a Grok finding still blocks advancement.
Branch: work/cu-integration-2698 targeting master; live master baseline is Spark 3.5.0 / Scala 2.12.17 (build.sbt:31,34).
Frozen input: C:\Users\boraelkin\.copilot\session-state\653e4257-0554-408f-9e0e-b23750249edb\files\cu-max-review-diff-20261104-stage1-attempt8.patch
Input SHA-256: 34b924b432dccae7c0fc607581440338281c1ac4efecbceaf6b49d8915032654
Input includes tracked source changes plus the three untracked Scala source files, excludes reviews/cu-connector artifacts.
