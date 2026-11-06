---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 11
round: 1
timestamp: "2026-11-04T17:34:54Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "095f8b25603f68531c8456d13e197bb18bb19ae03331130390e01679db894ad5"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-11-review-1-claude-opus-5.5.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Submission and poll path.** I read `ContentUnderstandingProtocol.scala`:
  - `exchange`/`exchangeBeforeDeadline` (375-431)
  - `withClient` (433-443): `disableAutomaticRetries().disableRedirectHandling()`, with a fresh client per operation
  - `decode`/`checkSharedFailure`/`decodeSubmission`/`safeSubmission` (459-583)
  - `pollWithClient`/`execute`/`rowHandler` (613-701)

  Findings: no automatic POST replay; ambiguous non-2xx `Operation-Location` responses are not followed; accepted handles bypass the stop; poll mode never stops.
- [x] **DNS bound.** In `ContentUnderstandingDnsResolver.scala`:
  - A fair semaphore with 32 permits is released only in the worker's `finally` after `delegate.resolve` returns (42-49).
  - The executor has 32 threads and a 32-slot queue (84-101).
  - Permits are never released early and stuck workers are never replaced.
  - When capacity is exhausted, `DnsCapacityExceeded` maps to `Rejected`.
- [x] **Per-task circuit state.** `SharedVariable` uses a `@transient lazy val` (core `io/http/SharedVariable.scala` 17-23). `HTTPTransformer.clientHolder` (107-117) is created inside the per-task deserialized closure, so `rowHandler`'s `stopped` flag is task-local. `ContentUnderstandingConcurrencySuite` "shared failure circuit state is isolated per partition and action" (127-145) confirms this.
- [x] **Credentials and headers.**
  - `ServiceAuthHeaders.addContentType` (CognitiveServiceBase.scala 358-361) removes Content-Type headers case-insensitively.
  - Auth fingerprints are SHA-256 hashes (protocol 496-500).
  - `FailureClassifier.isSharedAuthFailure` requires the scalar credential to have been sent and no row credential to have been selected.
  - The row-auth marker is re-added only when `rowUsesColumnAuth` holds.
- [x] **Row validation.** `validateValue`, `validateRow` and `rowError` (ContentUnderstanding.scala ~174-283):
  - Null map values fail the type check and become `InvalidInput`.
  - `parseUri` wraps `URISyntaxException`.
  - Invalid rows are nulled before the HTTP stage in `buildInternalTransformer` (352-395), which uses DataFrame/UDF only: no RDDs and no `SQLConf` mutation.
  - `concurrentTimeout` is rejected (106-108).
- [x] **Python transfer.** I read:
  - the shared `ComplexParamsMixin` (core `schema/Utils.py` 61-131)
  - the generated ServiceParam setter (Wrappable.scala 182-213) and the `isSet` override (82-88)
  - the CU overrides (ContentUnderstandingParams.scala 208-244)
  - Python tests (test_ContentUnderstanding.py 255-279)
- [x] **Docs and tests against runtime behavior.**
  - Docs: `docs/Explore Algorithms/AI Services/Content Understanding.md` 192-211 and 255-281.
  - Tests: `ContentUnderstandingRecoverySuite.scala` 528-562 and 598-700.
  - `pipeline.yaml` 821-822 has the approved `contentunderstanding` entry.
- [ ] **No builds or tests run.** The review was read-only with view/grep/glob only. The validation claims in the plan were not reproduced.
- [ ] **Full patch not visible.** The diff in the prompt was truncated, the frozen patch is in the excluded reviews directory, and git was unavailable. So I couldn't confirm whether `Utils.py` lines 90-101 predate this change. I also didn't inspect the generated Python under `target/`.
- [ ] **No live CU service or external CI evidence.** Both are out of scope for this review.

## Issues

### Issue 1: A DNS deadline expiry before sending is reported as possibly accepted (`Unknown`)
- **Severity**: Medium
- **File**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingDnsResolver.scala`; `.../ContentUnderstandingProtocol.scala`; `docs/Explore Algorithms/AI Services/Content Understanding.md`
- **Line(s)**: DnsResolver 13-14, 31-33, 56-63, 80-82; Protocol 380-381, 412-415, 466-467, 543-567; docs 255-271
- **Description**:
  - **Where the exception comes from.** `BudgetedDnsResolver` throws `OperationDeadlineExceeded` in three cases: the budget is already zero on entry, `resolution.get(remainingMs())` times out, or the lookup returns exactly at the deadline. `OperationDeadlineExceeded` is an `UnknownHostException` carrying the generic `OperationTimedOut` message.
  - **How it is classified.**
    - `exchangeBeforeDeadline` records it as `transportFailure = true`.
    - `BudgetedDnsResolver.diagnostic` also maps a `DnsResolutionFailed` that coincides with the deadline to `timeoutError`.
    - `dnsFailedBeforeSend` recognizes only `DnsResolutionFailed` and `DnsCapacityExceeded`.
    - As a result, `decodeSubmission` returns `Unknown` with `OperationTimedOut` ("Retain any handle…") and no handle.
  - **Why nothing was sent.** The resolver runs before the socket connects, and each submission uses a fresh no-retry client, so this POST provably never left the JVM.
  - **A realistic trigger.** When DNS is saturated, a caller can spend most of its budget queued in `Capacity.tryAcquire(remaining)`, get a permit, then time out in `resolution.get`. The same root cause then produces `Rejected/DnsCapacityExceeded` for some rows and `Unknown/OperationTimedOut` for others. A slow resolver combined with a short `maxOperationDuration` in submit mode triggers it too.
  - **Test gaps.**
    - The stall is tested only in poll mode (RecoverySuite 598-682).
    - Submit mode is tested only with an immediate `DnsResolutionFailed` (684-700).
    - The concurrency suite asserts `OperationDeadlineExceeded` only at the resolver level.
- **Risk**:
  - This violates the requirement that unsent DNS failures be diagnosable and distinguishable from possibly accepted work.
  - It contradicts the docs, which say `Rejected` includes "a DNS failure before the HTTP request was sent" and tell users to "inspect the outcome" for `Unknown`. With no handle, there is nothing to inspect.
  - Users must either drop documents that were never sent or treat ambiguous rows as safe to resubmit, which risks duplicate billing.
- **Suggested Fix**:
  - Classify every `UnknownHostException` raised by `BudgetedDnsResolver`, and the `remainingMs == 0` pre-check in `exchange`, as unsent. Use a dedicated diagnostic code or a `sent = false` flag on `WireResponse`.
  - Have `decodeSubmission` return `Rejected` for unsent failures. Poll mode should keep its handle and previous state.
  - Optionally, apply the same treatment to connection-establishment failures, which also happen before any bytes are sent.
  - Add submit- and analyze-mode stalled-DNS tests that assert `Rejected`, no handle and zero connections, and align the docs.

### Issue 2: The shared Python ServiceParam transfer sends `None` placeholders and overwrites column bindings on non-CU wrappers
- **Severity**: Medium
- **File**: `core/src/main/python/synapse/ml/core/schema/Utils.py` (with `core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/Wrappable.scala`)
- **Line(s)**: Utils.py 90-101, 109-124; Wrappable.scala 182-213; for comparison, ContentUnderstandingParams.scala 208-244 and test_ContentUnderstanding.py 255-279
- **Description**:
  - **The stored placeholder.** `_transfer_params_from_java` stores `None` in `_paramMap` for any set ServiceParam whose JVM value is `Right(column)` (lines 97-101). PySpark runs this from `JavaParams._from_java`, which covers `Stage.load`, `PipelineModel.load` and model wrapping.
  - **How it reaches the JVM.** The modified `_transfer_params_to_java` passes every ServiceParam entry to the generated Python setter (117-121).
  - **Effect.** That setter calls `self._java_obj.setX(None)` (Wrappable.scala 205). The result is `setScalarParam(p, null)`, which yields `Left(null)`. For setters that don't take a String, Py4J likely raises a method-resolution error instead.
  - **When it runs.** The transfer runs from `JavaTransformer._transform`, `JavaParams.copy` and the writer. So `AnalyzeDocument.load(path).transform(df)` silently loses a saved `setImageUrlCol` or `setSubscriptionKeyCol` binding, and a later save persists or fails on the corrupted value.
  - **Why CU is unaffected.** CU avoids this only through its own overrides, whose comment reads "JVM-backed service params must not become None scalars". The new non-CU regression test covers only a scalar `subscriptionKey`.
- **Risk**:
  - Generated cognitive wrappers with column-bound ServiceParams break after a Python load or copy, including inside saved PipelineModels. Requests go out without the URL or key, every row gets a 401, or Py4J fails.
  - If the code before this change never passed `None` to a JVM setter, this is a regression and should be raised to High. I couldn't confirm the provenance.
- **Suggested Fix**:
  - In the shared mixin, don't store column-bound ServiceParams as `None`: pop them in `_transfer_params_from_java`, as CU does.
  - Skip `None` in the ServiceParam branch of `_transfer_params_to_java`.
  - Add a non-CU test that runs `setXCol` → save → load → `copy`/`transform`-path transfer → save/load, and asserts the column binding is preserved.

### Issue 3: Rows that trigger a non-auth shared failure are marked `Failed`, so the documented retry recipe skips them
- **Severity**: Medium
- **File**: `.../ContentUnderstandingProtocol.scala`; `docs/Explore Algorithms/AI Services/Content Understanding.md`; `.../ContentUnderstandingRecoverySuite.scala`
- **Line(s)**: Protocol 466-471, 528-541, 556-567, 687-697; docs 192-197, 272-281; RecoverySuite 528-536, 547-562
- **Description**:
  - **What the triggering row gets.** When a pre-acceptance shared failure fires, `rowHandler` returns the triggering response and replaces only its error with `SharedConfigurationFailure`.
  - **Inconsistent status.** The status is:
    - `Rejected` for 401/403 (`AdmissionRejections`);
    - `Failed` for the conservatively shared scalar-analyzer 404, a 400 `DeploymentNotFound`/`DefaultsNotSet`, or a 200 response with status `Failed` and `AnalyzerNotFound`.
  - **Concurrency.** With `concurrency > 1`, every in-flight sibling that hits the same failure becomes such a `Failed` row.
  - **The recipe.** The docs say to fix the configuration and then resubmit only rows with `analysis.status = 'Rejected'`. That excludes these `Failed` rows, which had no handle and were never accepted. Their status is also indistinguishable from genuine document failures.
  - **Test gaps.** Tests check only the error text on the triggering row (RecoverySuite 556), not its status.
- **Risk**:
  - Following the documented recovery after a 404 or deployment shared failure permanently drops at least one document per affected partition, up to `concurrency`.
  - The same `SharedConfigurationFailure` gets different statuses depending on its cause.
- **Suggested Fix**:
  - For pre-acceptance triggering rows with a definitive `Failed` reply and no `Operation-Location`, set `status = "Rejected"` and keep `httpStatus` and `rawResponse`. Leave ambiguous `Unknown` rows unchanged.
  - Alternatively, extend the recipe to also select non-`Unknown` rows where `get_json_object(analysis.error,'$.code') = 'SharedConfigurationFailure'`.
  - Add status assertions for the 401 and 404 triggering rows.

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

### Issue 3
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

## Driver follow-up: attempt 11

The original findings and verdict above are preserved. Fixes are implemented; post-fix validation is pending.

- **Issue 1:** Red submit/analyze tests reproduced an unsent DNS deadline classified as `Unknown`; poll behavior passed. An explicit private `WireResponse.notSent` flag now distinguishes unsent budget/DNS failures. Submission decoding returns `Rejected` and a dedicated `RequestNotSent` diagnostic for an unsent deadline. Poll handles and physical DNS worker/permit ownership are preserved.
- **Issue 2:** Corrected the test fixture to inspect actual JVM `Either` column state (AnalyzeDocument has no Python `getImageUrlCol`). The corrected test reproduced a `Right(column)` becoming scalar state after load/copy. Shared `_transfer_params_from_java` now removes column placeholders rather than recording `None`; transfer therefore cannot re-send a fabricated scalar. Explicit caller-supplied values are not broadly skipped. The regression covers load, copy, explicit transfer and resave for AnalyzeDocument URL/key columns.
- **Issue 3:** Preserved the triggering service status rather than normalizing all shared failures to `Rejected`. The documented persisted-result predicate now includes handle-free `Failed` rows whose error code is `SharedConfigurationFailure`, alongside handle-free `Rejected` rows. It excludes `Unknown` and accepted handles. Extended the accepted-sibling/shared-failure test with 401/404 triggering status assertions and the actual SQL retry predicate over materialized rows; it must select the triggering and skipped rows without resubmitting the accepted sibling.
- **Evidence:** `cu-attempt11-python-red2.log` records 2 failures/15 tests, including this column-state failure; `cu-attempt11-scala-red.log` records the DNS failures. Full Black passed. The first post-fix style run identified an overlong test helper, now split; `cu-attempt11-fixes-validation2` is running. Generated Python and fresh-JAR validation remain pending.
- **Coverage qualification:** The original frozen patch was a session artifact, not inside the excluded repository feedback directory. The reviewer reported prompt truncation; the finding evidence is retained, but this run is not counted as comprehensive clean signoff.

### Post-fix validation completed

- `cu-attempt11-fixes-validation2.log`: style/compilation, 7/7 shared HTTP tests and 82/82 CU tests passed, including unsent DNS classifications, triggering 401/404 statuses and the saved-row retry predicate excluding accepted siblings.
- `cu-attempt11-python-fixed.log`: 15/15 offline tests passed, including non-CU loaded column binding preservation through copy/transfer/resave. Actual loaded core/cognitive JAR fingerprints matched the rebuilt artifacts; generated shared Python matched canonical source. Codegen, local publication and full Black also passed.
- Issues 1-3 are locally addressed with evidence. The original reviewer verdict remains dirty and is not rewritten as clean; the next independent attempt is required.