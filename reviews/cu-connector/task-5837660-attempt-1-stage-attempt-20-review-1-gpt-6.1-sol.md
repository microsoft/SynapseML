---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 20
round: 1
timestamp: "2026-11-05T06:51:48Z"
outcome: "issues_found"
reviewer_model: "gpt-6.1-sol"
input_sha256: "6712b9b2b198b9b2e02f6379cef576243a56e71685cd122e81e402b6defdba27"
---

---
producer: review-code
schema_version: 1
tier: max
contract: direct
execution_backend: copilot_cli
attempt: 1
stage_attempt: 20
round: 1
timestamp: "2026-11-05T06:44:37.273Z"
outcome: issues_found
reviewer_model: gpt-6.1-sol
input_sha256: 808165b4cee4533c3a477f8d159086e1ddbe08434253b478297d8cd45c2be005
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: gpt-6.1-sol
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-20-review-1-gpt-6.1-sol.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Reviewed all supplied changes, including connector implementation, shared HTTP/schema/codegen changes, tests, documentation, and the pipeline matrix entry.
- [x] Traced submission classification, partition stops, polling recovery, DNS capacity ownership, and parsing limits through `ContentUnderstandingProtocol.scala` and its supporting helpers.
- [x] Inspected Python scalar/column parameter handling, binary conversion, persistence, and default-column lifecycle in `Utils.py`, `Wrappable.scala`, `ContentUnderstandingParams.scala`, and `test_ContentUnderstanding.py`.
- [x] Compared retry-budget control flow with `ContentUnderstandingRecoverySuite.scala:147-160`, `ContentUnderstandingConcurrencySuite.scala:191-218`, and the documented diagnostic-precedence requirement.
- [ ] Runtime reproduction was not performed: this review permits read-only inspection only. Supplied validation results were considered caller-provided evidence, not independently executed.

## Issues

### Issue 1: Budget expiration after a retry wait overwrites the last GET error
- **Severity**: Medium
- **File**: `cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala`
- **Line(s)**: 609-615, 705-714
- **Description**: Both retry loops check the budget before `pause(delay)` but proceed afterward without preserving the previous error if the budget has expired. For example, after a GET returns HTTP 503 with `Unavailable`, a delay can fit when checked but exhaust the budget during sleep or scheduling. `exchange` then returns its synthetic, unsent `OperationTimedOut` response at lines 368-369. Polling overwrites the previous error/status at line 588; management retrieval discards the previous response entirely. This also affects the final analyzer GET after successful creation. Existing budget tests exercise delays already exceeding the budget, not expiration after an allowed wait.
- **Risk**: The actionable last GET diagnostic and HTTP status—and management response body—are replaced by a generic timeout despite no subsequent GET being sent, violating the acceptance contract.
- **Suggested Fix**: Preserve the last response when the next exchange expires locally before dispatch. Carry the last wire response through management retries and distinguish a synthetic unsent timeout from a newer HTTP failure. Add deterministic post-wait expiration coverage for polling, management retrieval, and final retrieval after creation.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending.
- **Why**: Budget exhaustion before another GET must not replace the last real GET error.
- **How verified**: Static control-flow inspection only; no fix or runtime regression verified.

### Driver resolution (2026-11-05)
- **Status**: Fixed and locally validated; independent re-review pending. Original finding and verdict are preserved.
- **What changed**: `WireResponse.expiredBeforeDispatch` records entry/DNS/pre-write deadline expiration separately from a dispatched request's failure. Expired unsent work is not retried. Polling keeps an existing useful diagnostic/status; management recursion keeps the prior wire response, including raw body. Newer dispatched HTTP failures continue to win, and an operation with no previous error still reports timeout.
- **Testability**: Extracted the existing clock/sleep behavior into package-private `ContentUnderstandingBudget`, with per-call injection on internal protocol entrypoints only. No public transformer parameter, global test hook, timeout relaxation or safety bypass. Production interrupt handling is unchanged. The extraction also keeps Protocol within the 800-line limit.
- **Red evidence**: After correcting a low-level fixture's endpoint normalization, the deterministic test failed in all four routes: poll, analyze-and-poll, direct management GET and final GET after creation. The no-prior-error timeout case passed before the fix. `cu-attempt21-scala-red2.log` is the meaningful red run; the earlier fixture failure is not counted.
- **Green coverage**: Matrix tests cover503,429,disconnected transport versus post-wait expiration/newer403; retain original error/status/raw/handle and exact request counts. Additional tests cover expiry during DNS before dispatch and newer HTTP errors arriving at budget expiry. All new regressions passed in the full120-test run.
- **Complete validation accounting**: Full run119/120 passed. The only failure was an existing cleanup test reflecting the removed nested Budget class. Updated its constructor/type reference without changing assertions, and its targeted rerun passed1/1. No production change after the full run. Main/test style, compilation, codegen/publication, Black205 unchanged and whitespace passed; Python25/25 passed against rebuilt, published and actually loaded JAR fingerprints.
- **Artifacts**: `cu-attempt21-validation3.log` (full run), `cu-attempt21-validation4.log` (corrected cleanup test/publication), `cu-attempt21-python-green.log`. Cognitive SHA256 `af65240cba8acecb2a100ee8c118a9bb8354753a6140a12767a06d0edc73e2da`; core SHA256 `8bd5e1e05d73a81831a6795d00597110a0f8133905295702e78a9f1d0054a05e`. Style failures were resolved by consolidating deadline checks and using a test data table, without suppression.

### Driver decision: Stage 1, attempt 21
- **Tier/sources**: Max/direct and all-clean advancement from driving-human instruction. Grok low reasoning/unavailability exception remain human overrides; other exact slots follow review-code live-catalog policy. Primary `gpt-6-astra`; freshly resolved current catalog reviewers `gpt-6.1-sol`/max, `grok-4.7`/low, `claude-opus-5.5`/max. Three independent concurrent Execute invocations.
- **Size/complexity**: Broad34 paths,406727 bytes; high interaction complexity across HTTP recovery/concurrency, Spark planning, persistence and code generation. Preserve broad-sweep scope and all required reviewer signoffs. No new timeout/credit/cycle bound or permission bypass.
- **Snapshot**: `cu-stage1-attempt21.patch`, SHA256 `0da14820c09316126cf836e1408db6943cdcfe606f9f0bdafea0d63147c54f72`, HEAD `f3a7963c217677e9b6564cc6736495e0f1bcdce4`. Source frozen through collection; reviewer feedback excluded and forbidden.
- **Delta accounting**: Five paths changed since20: new budget helper, protocol, admission regression suite, existing cleanup regression and CU docs. No dependency, pipeline, public wrapper or unrelated changes.
- **Prior stage result**: Attempt20 Grok and Opus clean; GPT finding fixed above. Read-only independence audits passed: GPT64 calls, Grok24, Opus67. Last Opus completed1033.167s, without runtime outlier.
- **Runtime monitoring**: Existing schedule1 every ten minutes remains active, with public progress/metadata checks and no automatic cancellation. Still stage1 of6; no commit/push/live CU/Azure CI/ADO operation or merge-ready claim.