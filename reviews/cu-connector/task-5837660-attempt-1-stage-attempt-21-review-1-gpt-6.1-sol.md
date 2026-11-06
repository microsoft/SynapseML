---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 21
round: 1
timestamp: "2026-11-05T07:44:21Z"
outcome: "issues_found"
reviewer_model: "gpt-6.1-sol"
input_sha256: "9564e6eaf04441cae024dc941133d89dadc10bedbaa17abd11dd0acf147368c0"
---

---
producer: review-code
schema_version: 1
tier: max
contract: direct
execution_backend: copilot-cli
attempt: 1
stage_attempt: 21
round: 1
timestamp: "2026-11-05T07:33:54.231Z"
outcome: issues_found
reviewer_model: gpt-6.1-sol
input_sha256: 0da14820c09316126cf836e1408db6943cdcfe606f9f0bdafea0d63147c54f72
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: gpt-6.1-sol
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-21-review-1-gpt-6.1-sol.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Reviewed the complete supplied 34-path diff, including removed writer APIs, replacement recovery examples, documentation, and the CI selector.
- [x] Traced submission, polling, management, authentication, handle validation, response bounds, and deadline handling through `ContentUnderstanding.scala`, `ContentUnderstandingProtocol.scala`, and their local helpers.
- [x] Cross-checked `DatasetExtensions.scala`, `SimpleHTTPTransformer.scala`, `Wrappable.scala`, and `Utils.py` against the supplied Scala/Python tests for session resolution, persistence, defaults, bytes, maps, and column bindings.
- [x] Inspected `ContentUnderstandingAdmissionSuite.scala:129-155` and the DNS-capacity regression in `ContentUnderstandingConcurrencySuite.scala`; existing coverage does not combine a preceding GET failure with capacity-wait expiration.
- [x] Reviewed credential persistence/redaction warnings and opt-in live-test behavior against the acceptance contract.
- [ ] No commands, tests, or live requests executed; supplied validation evidence was considered but not independently reproduced.

## Issues

### Issue 1: DNS-capacity expiration overwrites the preceding GET failure
- **Severity**: Medium
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala
- **Line(s)**: 391-395
- **Description**: The `UnknownHostException` handler marks expiration before dispatch only for `OperationDeadlineExceeded`. However, `BudgetedDnsResolver.resolve` throws `DnsCapacityExceeded` when waiting for a worker consumes the remaining operation budget (`ContentUnderstandingDnsResolver.scala:34-36`). After an earlier GET failure—for example, HTTP 503 with `Connection: close`—a retry can exhaust its budget waiting for occupied DNS workers without dispatching another GET. Because `expiredBeforeDispatch` remains false, polling replaces the earlier error/status with `DnsCapacityExceeded`/HTTP 0, and management retrieval also discards the previous response body.
- **Risk**: An unsent, expired retry loses the last useful service diagnostic and management response body, violating the required error-retention behavior across polling and analyzer retrieval.
- **Suggested Fix**: Mark DNS-capacity wait expiration as `expiredBeforeDispatch`, while retaining its diagnostic when no prior response exists. Add regressions covering a preceding GET failure followed by DNS-capacity expiration in polling and management retrieval, including final retrieval after creation.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending.
- **Why**: DNS-capacity wait expiration currently bypasses the previous-response retention branch.
- **How verified**: Static control-flow inspection; no tests executed.

### Driver resolution - attempt 22
- **Status**: Fixed locally; independent re-review pending. Original finding and verdict retained.
- **What/why**: `ContentUnderstandingDnsResolver.scala` now distinguishes an expired semaphore-capacity wait from immediate executor rejection using cause metadata. `ContentUnderstandingProtocol.scala` classifies the expired wait as an unsent deadline, preserving the last GET error/status/body. A first capacity failure still reports `DnsCapacityExceeded`; genuine DNS failures and immediate rejection remain new diagnostics even when the clock has expired.
- **Regression evidence**: `ContentUnderstandingAdmissionSuite.scala` occupies all 32 DNS workers after the preceding response and tests polling, analyze-and-poll, direct analyzer GET, and final GET after creation. Assertions include the retained error/status/body/handle and exact requests. First-failure and scripted genuine-DNS/immediate-rejection controls prevent overclassification. Together with clean-state/connect regressions, the pre-fix run had seven meaningful failures (11/18 passed); the fixed full CU run passed 127/127.
- **Validation**: Cognitive main/test scalastyle, compilation, codegen and publication passed. Offline Python passed 25/25 with built, published and actually loaded JAR fingerprints matching: core `8bd5e1e05d73a81831a6795d00597110a0f8133905295702e78a9f1d0054a05e`; cognitive `c31e340a3e04a8a1fb39b7e93873bf744aeb7625f73658f9d101cad1e7b0bdf6`. Black: 205 unchanged; whitespace clean. Evidence: session files `cu-attempt22-scala-red.log`, `cu-attempt22-validation.log`, `cu-attempt22-python-green.log`, `cu-attempt22-black.log`. An initial Python shell launch failed before tests; the explicit-environment invocation succeeded.

## Driver decision - stage 1 attempt 22
- **Tier/source**: Max/direct, explicit human tier and all-clean gate. Human Grok-low override retained; unavailable-Grok exception applies only when GPT and Opus clear. Model classes and remaining fields follow review-code policy.
- **Change size/complexity**: Broad source-only snapshot, 34 paths and 413857 bytes; high interaction complexity across HTTP recovery/concurrency, Spark planning, persistence and Python codegen. Exactly four paths changed since attempt 21: DNS resolver, protocol, admission tests and CU documentation.
- **Primary/reviewers**: Primary `gpt-6-astra`. Fresh current task-catalog resolution: GPT flagship `gpt-6.1-sol` / max, Grok frontier `grok-4.7` / low (human override), newest Opus `claude-opus-5.5` / max. Three independent concurrent script-owned Execute invocations; no lower-scope substitute.
- **Rationale/gate**: Both confirmed findings have targeted red evidence and green validation. Repeat the full stage-1 broad sweep; advance only when every required reviewer is independently clean. No new timeout/credit/cycle limit or permission bypass.
- **Snapshot**: `cu-stage1-attempt22.patch`, SHA-256 `89c7ee46006dcdf0bf8f804eed43db4f55cc34dcb738731dee17aa7e36a4a237`, HEAD `f3a7963c217677e9b6564cc6736495e0f1bcdce4`. Source frozen through collection; known generated feedback excluded. Reviewers must not read prior/sibling feedback.
- **Runtime monitoring**: Schedule 1 retained. Attempt 21 GPT: 634.310 seconds / 62 tool calls; Grok: 313.024 seconds / 27 calls; Opus: 1535.264 seconds / 63 calls. All transport/independence audits passed; no attempt-17-style non-convergence. Review meaningful progress, not liveness alone; ask before cancellation.
- **Execution gap**: Scala validation completed at 08:18 UTC, but the driver did not resume Python verification/dispatch until the user's continuation. Monitoring-only ticks did not advance the loop. This was a driver lapse, not reviewer runtime or an external blocker.
- **Limits**: Local review only. No commit, push, live CU, external Azure CI or ADO edits authorized. Target master remains established context; fresh remote verification unavailable.