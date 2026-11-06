---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 7
round: 1
timestamp: "2026-11-04T06:16:41Z"
outcome: "issues_found"
reviewer_model: "gpt-6.1-sol"
input_sha256: "1c75282f013590f0ce6593e5e5f5f96c83ffee92e2cfbde8095146b276313b93"
---

---
producer: review-code
schema_version: 1
tier: max
contract: direct
execution_backend: copilot-cli
attempt: 1
stage_attempt: 7
round: 1
timestamp: "2026-11-04T06:07:00.943Z"
outcome: issues_found
reviewer_model: gpt-6.1-sol
input_sha256: "898F82344C5A105D031F525A7C589A804F3E2D3C31EEE1A52D5DE25D341080CD"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: gpt-6.1-sol
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-7-review-1-gpt-6.1-sol.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Reviewed the supplied embedded frozen diff and inspected the corresponding connector implementations in `cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding`.
- [x] Traced authentication resolution and internal-marker handling through `cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\CognitiveServiceBase.scala` and `core\src\main\scala\com\microsoft\azure\synapse\ml\io\http\HTTPSchema.scala`.
- [x] Inspected submission, polling, deadline, recovery, provisioning, and persistence cases in the four Content Understanding Scala suites and both supplied Python test files.
- [x] Reviewed shared code-generation and column-resolution changes in `core\src\main\scala\com\microsoft\azure\synapse\ml\codegen\Wrappable.scala` and `core\src\main\scala\com\microsoft\azure\synapse\ml\core\schema\DatasetExtensions.scala`, together with the documentation and CI selection changes.
- [x] Considered the caller-reported validation results without treating them as independently rerun evidence.
- [ ] No commands, tests, or live requests were executed under this read-only review. The supplied timeout tests do not exercise blocked DNS resolution.

## Issues

### Issue 1: DNS resolution can outlive the operation deadline
- **Severity**: Medium
- **File**: `cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala`
- **Line(s)**: 394-401, 438-441
- **Description**: The deadline only calls `request.abort()`, while `client.execute(request)` remains synchronous and the constructed client uses Apache HttpClient's default DNS resolver. That resolver performs blocking JVM name resolution, which request abortion and the configured connect/socket timeouts do not interrupt. A stalled lookup therefore prevents the call from returning `OperationTimedOut` within `maxOperationDuration`. This contradicts the new total-operation bound, especially now that `concurrentTimeout` is rejected and asynchronous waiting is unbounded.
- **Risk**: Slow or unavailable DNS can hold executor operations beyond the configured budget and delay returning already accepted handles during polling. The loopback timeout test covers socket waiting, not this path.
- **Suggested Fix**: Bound DNS resolution by the remaining operation budget, ensuring an expired request cannot subsequently dispatch. Add a regression using a deliberately blocked `DnsResolver` and verify bounded completion, an `OperationTimedOut` diagnostic, and handle retention for polling.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Resolved
- **What changed**: Added a bounded, budget-aware DNS resolver. It caps lookup waits by the remaining operation budget, cancels timed-out lookups, and maps deadline expiry to `OperationTimedOut`.
- **Why**: Request abortion and socket timeouts do not interrupt synchronous JVM name resolution.
- **How verified**: Added a blocked-resolver regression that verifies bounded return, timeout diagnostics, retained poll handle, and no TCP connection after releasing DNS. Both CU suites passed 71/71 tests; root `FuzzingTest` passed 10 tests with 1 ignored.