---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 2
stage_attempt: 2
round: 6
timestamp: "2026-11-05T22:47:08Z"
outcome: "issues_found"
reviewer_model: "grok-4.7"
input_sha256: "91c05b0cfae1fd8a89a37a079a872e52716bb57aa8f7346ffdc1990ab8d00722"
---

## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-2-stage-attempt-2-review-6-grok-4.7.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Compared `pollingDelay` docs with poll entry in `ContentUnderstandingProtocol.execute` / `pollWithClient` (`cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala`, `ContentUnderstandingParams.scala`, `docs/Explore Algorithms/AI Services/Content Understanding.md`)
- [x] Checked management logging and shared-stop warnings against `ContentUnderstandingLogging.scala` and the bounds section of the CU doc (outcome, elapsed time, exception type, no response bodies)
- [x] Checked DNS/deadline wording against `BudgetedDnsResolver` and `DeadlineControl` (executor DNS cap is documented separately from per-partition concurrency; deadlines remain best-effort)
- [ ] Live CU, Python runtime, and publication JAR runs were not inspected; this review used frozen source and the supplied validation notes only

## Issues

### Issue 1: pollingDelay docs claim a first-poll wait that poll mode skips
- **Severity**: Medium
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala
- **Line(s)**: 640-642
- **Description**: `pollingDelay` is documented as the wait before every operation poll, including the first, when `Retry-After` is absent (`ContentUnderstandingParams.scala` lines 113-115; CU doc line 387). Analyze mode does that via `retryDelay(..., settings.pollingDelay)`. Poll mode calls `pollWithClient(..., None, 0L, budget)`, so the first GET is immediate. Later polls still use `pollingDelay`. Management retries match the “between retries” wording; only the operation-poll sentence is wrong.
- **Risk**: Callers using poll mode with the default 1000 ms delay will hit CU immediately and can trip throttling they thought `pollingDelay` would space. Generated Python docs repeat the same sentence.
- **Suggested Fix**: Pass `settings.pollingDelay.toLong` as poll mode’s `firstDelay`, or narrow the param and doc to say the first poll-mode GET is immediate and `pollingDelay` applies to later polls and to the first analyze-mode poll when `Retry-After` is absent.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Addressed locally after final review; not independently re-reviewed.
- **What changed**: Corrected the Scala parameter description and Markdown guide, then regenerated Python documentation. Poll-only firstDelay remains zero; execution timing is unchanged.
- **Why**: The docs-only option preserves existing behavior while distinguishing immediate poll-only entry from waits after submission and between later polls.
- **How verified**: The driver ran all 47 tests in AdmissionSuite and SubmissionSuite, including a new public-transformer timing-contract regression. Main/test style and compile, codegen, Black, and whitespace passed. All four generated Python descriptions contain the corrected wording.
- **Evidence**: Final local snapshot5 and validation logs are recorded in the sibling GPT artifact's Final driver disposition. This is the same documentation finding reported by GPT and Opus. The original Medium severity and ISSUES_FOUND verdict remain unchanged; no third model review was run.