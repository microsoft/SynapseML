---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 24
round: 1
timestamp: "2026-11-05T17:52:36Z"
outcome: "issues_found"
reviewer_model: "gpt-6.1-sol"
input_sha256: "8b938cedd467879ec175820a25df36c617b082808e32343f87b81fbcf3b936e9"
---

---
producer: review-code
schema_version: 1
tier: Max
contract: direct
execution_backend: task
attempt: 1
stage_attempt: 24
round: 1
timestamp: "2026-11-05T17:38:24.729Z"
outcome: issues_found
reviewer_model: gpt-6.1-sol
input_sha256: 1d1ad85ceea641fd031f6d78cf8130ef996d765363d6cbfde635fe8f9fbb7ce4
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: gpt-6.1-sol
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-24-review-1-gpt-6.1-sol.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Reviewed CU transformation, authentication precedence, submission classification, polling, creation, DNS deadlines, and parsing bounds in the supplied diff and corresponding Scala sources.
- [x] Inspected shared compatibility changes in `Utils.py`, `Wrappable.scala`, `ComplexParamsSerializer.scala`, `DatasetExtensions.scala`, and `SimpleHTTPTransformer.scala`, including canonical HTTP and ServiceParam dependencies.
- [x] Compared creation handling against `ContentUnderstandingAdmissionSuite.scala:138–169` and `ContentUnderstandingSuite.scala:739–763`: the resource-status matrix always supplies a handle; the handle-less test covers only `creating`.
- [x] Inspected generated-wrapper test coverage, including complete-response preservation, credential redaction, binary handling, and copy/save/load. Reviewed caller-supplied validation claims; no commands or tests were executed.
- [ ] Current-target integration and external CI remain unverified; this review does not establish merge readiness.

## Issues

### Issue 1: Handle-less creation responses overwrite primary status diagnostics
- **Severity**: Medium
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala
- **Line(s)**: 773–779
- **Description**: The creation-specific decoder is used only when `Operation-Location` exists. Without that header, an HTTP 201 body such as `{"analyzerId":"custom"}` or `{"analyzerId":"custom","status":"future-state"}` passes `analyzerBody`, then unconditionally replaces the decoder’s `InvalidResponse` with `MissingOperationLocation`. Likewise, `status:"failed"` without an error object loses `OperationFailed`. This violates the retained requirement that missing/unknown creation statuses preserve `InvalidResponse` and failed resources retain failure handling.
- **Risk**: Callers receive the wrong machine-readable failure category, obscuring malformed responses or actual creation failures and defeating documented diagnostic-based handling.
- **Suggested Fix**: Decode every initial PUT response using `CreationStatusNames` and preserve existing diagnostics. Synthesize `MissingOperationLocation` only when it is the primary problem for an otherwise valid non-ready resource. Extend the creation matrix with handle-less missing, unknown, and failed statuses.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Fixed and locally verified by the driver; subsequent independent review pending.
- **What changed**: Only the URL-less non-ready branch now decodes with `CreationStatusNames` and retains `response.error` before falling back to `MissingOperationLocation`. Ready responses, handle-present polling, analysis vocabulary, HTTP/transport/service-error precedence and shared code remain unchanged.
- **Why**: Missing/unknown creation status must retain `InvalidResponse`; a failed resource without explicit service error must retain `OperationFailed`. A valid non-ready resource lacking its operation URL still reports `MissingOperationLocation`.
- **How verified**: `cu-attempt25-scala-red.log` reproduces the old failure through public `createAnalyzer`. Added one focused matrix in the existing admission suite for200/201/202, eight body cases, exact raw body/HTTP retention, absent handle/id and single PUT. `cu-attempt25-validation.log`:143/143 across six CU suites including fuzzing, cognitive main/test style/compilation, core/cognitive codegen/publication passed. `cu-attempt25-python-green.log`:29/29 against actual loaded hash-matched JARs. Black205 unchanged and whitespace clean.

## Driver decision and collection

- **Effective dispatch:** Human-selected Max/direct, stage 1 attempt 24; primary `gpt-6-astra`. Live catalog selections: `gpt-6.1-sol/max`, `grok-4.7/low` (human override), `claude-opus-5.5/max`. Broad 36-path Scala/Python/transport/persistence change with qualitatively high state/concurrency complexity supports the chosen tier. Snapshot SHA256 `1d1ad85ceea641fd031f6d78cf8130ef996d765363d6cbfde635fe8f9fbb7ce4`.
- **Collection:** GPT reported this one issue; Grok reported clean. Their runtime identity and read-only tool arguments were audited with no prior/sibling feedback access. The first runner-owned YAML header is authoritative; the additional reviewer-written header above is retained but not provenance.
- **Opus cancellation:** The user explicitly stopped Opus after approximately 59 minutes on 2026-11-05. Its receipt remained `dispatching`, with no terminal result or finalized artifact; it cannot be replayed as a completed review. All 31 nonempty public progress messages were inspected: no concrete finding or final verdict was recorded. Its 194 completed tool calls and raw transport remain in the external run directory. Private reasoning was not used as review findings. Cancellation is not clean signoff.
- **Causality:** The original committed HEAD `f3a7963c217677e9b6564cc6736495e0f1bcdce4` already contained the unconditional handle-less diagnostic overwrite. The latest creation-vocabulary fix corrected the handle-present path but left this sibling path unchanged. Existing tests covered the resource-status matrix with a handle and only `creating` without a handle.
- **Next policy:** The user explicitly selected High tier with GPT/Opus at `high` reasoning and Grok at `low` after this attempt. Two subsequent High attempts are allowed per stage; unresolved findings remain visible. Earlier Max attempts do not consume this new allowance, and cancellation does not count as completed review. Existing safety controls and stopped monitoring schedules remain unchanged.
- **Next snapshot:** Global stage attempt25, High-policy attempt1;36 paths,448573 bytes, SHA256 `e1a91c8073e8822133d81f6e7ceb567953a05d7abcac4295342e4dd48d1213a2`. Only protocol and admission suite changed since snapshot24. Built/published/loaded core SHA256 `4890b2366afa1c796f2826358912142bce434d4fa2bfe60c9e65c00f67b15b6a` remains unchanged; cognitive SHA256 `0e90bc938443c134d2c46b7f3cce27f2669121dfc99f39675af83efbd2e76003`. Live CU, current-target integration and external CI remain outstanding; this is not merge readiness.