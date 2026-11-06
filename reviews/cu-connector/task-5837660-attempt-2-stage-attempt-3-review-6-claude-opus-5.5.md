---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 2
stage_attempt: 3
round: 6
timestamp: "2026-11-05T23:11:28Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "c3810c19ee913d5657a0bdb5c6537f9b81671a370862c9e5fdd88cec59c44497"
---

## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-2-stage-attempt-3-review-6-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Checked that the new generated `isSet` in `Wrappable.pyAdditionalMethods` (core\src\main\scala\com\microsoft\azure\synapse\ml\codegen\Wrappable.scala:83) is not lost by overrides. Every override chains `super.pyAdditionalMethods`: CognitiveServiceBase.scala:284/636/679, AzureMapsTraits.scala:23, ContentUnderstandingParams.scala:216 and OpenAIPrompt.scala:168.
- [x] Checked that the old CU test package is gone from the form CI leg. `cognitive\src\test\scala\...\services\form` contains only the FormRecognizer/FormOntology/FormCore suites. CU suites now live under `services\contentunderstanding` and run in the dedicated `contentunderstanding` pipeline leg.
- [x] Checked the new public `getCustomHeaders: Map[String,String]` (CognitiveServiceBase.scala:226). It overloads the existing protected `getCustomHeaders(row: Row)` (CognitiveServiceBase.scala:543, OpenAI.scala:590) by parameter list. There is no signature collision or public JVM signature break.
- [x] Checked the `pollingDelay` and `maxOperationDuration` param docs (ContentUnderstandingParams.scala:113-117, 145-147) against the docs bounds table (Content Understanding.md:381-390). Both state that poll mode sends its first GET immediately and that post-submission and management-retry waits use the fallback delay. They describe the budget as best-effort and `concurrentTimeout` as rejected; ContentUnderstanding.scala:153-155 enforces the rejection.
- [x] Checked the observability claims in Content Understanding.md:392-398 against the code:
  - `ContentUnderstandingLogging.sharedStop` (ContentUnderstandingLogging.scala:27-34) logs a connector-derived category and the task IDs, or `taskContext=unavailable`.
  - `logManagement` (ContentUnderstanding.scala:445-466) records outcome, elapsed time and exception class name only. It rethrows the original error and keeps the `InterruptedException` behavior.
  - Test ContentUnderstandingSubmissionSuite.scala:149 accepts either task-context form.
- [x] Checked the Python lifecycle and introspection code in core\src\main\python\synapse\ml\core\schema\Utils.py:62-156:
  - `getOrDefault` raises `KeyError` on column bindings and never unpickles bytes (`_service_value_to_python`).
  - `explainParam` redacts the five auth ServiceParams.
  - `clear` clears both the JVM and Python state.
  - The docs (Content Understanding.md:119-133) say redaction is not a persistence or MLflow guarantee.
- [x] Checked the creation and finish flow in ContentUnderstandingProtocol.scala:699-798:
  - Completed creation that expires before retrieval keeps `completed` plus the timeout diagnostic (line 745).
  - A dispatched retrieval failure keeps its own HTTP status and raw response (lines 752-754).
  - A URL-less non-ready result reports `MissingOperationLocation` only when there is no primary error (lines 788-792).
  - A final retrieval succeeds only when the analyzer is ready (line 705).
- [x] Checked the DNS bounds (ContentUnderstandingDnsResolver.scala:13-109). They use a fixed daemon pool, a bounded queue and a semaphore acquired with the remaining budget. The docs (lines 452-455) state that submitted lookups keep their capacity until the resolver returns.
- [ ] Tests not executed. This was a read-only review using view, grep and glob only. Validation evidence is as supplied by the driver: scalastyle, compile, the 47 AdmissionSuite and SubmissionSuite tests, codegen and Black. There was no live CU call.

## Issues

### Issue 1: `ContentUnderstandingException` message gives generic and sometimes contradictory advice and omits the connector diagnostic code
- **Severity**: Low
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala
- **Line(s)**: 44-48 (message); throw sites 703, 706, 745, 752, 766, 774, 782, 790
- **Description**: Every management failure has the same message: "Content Understanding returned ${status} (HTTP ${httpStatus}). Check the configured authentication, analyzer, deployments, and operation budget."
  - Several throw sites keep a non-failed status by design. Line 745 keeps a completed creation (`Succeeded`, last poll HTTP 200) plus `OperationTimedOut`. Line 782 can carry `Running` or `NotStarted` with `PollAttemptsExhausted`. Those messages read "returned Succeeded (HTTP 200)…" or "returned Running…", followed by advice to check authentication and deployments.
  - The message never contains the connector's own diagnostic code, such as `OperationTimedOut`, `PollAttemptsExhausted`, `AnalyzerNotReady`, `AnalyzerRetrievalFailed` or `MissingOperationLocation`. That code is the only safe signal that tells these cases apart.
  - `logManagement` correctly records only the exception class, so anyone working from driver stack traces or notebook output has to inspect `error.response` to learn why creation failed.
- **Risk**: The behavior is correct, but the diagnosis is misleading. A user who sees "returned Succeeded … check authentication" after a creation that finished but whose retrieval was cut off by the budget may retry `createAnalyzer`. If that retry uses `allowReplace`, it repeats provisioning. The actual case is a completed creation with an expired budget, which only needs `getAnalyzer`. This undercuts the contract's requirements for useful recovery advice and safe observability.
- **Suggested Fix**: Change the message text only; do not change exception types or response contents.
  - Add the error code when it is one of a fixed allowlist of connector-defined diagnostic codes (the ones built with `ContentUnderstandingProtocol.diagnostic(...)`). Leave out service-supplied codes and messages.
  - Replace the fixed advice with neutral wording, such as "Inspect the exception response for the diagnostic and any retained operation handle". Alternatively, add one specific hint for `OperationTimedOut`/`AnalyzerRetrievalFailed` after a `Succeeded` creation: retrieve it with `getAnalyzer`; do not re-create.
  - Add one assertion to an existing creation-expiry test that the message contains the code and does not contain service body text.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Previously deferred; remains deferred, not a new defect counted in this pass.
- **What changed**: No source changes. The exception-message diagnostic-code UX recommendation was already retained outside the selected six-finding polish scope.
- **Why**: This additional review does not reopen the earlier deferred exception-message redesign. The recommendation remains visible rather than being relabeled clean or silently discarded.
- **How verified**: Driver inspected the unchanged public exception message at Protocol44-48 and matched this recommendation to the recorded deferred exception diagnostic-code UX item. Structured response diagnostics remain available; no runtime behavior change or new validation claim.
- **Driver record**: Effective decision, validation, frozen snapshot, audit and aggregate disposition are in the same-pass GPT artifact's Third-pass driver disposition. Original Low severity and ISSUES_FOUND retained.

### Subsequent authorized remediation
- The human's fix-and-rerun-until-clean request reopened this current finding; unrelated backlog stays deferred.
- Exception text is now neutral and includes only a fixed allowlisted code when a bounded diagnostic can be decoded. Service payloads/arbitrary codes are not copied into the message; the structured response and public exception signature are unchanged.
- Public management regressions cover safe diagnostic rendering, private/large service errors, creation exhaustion/disconnect and completed-creation deadline messages. All165 CU tests plus main/test style/compile, codegen, formatting and whitespace passed. Snapshot6/evidence are in the same-pass GPT artifact; independent rerun pending. Original Low/ISSUES_FOUND preserved.