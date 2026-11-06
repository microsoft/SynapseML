---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 15
round: 1
timestamp: "2026-11-04T23:03:19Z"
outcome: "issues_found"
reviewer_model: "gpt-6.1-sol"
input_sha256: "7d7c26904c029dfffc57f754b4b23726798a9dc4cb3dc5748baa5c61990ee2bd"
---

---
producer: review-code
schema_version: 1
tier: Max
contract: direct
execution_backend: copilot-cli
attempt: 1
stage_attempt: 15
round: 1
timestamp: "2026-11-04T22:50:50.891Z"
outcome: issues_found
reviewer_model: gpt-6.1-sol
input_sha256: de064985c18a27634078b6effe39c96410855a5438bb29cec7b08c6264a92ec2
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: gpt-6.1-sol
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-15-review-1-gpt-6.1-sol.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Traced request preparation, schema planning, partition stopping, polling, DNS admission, and transport classification through `ContentUnderstanding.scala`, `ContentUnderstandingProtocol.scala`, `ContentUnderstandingDnsResolver.scala`, and `ContentUnderstandingTransport.scala`.
- [x] Compared management-response handling with provisioning coverage in `ContentUnderstandingSuite.scala:656–785` and diagnostic-preservation coverage in `ContentUnderstandingConcurrencySuite.scala:156–172`.
- [x] Reviewed shared Python parameter handling in `core\src\main\python\synapse\ml\core\schema\Utils.py`, generated methods in `Wrappable.scala`, and header definitions in `CognitiveServiceBase.scala`.
- [x] Inspected the Python inspection/persistence tests, CU recovery/concurrency tests, CU documentation, MLflow warning, and dedicated CI package selection.
- [ ] Tests were not executed, as required by this read-only review. Supplied validation results are caller-provided evidence, not independently reproduced results.

## Issues

### Issue 1: Analyzer creation overwrites response-read diagnostics for unsafe handles
- **Severity**: Medium
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala
- **Line(s)**: 747–754
- **Description**: An accepted PUT response can contain both an unsafe `Operation-Location` and a body-read failure. `exchange` retains `ResponseTooLarge`, `TransportError`, or `OperationTimedOut`, but this catch unconditionally replaces that diagnostic with `InvalidOperationLocation`. Moving `analyzerBody(raw)` behind the location branch makes this combination newly reachable; the previous early check preserved the read failure.
- **Risk**: The exception loses the original failure evidence, violating the requirement that invalid handles must not overwrite transport/response-size diagnostics. Callers cannot distinguish the response-bound or transport failure from handle validation alone.
- **Suggested Fix**: Preserve `raw.diagnostic` ahead of the invalid-location diagnostic, while continuing to reject the unsafe URL. Add public `createAnalyzer` cases combining an accepted status and unsafe handle with oversized and truncated bodies; verify the original diagnostic survives and no GET or repeated PUT occurs.

### Issue 2: Telemetry-header values bypass diagnostic redaction
- **Severity**: Medium
- **File**: core\src\main\python\synapse\ml\core\schema\Utils.py
- **Line(s)**: 125–148
- **Description**: The redaction list includes `customHeaders` but omits `telemHeaders`, another map-valued header `ServiceParam` defined in `CognitiveServiceBase.scala:242–263`. Scalar telemetry-header maps therefore fall through to JVM or Python parameter explanations, exposing their values through `explainParam` and `explainParams`.
- **Risk**: Sensitive header contents can enter diagnostic output despite the required scalar auth/header-description redaction. This is separate from the explicitly accepted disclosure through getters, parameter maps, and persistence.
- **Suggested Fix**: Include `telemheaders` in diagnostic redaction. Extend the existing Python inspection test to cover JVM-backed and pending Python scalar telemetry maps, while retaining column-binding names.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending; no edits performed.
- **Why**: Management handling must preserve the original response-read diagnostic.
- **How verified**: Static source trace; regression execution pending.

### Issue 2
- **Status**: Open
- **What changed**: Pending; no edits performed.
- **Why**: Header-description redaction must also cover telemetry-header maps.
- **How verified**: Static inspection of explanation fallthrough and header parameter definitions; regression execution pending.

## Driver resolution update: attempt 15

Original findings and verdict are retained. Collection verified unchanged
source fingerprints, patch hash, HEAD and path set. All 68 GPT calls used
read/search tools with no prior/sibling feedback queries.

- **Issue 1 - reproduced and implemented; final validation pending.**
  Public Python `createAnalyzer` probes confirmed that unsafe handles replaced
  oversized/truncated response diagnostics with `InvalidOperationLocation`,
  while sending one PUT and no GET. The management catch now prioritizes
  `raw.diagnostic`; unsafe locations remain rejected. Scala regressions cover
  HTTP 200/201/202, truncated/oversized bodies, foreign/relative handles, original
  HTTP status, absent unsafe handle, and exactly one PUT. The regression failed
  before the fix. Evidence: `cu-attempt15-gpt-probe.log` and
  `cu-attempt15-scala-red.log`.
- **Issue 2 - reproduced and implemented; final validation pending.**
  Shared Python explanation redaction now includes `telemheaders`. Regressions
  cover CU and AnalyzeDocument, JVM-backed and pending Python maps, both
  explanation APIs, continued explicit scalar access, and visible column
  binding names. Four scalar-description subcases failed before the fix in
  `cu-attempt15-python-red.log`; public probes also confirmed the exposure.
  CU documentation now explicitly includes telemetry-header maps.

These are local fixes, not a clean independent re-review or a merge-ready claim.

### Completed local validation and next dispatch decision

Both findings are locally resolved. `cu-attempt15-fixes-validation2.log` records
core/cognitive main/test style and affected compilation, 1/1 generated-wrapper
and 98/98 CU tests, codegen and local publication passing.
`cu-attempt15-python-fixed.log` records 22/22 Python tests passing; generated
Utils.py is byte-identical to source and loaded JAR hashes match rebuilt and
published artifacts. Full Black passed. The initial validation hit a method
complexity limit; extracting the shared admission predicate corrected it
without suppression.

Built, published and loaded SHA-256:
- Core: `4c28bcdc3dc77a4608f8b21882e3a0de92fbbeac7f62db271d203345911a9026`
- Cognitive: `0ab1782091bd6c0a666f0ee2dfd2cd8d3185ae99ca5b33d74d5570d35588f1cd`

**Stage 1 attempt 16 decision:** Max from the human; stage order and clean gate
from the direct review skill. Primary model `gpt-6-astra`. Change size remains
broad (30 paths, 361689 bytes), with high distributed HTTP/recovery/auth/schema
and JVM/Python interaction complexity. Fresh live-catalog selections are
`gpt-6.1-sol/max`, `grok-4.7/low`, and `claude-opus-5.5/max`, independently
dispatched concurrently. Grok low reasoning retains the human override;
unavailable Grok can be excluded only if GPT and Opus clear. Available findings
count. Existing safety controls remain unchanged; no new bounds or bypass.

Shared frozen snapshot: `cu-stage1-attempt16.patch`,
SHA-256 `cb84049ff900bd8e25b48e0a37988b73b8553b54c13e78680e5620edab9fb4bf`.
All source paths and original PR HEAD remain; generated feedback is excluded by
known filenames. No external CI, live CU request, commit, push or ADO edit ran.
Max stage 1 has not advanced.