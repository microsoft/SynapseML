---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 13
round: 1
timestamp: "2026-11-04T20:38:21Z"
outcome: "issues_found"
reviewer_model: "gpt-6.1-sol"
input_sha256: "e6e4acb0bb443fe7a40e510885a58dabd18b645bdb32112af6655d2e8577e635"
---

---
producer: review-code
schema_version: 1
tier: Max
contract: direct
execution_backend: copilot-cli
attempt: 1
stage_attempt: 13
round: 1
timestamp: "2026-11-04T20:25:15.633Z"
outcome: issues_found
reviewer_model: gpt-6.1-sol
input_sha256: fb18f5f61ab57ea46d86d460177c0e5d69f4daae5c99611df1bb34863cd958d7
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: gpt-6.1-sol
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-13-review-1-gpt-6.1-sol.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Reviewed CU runtime, request validation, authentication precedence, submission classification, polling, management operations, deadlines, and DNS lifecycle in `cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\`.
- [x] Traced HTTP integration through `SimpleHTTPTransformer.scala`, `HTTPTransformer.scala`, `HTTPClients.scala`, `Clients.scala`, and `HTTPSchema.scala`.
- [x] Reviewed shared Python parameter transfer and code generation in `core\src\main\python\synapse\ml\core\schema\Utils.py` and `core\src\main\scala\com\microsoft\azure\synapse\ml\codegen\Wrappable.scala`, including binary-data handling and credential redaction.
- [x] Inspected Scala/Python regression coverage, dataset-resolver changes, documentation, and the `pipeline.yaml` package selection.
- [ ] Tests were not executed: this invocation permits read-only inspection. Supplied validation results were considered but not independently reproduced; external Azure CI and live-service validation remain unavailable.

## Issues

### Issue 1: Transport-failed admission responses become definite rejections
- **Severity**: High
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala
- **Line(s)**: 538–550
- **Description**: If response headers contain HTTP 401, 403, or 429 without `Operation-Location`, but reading the body times out or encounters truncation, `exchangeBeforeDeadline` returns `transportFailure = true`, `notSent = false`, and a transport diagnostic. Nevertheless, the admission-rejection branch executes before `uncertainSubmission` and changes the decoded `Unknown` status to `Rejected`. For scalar-authentication 401/403 responses, this also triggers the shared-failure circuit, replacing the original diagnostic. The regression at `ContentUnderstandingConcurrencySuite.scala:219–238` covers truncated 404 responses and 401 responses with operation headers, but misses this combination.
- **Risk**: An ambiguous, already-sent submission enters the documented retry-safe selection, and scalar-authentication cases unnecessarily stop subsequent submissions while losing their transport diagnostic. This violates the requirement to preserve ambiguous `Unknown` outcomes.
- **Suggested Fix**: Give transport uncertainty precedence over admission-status classification after handling proven-unsent requests. Add truncated/body-timeout cases for 401, 403, and 429 without operation headers; verify `Unknown`, preservation of the original diagnostic, and continued processing of subsequent rows.

### Issue 2: JVM-backed defaults remain invisible to Python Params inspection
- **Severity**: Low
- **File**: core\src\main\scala\com\microsoft\azure\synapse\ml\codegen\Wrappable.scala
- **Line(s)**: 83–89, 155–158
- **Description**: The new bridge makes `getOrDefault` and `extractParamMap` consult JVM service defaults, but generated `isSet` only consults JVM explicit settings. Service defaults are still omitted from Python’s `_defaultParamMap`, and neither `hasDefault` nor `isDefined` is bridged. Consequently, a fresh `ContentUnderstanding` returns `"prebuilt-read"` from `getOrDefault("analyzerId")` while reporting both `hasDefault("analyzerId")` and `isDefined("analyzerId")` as false. The default API version has the same inconsistency.
- **Risk**: Standard Params-based configuration and inspection incorrectly treat usable defaulted parameters as undefined, potentially replacing intended defaults.
- **Suggested Fix**: Make default/defined-state inspection consistent with the JVM-backed getters, while preserving column bindings. Ensure `_transfer_params_to_java` does not assume JVM-only defaults exist in Python’s default map. Cover fresh instances, load/copy, and clearing explicit overrides.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending.
- **Why**: Transport-uncertain submissions must not become retry-safe rejections.
- **How verified**: Static trace through exchange, submission classification, and shared-failure handling; no tests executed.

### Issue 2
- **Status**: Open
- **What changed**: Pending.
- **Why**: Python Params inspection must agree with JVM-backed default values.
- **How verified**: Inspected generated default handling and the shared Python parameter bridge; no tests executed.

## Driver resolution and evidence - attempt 13

The original report and `issues_found` verdict above are retained. The driver
completed local resolution; independent clearance requires a new review.

### Effective review decision and transport audit

Max/direct, stage 1 broad sweep, attempt 13, selected by the driving human.
Primary: `gpt-6-astra`. This was a broad 29-path, 297704-byte snapshot with
high interaction complexity across distributed HTTP, recovery, credentials,
code generation and JVM/Python persistence. The live catalog resolved
`gpt-6.1-sol/max`, `grok-4.7/low` (human reasoning override), and
`claude-opus-5.5/max`. The human exception for an unavailable Grok applies only
when GPT and Opus clear; all three were available and returned `issues_found`.
Stage 1 has not advanced. Existing outer safety bounds remain authoritative.

All three runner receipts persisted. Tool transports contained only read/search
calls: GPT 59, Grok 34, Opus 119; no prior/sibling feedback queries were found.
HEAD, all source fingerprints, changed-path set and patch hash were unchanged
through collection. Snapshot SHA256:
`fb18f5f61ab57ea46d86d460177c0e5d69f4daae5c99611df1bb34863cd958d7`.
The runner-owned frontmatter is authoritative; reviewer-authored hash blocks
inside responses are not replacement provenance.

### Issue 1 - addressed conservatively

The local truncated-401 fixture reproduced `Rejected` instead of the expected
`Unknown`. This demonstrates classification/diagnostic behavior, not that a
real CU service accepted an operation after sending 401. The fix deliberately
uses the conservative contract: `decodeSubmission` treats admission statuses as
definitive only without `transportFailure`. Complete 401/403/429 responses still
produce rejection; proven-unsent failures remain retry-safe, and accepted
handles remain preserved. No automatic POST replay was added.

`ContentUnderstandingConcurrencySuite` now exercises truncated 401, 403, 429
and 404 responses plus a rejection carrying an operation header. It checks
Unknown, retained transport diagnostics, absence of an invented handle, and
continued submission of the next row. Documentation states the incomplete-body
boundary explicitly.

### Issue 2 - addressed

`ComplexParamsMixin.hasDefault` now consults JVM ServiceParam defaults, so the
inherited `isDefined` agrees with scalar access. `_transfer_params_to_java`
checks actual Python default-map membership rather than assuming every JVM
default exists there. No raw default or column binding is pushed as an Either.

Regression coverage includes fresh CU and AnalyzeDocument instances, copy,
load, scalar/column overrides and clear. An initial assertion that loaded
defaults must have `isSet == false` was incorrect for the existing SynapseML
serializer: `ComplexParamsSerializer.scala:111-126` saves `extractParamMap`
as explicit metadata as well as a separate default map. The corrected test
requires fresh defaults to be unset and loaded Python state to match the JVM;
serialization behavior was not changed.

### Validation and retained qualifications

Session logs under `653e4257-0554-408f-9e0e-b23750249edb\files`:

- `cu-attempt13-python-red2.log`: 18 tests; 12 failing subcases across the two
  added default/map tests, without fixture errors. The earlier red log retains
  four errors from attempting an unsupported dedicated telemetry getter; the
  corrected telemetry cases use generic parameter inspection.
- `cu-attempt13-scala-red.log`: three selected tests; classification and the
  explicit unused-handler guard failed, while the existing mixed-credential
  partition-stop policy passed.
- `cu-attempt13-fixes-validation.log`: core main/test and cognitive main style
  passed; three test null-argument style errors were corrected without rule
  suppression.
- `cu-attempt13-fixes-validation2.log`: test style, affected main/test
  compilation, one generated-wrapper test and 89/89 CU tests passed; codegen
  and core/cognitive local publication passed.
- `cu-attempt13-python-fixed.log`: map cases passed; only the incorrect loaded
  `isSet` expectation failed. `cu-attempt13-python-fixed2.log`: 18/18 passed.
  Generated Utils.py equals source. Loaded core/cognitive JAR fingerprints
  equal rebuilt artifacts: core
  `b31a625a320ed926727eb4b4c71c7cf851051418f457c9e99f55fe06c15591df`,
  cognitive
  `6a3414baa3cb64877cb9d9578404bb2b850b761951c22539f1f35b3acfbd585d`.
- Full Black passed (205 files); the subsequently adjusted Python test also
  passed. The existing Scala TestBase deserialization configuration was
  unchanged; Python validation did not enable legacy unsafe loading.

No live CU call, external Azure CI, commit, push or ADO edit was performed.
Latest-target synchronization remains outstanding; local validation is not
merge readiness or an independent clean verdict.