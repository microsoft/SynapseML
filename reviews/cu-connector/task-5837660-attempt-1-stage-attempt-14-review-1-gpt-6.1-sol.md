---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 14
round: 1
timestamp: "2026-11-04T21:42:34Z"
outcome: "issues_found"
reviewer_model: "gpt-6.1-sol"
input_sha256: "e6c757e41d93a44925fb5817d115c8332041fea0bd4f0bb68d505d6a52f3fe50"
---

---
producer: review-code
schema_version: 1
tier: Max
contract: direct
execution_backend: copilot-cli
attempt: 1
stage_attempt: 14
round: 1
timestamp: "2026-11-04T21:32:23.398Z"
outcome: issues_found
reviewer_model: gpt-6.1-sol
input_sha256: a0965abda2a4d48a51c23990f0f95a03441731dc7255b5c3ca513293c72b5cef
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: gpt-6.1-sol
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-14-review-1-gpt-6.1-sol.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Inspected submission classification, polling, authentication precedence, deadlines, and DNS lifecycle in `ContentUnderstanding.scala`, `ContentUnderstandingProtocol.scala`, `ContentUnderstandingDnsResolver.scala`, and `ContentUnderstandingTransport.scala`.
- [x] Reviewed shared Python parameter conversion and generated getters/setters in `core\src\main\python\synapse\ml\core\schema\Utils.py`, `Wrappable.scala`, and the generated-wrapper regression test source.
- [x] Traced Dataset/session handling through `DatasetExtensions.scala`, `NamespaceInjections.scala`, `SimpleHTTPTransformer.scala`, and `Lambda.scala`; compared it with the documented multi-session boundary.
- [x] Inspected CU main, recovery, concurrency, persistence, and opt-in live test sources, plus the supplied validation report and approved `pipeline.yaml` selection.
- [ ] No tests or commands executed under this read-only assignment. Supplied passing results were not independently reproduced; external CI and live-service behavior remain unverified.

## Issues

### Issue 1: Internal schema planning still uses the wrong session for direct transforms
- **Severity**: Medium
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstanding.scala
- **Line(s)**: 375–377
- **Description**: The input Lambda has no explicit schema callback. `PipelineModel.transform` performs schema planning, and `Lambda.transformSchema` (`core\src\main\scala\com\microsoft\azure\synapse\ml\stages\Lambda.scala:60–64`) evaluates that Lambda against an empty DataFrame created in the active session. Consider a case-sensitive input session containing distinct `document` and `DOCUMENT` columns, an exact `setDocumentUrlCol("document")` binding, and a different case-insensitive active session. The Lambda's quoted `document` reference becomes ambiguous during internal schema planning, despite `transform` obtaining the correct Dataset resolver.
- **Risk**: A valid direct `transform(dataset)` fails before dispatch, violating the documented Dataset-session behavior. Existing cross-session tests cover output-name collisions, not case-distinct bound input columns.
- **Suggested Fix**: Supply session-independent schema callbacks for the internal Lambdas, avoiding active-session expression resolution during direct transformation. Add a regression using case-distinct input columns and opposite active-session case sensitivity; do not mutate shared SQLConf.

### Issue 2: Location assessment overwrites diagnostics from ambiguous response reads
- **Severity**: Low
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala
- **Line(s)**: 564–567
- **Description**: A POST response with HTTP 202, no `Operation-Location`, and a truncated or oversized body first becomes `Unknown` with `TransportError`, `OperationTimedOut`, or `ResponseTooLarge`. Because `assessment.missing` is true, this branch replaces that diagnostic with `MissingOperationLocation`. An invalid location can similarly replace an existing transport diagnostic.
- **Risk**: The original failure cause is lost, contrary to the specified preservation of ambiguous `Unknown` diagnostics. In particular, oversized responses no longer reliably report the configured response bound.
- **Suggested Fix**: Preserve `raw.diagnostic` as the primary diagnostic, or retain both causes structurally, while maintaining `Unknown` and rejecting unsafe handles. Add focused truncated/oversized HTTP 202 cases without a handle and verify that no POST is replayed.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending.
- **Why**: Direct transforms must honor the Dataset session throughout internal planning.
- **How verified**: Static call-path inspection; regression execution pending.

### Issue 2
- **Status**: Open
- **What changed**: Pending.
- **Why**: Ambiguous submission diagnostics must retain their original cause.
- **How verified**: Static classification-path inspection; regression execution pending.

## Driver resolution update: attempt 14

The original review and verdict above are retained. The source freeze was
lifted only after collecting all three reviewers. The collection audit found
unchanged source fingerprints, patch hash, HEAD, and path set; all 67 GPT tool
calls were read/search calls with no prior/sibling feedback queries.

- **Issue 1 - implemented, awaiting final validation:** CU's input and output
  Lambdas now supply explicit schema callbacks instead of constructing a
  temporary DataFrame in the active session. Direct transforms preserve the
  Dataset-owned resolver without changing SQLConf or the active session.
  Public schema-only/external Pipeline planning still requires session
  alignment, and the documentation distinguishes that boundary.
- **Regression evidence:** `cu-attempt14-scala-red.log` reproduced the
  `AMBIGUOUS_REFERENCE` failure. The intermediate run passed a 20-scenario
  matrix covering both matching/opposite session directions, URL/binary
  submission and analysis, polling handles, MIME/auth bindings, complete schema
  equality, original-column preservation, zero planning-time requests, actual
  submitted values, method counts, and session/config preservation.
  Additional negative cases cover output/error collisions and missing/wrongly
  typed bindings under opposite sessions.
- **Issue 2 - implemented, awaiting final validation:** `safeSubmission` gives
  the original `raw.diagnostic` precedence over missing/unsafe-handle
  diagnostics. It retains `Unknown`, rejects unsafe handles, and does not
  replay POST. Truncated and oversized 202 responses, missing and foreign
  handles, and both submit/analyze modes failed before the fix and passed in
  `cu-attempt14-scala-intermediate.log`.

### HTTP rejection-policy correction

The earlier attempt-13 resolution was overly conservative. A body-read failure
does not erase a complete HTTP rejection status/header section. HTTP 401/403/429
without a contradictory operation handle now remain `Rejected`, retaining the
connector-generated read diagnostic. Scalar authentication failures still stop
the partition and nest that diagnostic under `SharedConfigurationFailure`.
Service-provided error messages remain outside the generic shared diagnostic.
No evidence demonstrated CU accepting work behind a genuine rejection response.
This follows the status semantics of RFC 9110 sections 15.5.2/15.5.4 and RFC 6585
section 4; it does not establish exactly-once execution.

The new header-policy regression failed as expected in the intermediate run.
It covers 401/403/429, scalar/column credentials, oversized/truncated bodies,
trigger diagnostics and subsequent-row behavior. Separate canary cases ensure
service JSON cannot imitate a connector diagnostic and leak its message into
the generic shared error. Contradictory operation-header and truncated-404
ambiguity cases remain covered.

Final Scala/codegen/publication and Python validation were still pending when
this update was written; this is not a clean re-review or merge-ready claim.

### Completed local validation

Both findings are locally resolved, pending independent re-review.
`cu-attempt14-fixes-validation2.log` records core/cognitive main and test
scalastyle, affected compile/test-compile, 1/1 generated-wrapper test, and 95/95
CU main/recovery/concurrency tests passing with no failed/skipped tests.
Codegen and local publication passed. `cu-attempt14-python-fixed3.log` records
20/20 offline Python tests passing against generated wrappers and verified
rebuilt JARs. Generated Utils.py matches source bytes; repository Black passed,
as did the subsequently corrected Python test. The initial validation command
had a WSL shell-quoting error before SBT started; its corrected invocation is
the recorded successful Scala run.

Built, published and actually loaded JAR SHA-256 values:

- Core: `2be4ef6cefd69e27b76c3843e6c655a4417862d0c4b0656a4296d12a4e8d6dba`
- Cognitive: `31174ca351dfa83af6532a409a55f209bf9bba0d2a68ffc48f08618f1ed978be`

No live CU call, external Azure CI, commit, push or ADO edit was performed.
The branch remains behind cached master, with fresh remote verification
unavailable. Max stage 1 has not advanced.

### Driver pre-dispatch decision: stage 1 attempt 15

- **Tier/source:** Max from the driving human; ordered stages and clean-stage
  gate from the direct review skill. Primary model: `gpt-6-astra`.
- **Size/complexity:** broad, 30 paths and 353053 patch bytes; high interaction
  complexity across distributed HTTP, recovery, credentials, Spark schema
  planning and JVM/Python serialization.
- **Reviewers:** freshly resolved live-catalog `gpt-6.1-sol/max`,
  `grok-4.7/low`, `claude-opus-5.5/max`, independently dispatched concurrently.
  Grok low reasoning retains the human override. Unavailable Grok may be
  excluded only if GPT and Opus clear; available Grok findings count.
- **Rationale/gate:** locally validated fixes require an independent
  stage-1 retry; all required available reviewers must clear before advancement.
  Existing safety controls remain unchanged; no new bound or bypass.
- **Snapshot:** `cu-stage1-attempt15.patch`,
  SHA-256 `de064985c18a27634078b6effe39c96410855a5438bb29cec7b08c6264a92ec2`.
  The added source path is the Autologging documentation. Generated feedback is
  excluded by known filename, including nested review folders. Source remains
  frozen through collection. Original PR HEAD and all earlier artifacts remain.