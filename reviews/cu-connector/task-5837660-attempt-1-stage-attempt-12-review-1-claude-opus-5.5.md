---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 12
round: 1
timestamp: "2026-11-04T19:49:46Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "2ed4d761fc152a303d7562b702355182fdcddceee5ee9c4cbfc9c7e7b3ee6d60"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-12-review-1-claude-opus-5.5.md
- **Issues Found**: 4
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Protocol path traced end to end** in `cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala`. Sections read:
  - constants and `AdmissionRejections`: L77–99
  - `Settings`, `WireResponse` and `DeadlineControl`: L129–200
  - authority and Operation-Location validation: L208–277
  - bounded body reads: L319–357
  - `exchange` and `exchangeBeforeDeadline`: L359–419
  - per-operation client (no retries, no redirects): L421–431
  - `decode`: L447–478
  - shared-failure and submission classification: L516–572
  - poll loop and `execute`: L574–662
  - `rowHandler`: L671–690

  Behaviour confirmed:
  - A POST or PUT is never replayed.
  - Operation handles must be same-origin and match the exact path prefix.
  - The preserve_handles rules hold. The `acceptedHandle` gate is applied, poll paths never throw, only `ContentUnderstandingException` is caught, and in-flight siblings finish.
  - The `stopped` flag is per task.
- [x] **Transformer** in `ContentUnderstanding.scala` L90–480:
  - Scalar settings are validated on the driver, and `concurrentTimeout` is rejected.
  - The row-validation UDF nulls invalid rows before HTTP dispatch.
  - The mapPartitions boundary prevents the HTTP stage from running again.
  - The pipeline works on a `copy(ParamMap.empty)` snapshot.
  - Output/error column collision checks use the dataset's case-sensitivity resolver.
  - Analyzer management runs on the driver only.
  - The output parser was reviewed.
- [x] **DNS resolver** (`ContentUnderstandingDnsResolver.scala`), read in full:
  - A permit is held until the physical lookup completes.
  - A caller timeout neither releases the permit nor replaces a stuck worker.
  - Every `UnknownHostException` subclass becomes `notSent` → `Rejected`.
- [x] **Shared auth and header code:**
  - `CognitiveServiceBase.scala`: the new accessors at L226–230, the `addContentType` dedupe at L358–361 and L431, `resolve` precedence, and `addHeaders`.
  - `ContentUnderstandingHeaderValidation` and the `HTTPSchema.scala` marker handling. The CU marker never activates Fabric auth and is stripped by `toHTTPCore`.
  - `rowUsesColumnAuth` and the scalar fingerprints agree with the `resolve` precedence.
- [x] **Python and codegen:**
  - `core\src\main\python\synapse\ml\core\schema\Utils.py` L16–140.
  - `core\src\main\python\synapse\ml\core\serialize\java_params_patch.py` L13–38.
  - `Wrappable.scala` L111, L172–215, L266–300 and L522. Generated runtime classes import `ComplexParamsMixin` from `Utils.py`.
  - The CU Python overrides in `ContentUnderstandingParams.scala` L155–264.
  - `ByteArrayParam.scala` (a ComplexParam with a class filter) and `JsonEncodableParam.scala` (ServiceParams are persisted as JSON).
  - The binary ServiceParams: `ComputerVision.scala` L47–58 and `SpeechToText.scala` L39. `FormRecognizerV3.scala` L37–40 shows AnalyzeDocument mixes in `HasImageInput`.
- [x] **Tests inspected:**
  - `ContentUnderstandingSuite.scala`: the ambiguous-POST `TransportError` tests at L402–419.
  - `ContentUnderstandingRecoverySuite.scala`: the shared 404 and sibling-retention tests around L538–579, and the socket stub at L617.
  - `ContentUnderstandingConcurrencySuite.scala`.
  - `test_ContentUnderstanding.py` L200–410.

  A grep of CU sources and tests found no handling of or tests for `ConnectException`, `ConnectTimeout`, `NoRouteToHost` or `SSLHandshake`. No Python test covers a scalar binary ServiceParam on a non-CU stage.
- [x] **Documentation contract** in `docs\Explore Algorithms\AI Services\Content Understanding.md` L188–340 was compared against the code. It covers shared-failure semantics, what `Unknown` and `Rejected` mean, the retry selection, and bounds and timeouts.
- [ ] **No builds or tests were run.** The review used read-only tools only. The supplied local results were not re-run:
  - CU suites 82/82
  - shared suite 7/7
  - Python 15/15
  - Black

  There is no live, CI or remote-base evidence, and the snapshot SHA256 was not recomputed.
- [ ] **Other reviews not read.** `reviews\cu-connector` and other reviewers' artifacts were deliberately not accessed, to keep this review independent.

## Issues

### Issue 1: Shared Python model loading unpickles saved byte values for non-CU stages
- **Severity**: Medium
- **File**: core\src\main\python\synapse\ml\core\schema\Utils.py
- **Line(s)**: L96–108, with `_java2py` at L102. Reached from `from_java` (L37) and from `serialize\java_params_patch.py` L31.
- **Description**:
  - This change modifies ServiceParam handling in `ComplexParamsMixin._transfer_params_from_java`: column bindings are now popped and left JVM-backed. Scalar (`Left`) values, however, still go through `_java2py(sc, jvObj.value())`.
  - For a `ServiceParam[Array[Byte]]`, py4j returns `bytes`. PySpark's `_java2py` treats any `bytes` as a pickle dump and calls `CPickleSerializer().loads`.
  - Affected stages:
    - every `HasImageInput` stage through `imageBytes` (`ComputerVision.scala` L49), including AnalyzeDocument, FormRecognizer and the vision stages;
    - SpeechToText through `audioData` (L39).
  - ServiceParam values are stored as plain JSON, so they bypass the `DeserializationClassFilter` that protects `ByteArrayParam`. ByteArrayParam is a ComplexParam and takes the branch at L97–98, which does not call `_java2py`.
  - `secure_import_class` restricts which classes can be loaded, not the parameter values.
  - The CU override (`ContentUnderstandingParams.scala` L210–216) avoids this path explicitly; its comment says values must not become "pickle-decoded document bytes". The shared path was left unchanged.
  - The `Left` branch appears to predate this diff, but it sits in the shared transfer code this diff modifies and validates.
- **Risk**:
  - Loading an untrusted saved stage or PipelineModel with a crafted scalar `imageBytes` or `audioData` value runs arbitrary code on the Python driver. That defeats the repository's hardened model loading.
  - Harmless saved stages holding real image or audio bytes cannot be loaded in Python; they fail with `UnpicklingError`.
- **Suggested Fix**:
  - Do what the CU override does: pop all ServiceParams from `_paramMap` on load and keep the JVM as the source of truth. The new getters, `isSet` and `explainParam` already read the JVM.
  - At minimum, never pass `Left` values through `_java2py`.
  - Add load tests for `AnalyzeDocument().setImageBytes(...)` covering both ordinary bytes and a pickle payload; the payload must not execute.

### Issue 2: Python `explainParam(s)` now prints credentials for every service stage
- **Severity**: Low
- **File**: core\src\main\python\synapse\ml\core\schema\Utils.py
- **Line(s)**: L61–65. The new test asserting this is in `cognitive\src\test\python\synapsemltest\services\test_ContentUnderstanding.py` L305–317.
- **Description**:
  - The new `explainParam` passes ServiceParams that are not in `_paramMap` to the JVM. Spark's JVM `explainParam` renders them as `current: Left(<value>)`.
  - Generated setters now keep scalar ServiceParams only on the JVM. As a result, `explainParams()` on any generated SynapseML service stage prints `subscriptionKey`, `AADToken`, `CustomAuthHeader` and credential-bearing `customHeaders` in clear text.
  - The new test asserts that `"synthetic-key"` appears in both `explainParam` and `explainParams()`, which locks this behaviour in.
- **Risk**: Keys leak into notebook outputs, logs and support transcripts, which are often shared. Two things soften this: Scala's `explainParams` and Python values loaded from Java already exposed them, and storing credentials in saved models is accepted policy. Display is still a separate exposure channel.
- **Suggested Fix**:
  - Redact auth ServiceParams and auth-named `customHeaders` entries in the explain path (Python, and optionally Scala). Keep showing whether each is a scalar or a column, and show column names.
  - Change the test to assert the scalar/column state without asserting the secret text.

### Issue 3: A definite pre-acceptance shared failure can be reported as `Unknown`, which the documented retry excludes, and its original diagnostic is lost
- **Severity**: Low
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala
- **Line(s)**: L94, L386–415, L454–455, L516–529, L531–556, L647, L683–685. Docs: `Content Understanding.md` L192–197 and L272–291.
- **Description**: Two paths produce the inconsistent result.
  - **(a) 404 with an unreadable body.** A scalar-analyzer POST receives 404, then reading the body fails with an IOException.
    1. `code = 404` is already stored, and the exception produces `transportFailure = true` (L405–415).
    2. `decode` sets the status to `Unknown` (L454–455).
    3. 404 is not in `AdmissionRejections` (L94), so the status stays `Unknown` (L551–552).
    4. `checkSharedFailure` still classifies the row as shared on `httpStatus == 404` (L522–523) and stops the partition.
  - **(b) Rejection carrying an `Operation-Location`.** A 401/403/404 response with an `Operation-Location` header becomes `Unknown` through L537 but is still treated as a shared failure.
  - In both cases `rowHandler` replaces the original diagnostic (`TransportError`, `OperationTimedOut` or the service error) with the generic `SharedConfigurationFailure`.
  - The row ends up as `Unknown` + `SharedConfigurationFailure`. That says "definite shared failure" and "may have been accepted" at the same time.
  - The documented retry selection excludes `Unknown`, and the docs describe triggering rows only as `Failed`.
- **Risk**: The row that triggered a definite shared failure silently drops out of the documented recovery and loses its cause. This is rare and there is no replay risk, but it conflicts with the requirement that triggering rows must not be omitted, and with the instruction to assess inconsistent diagnostics.
- **Suggested Fix**:
  - Make the two classifications agree. Either:
    - report a received pre-acceptance scalar-analyzer 404 with no valid handle as `Failed`, even when the body is unreadable; or
    - do not stop the partition for `Unknown` submissions.
  - Keep the original diagnostic, for example as a `cause` field, and document any remaining combination.
  - Add a stub test that returns a 404 with a truncated body.

### Issue 4: Connection-phase failures where the request was never sent are reported as possibly accepted (`Unknown`)
- **Severity**: Low
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala
- **Line(s)**: L375–386, L404–415, L421–431. Docs: L255–266.
- **Description**:
  - Every IOException other than a DNS failure from `client.execute` becomes `transportFailure` with `notSent = false`. The row gets status `Unknown` and the message "A submitted request may have been accepted".
  - That includes failures raised before any request byte is written:
    - `HttpHostConnectException` / `ConnectException` (connection refused);
    - `ConnectTimeoutException`. It is an `InterruptedIOException`, but when the thread is not interrupted it reaches the generic branch.
    - `NoRouteToHostException`;
    - TLS handshake and peer-verification failures;
    - the deadline abort firing during connect after a slow DNS admission. This is reported as `OperationTimedOut` with "retain any handle", though no handle exists.
  - Each operation uses a new client, so the POST is always the first request on a fresh connection. These failures therefore mean the request definitely was not sent.
- **Risk**:
  - This contradicts the requirement that "Unsent DNS/capacity/deadline failures must remain distinguishable from possibly accepted work".
  - Common enterprise network problems (firewalls, private endpoints, TLS inspection) would mark every row `Unknown`, excluding them all from the documented retry.
  - The behaviour is conservative, so nothing is replayed and no handle is lost.
- **Suggested Fix**:
  - Map these connect-phase exceptions to `notSent = true` → `Rejected` with a `ConnectionFailed` or `RequestNotSent` diagnostic.
  - More robustly, record when the request is actually written, using a custom `HttpRequestExecutor.doSendRequest` flag. Treat any failure or deadline abort before that point as unsent.
  - Keep failures after the write ambiguous.
  - Update the docs and add tests for a closed port and for a connect timeout within a short budget.

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

### Issue 4
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

## Driver triage and reproduction

- **Issue 1 confirmed:** `cu-attempt12-python-red.log` failed loading ordinary image bytes with `UnpicklingError`. A harmless pickle-shaped fixture invoked a function that only raises an assertion, proving implicit unpickling without executing a harmful payload. `git show HEAD` confirmed the scalar `_java2py` load branch predates this diff. It is being fixed because it directly shares the modified transfer path, not represented as a newly introduced vulnerability. Shared loading now leaves ServiceParams JVM-backed; generic scalar access preserves bytes without pickle decoding. Generated-wrapper validation is pending.
- **Issue 2 confirmed:** The red inspection assertion found a synthetic key in the description. Python descriptions now redact scalar authentication parameters and whole custom-header maps, including pending values and JVM defaults, while showing column bindings. Explicit getters/maps, Scala inspection and saved metadata retain standard values; docs distinguish display redaction from persistence. Tests cover token/default/column and header paths. Post-fix validation is pending.
- **Issue 3 under reproduction:** Added truncated-404 and rejection-with-operation-header cases. The intended resolution is to preserve ambiguous outcomes/diagnostics and not trip the definite shared-failure circuit for `Unknown`, rather than falsely claim rejection or make an ambiguous row automatically retryable. Production Scala is unchanged until the red run completes.
- **Issue 4 under reproduction:** Overlaps Grok's report. Refused-loopback and stalled-TLS-deadline tests distinguish pre-send connection failures from existing post-send ambiguity tests. No new operation cap or cancellation engine is planned.
- **Evidence:** Python red suite ran 16 tests, recording 2 failures and 2 errors across the three affected tests (the byte test has two subcases). The Scala red run first encountered a test line-length violation; the corrected run is in progress. Original review outcomes remain dirty.

### Scala reproduction and implementation update

- `cu-attempt12-scala-red2.log` ran three tests: TLS connection-setup expiry and ambiguous shared-response handling failed; the initial refusal test passed. The TLS listener confirmed connection establishment before expiry, and the response was `Unknown` instead of `Rejected`. The truncated-404 case replaced the transport diagnostic with `SharedConfigurationFailure`.
- Strengthened the refused-port test because its original success did not prove which pre-send path ran: it now requires the actual `HttpHostConnectException`, with enough budget for first-client initialization.
- **Issue 3:** `checkSharedFailure` now excludes `Unknown`. These outcomes retain their original diagnostic and do not stop subsequent submissions. Definitive shared failures still stop new submissions and preserve sibling handles. Added both truncated-body and rejection-with-operation-header cases; the second case awaits post-fix execution because the first assertion stopped the red loop.
- **Issue 4:** A request-local context marker records the start of HTTP writing through a stateless Apache request-executor hook. Before that boundary, connection/TLS/deadline I/O failures are `Rejected`/`RequestNotSent`; at or after it, ambiguity remains. Polling still retains accepted handles. No remote cancellation or hard deadline guarantee is added.
- Main scalastyle found excess complexity in the first implementation. Diagnostic construction was factored into a small helper with unchanged messages; no lint rule was weakened. Full Black passes (205 files unchanged). `cu-attempt12-fixes-validation2` is running; generated Python and final loaded-artifact checks remain pending.

### Completed local verification

- `cu-attempt12-fixes-validation4.log`: main/test style and compilation, all 85 CU tests, core/cognitive codegen and local publication passed. The transport helper was extracted into private `ContentUnderstandingTransport.scala` and named consistently; complexity/method-count/naming checks were not suppressed.
- `cu-attempt12-python-fixed.log`: 16/16 offline tests passed with regenerated Python and fingerprint-matched JARs.
- **Issue 1:** Both ordinary media and the pickle-shaped sentinel loaded/copied/transferred as unchanged bytes; the rejecting function was not invoked. Generic scalar access/map extraction also retained the byte payload.
- **Issue 2:** Synthetic scalar keys, tokens, auth headers, header maps and a JVM default token were absent from Python parameter descriptions. Column names remained visible; explicit value access stayed intact.
- **Issue 3:** Both truncated-404 and rejection-with-operation-header cases retained `Unknown` plus their original diagnostics, allowed the following submission and preserved its accepted handle. Definitive shared-failure circuit regressions also passed.
- **Issue 4:** The strengthened refused-port case proved `HttpHostConnectException`; the TLS-setup timeout returned unsent rejection. Existing post-send ambiguity/handle tests passed.
- All four findings are locally addressed. The original report remains `issues_found`; independent re-review and external readiness evidence remain outstanding.