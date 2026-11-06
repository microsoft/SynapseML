---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 19
round: 1
timestamp: "2026-11-05T06:23:27Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "ba542f91bec19506a0c21f33501d9bf568061eb058834cdc3e5c10ea4c390722"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-19-review-1-claude-opus-5.5.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Main transformer: `cognitive/.../contentunderstanding/ContentUnderstanding.scala`
  - L59-64: the `transform` override uses `DatasetExtensions.resolverFor(dataset)`. It does not mutate SQLConf.
  - L77-90: `activeParams`. The `getVectorParamMap` override packs only the bindings that are active in the current mode.
  - L98-160: validation, the rejection of `concurrentTimeout`, settings, scalar auth fingerprints and `rowUsesColumnAuth`. `rowUsesColumnAuth` follows the same precedence as `ServiceAuthHeaders.resolve`.
  - L208-285: URL, option and schema validation. Bindings require an exact name and a resolver count of 1. Scalar values are validated on the driver and column values per row. Invalid rows get an `InvalidInput` diagnostic and send no HTTP request.
  - L286-334: request and URL building with `URIBuilder` and the row-auth marker.
  - L340-418: backtick-quoted columns, internal temp names chosen case-insensitively, output/error collision checks that use the resolver, and the snapshot copy.
  - L420-450: management. It is driver-only, requires scalar parameters and validates the JSON definition before any HTTP request.
- [x] `ContentUnderstandingProtocol.scala`
  - Submission assessment: `SubmissionLocation`, `uncertainSubmission` (L523-530), and `Rejected` vs `Unknown` for not-sent requests, 401/403/429 with a body-read failure or oversize body, 404 with a diagnostic, 408, 5xx and contradictory handles.
  - Shared stop: `FailureClassifier` (L484-505), `checkSharedFailure` (L508-521), and `rowHandler` (L657-686). Each deserialized task gets its own `AtomicBoolean`. Poll mode is never short-circuited. The nested `innererror` is included only for connector codes when the raw response is empty.
  - Client and execution: `withClient` (L425-437) disables retries, redirects and cookies, and uses budgeted DNS. `execute` covers L630-655.
  - Polling: `retainPollState` and `pollWithClient` keep the handle and prior state. The last error wins over timeout or exhaustion.
  - Management: create/get with bounded GET retries, no PUT replay, and the final GET error preserved.
- [x] Helper files:
  - `ContentUnderstandingDnsResolver.scala`: fair semaphore of 32 permits, a fixed pool, and the permit is released in the worker's `finally`.
  - `ContentUnderstandingResponseParsing.scala`:
    - header limits: 64KiB lines and 200 headers;
    - an iterative, string- and escape-aware pre-scan that caps nesting at 1000 and numeric tokens at 100 characters;
    - `Retry-After`: up to 19 digits with saturation, then HTTP-date, then `pollingDelay`;
    - tail-recursive `errorCodes`.
  - `ContentUnderstandingTransport.scala`: tracks whether request writing has started.
  - `ContentUnderstandingHeaderValidation.scala`: validates header names and values.
  - `ContentUnderstandingParams.scala` (L100-160 plus the Python overrides).
- [x] Shared code in `CognitiveServiceBase.scala`:
  - L223-230: new `getCustomHeaders`, `setCustomHeadersCol` and `getCustomHeadersCol`.
  - L340-440: `resolve` emits exactly one auth header.
  - `addContentType`: before this change, `addHeaders` (L555-557, `req.addHeader`) could send case-variant duplicate `Content-Type` headers. Removing the duplicate fixes that and does not regress other services.
- [x] Core: `SimpleHTTPTransformer`, `HTTPTransformer`, `SharedVariable`, marker stripping in `HTTPSchema.toHTTPCore`, and `AsyncHTTPClient` (wraps handlers in `blocking`).
  - `JsonEncodableParam.scala` L84-130: ServiceParam JSON round trip.
  - `Wrappable.scala` L83-92: runtime `isSet`. L434-437: the stub keeps PySpark's inherited signature.
  - `core/src/main/python/.../Utils.py`: ComplexParamsMixin, including redaction, JVM-only scalars, raw bytes and dict maps.
- [x] Security checks:
  - analyzerId must be a safe path segment (Protocol L215-218).
  - `documentUrl` must use http(s) and have no userinfo or fragment (L208-213).
  - `operationLocation` is validated against the endpoint.
  - No `.rdd`, `/defaults` or PATCH usage in CU main sources (grep found no matches).
- [x] Docs, `docs/Explore Algorithms/AI Services/Content Understanding.md`:
  - L44 and L145: defaults;
  - L95-125: credentials and MLflow;
  - L205-250: session alignment and shared stop;
  - L262-345: recovery selection;
  - L300-312: `Rejected` semantics;
  - L355-405: bounds;
  - L412-441: management.

  The approved `pipeline.yaml` entry is at L821-822.
- [x] Tests spot-checked for the findings:
  - `ContentUnderstandingSuite.scala` L424-440 and L657-735: every create test sets analyzerId explicitly.
  - `ContentUnderstandingConcurrencySuite.scala` create calls.
  - `test_ContentUnderstanding.py` L1-120 and L520-700.
- [ ] No builds or tests were executed (read-only tools). The validation evidence in the prompt was taken as supplied, not reproduced.
- [ ] The Recovery, Concurrency and Fuzzing suites and the Stub were not read exhaustively, only spot-checked for the findings.
- [ ] No live CU calls or fresh remote-base check (external limits). This is not a code defect.

## Issues

### Issue 1: A 401/403 with no credential, or with a non-standard auth header, never stops the partition
- **Severity**: Low
- **File**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala` (also `ContentUnderstanding.scala` and the CU docs)
- **Line(s)**: Protocol L491-505, L508-521; ContentUnderstanding.scala L118-130, L136-138; docs L224-227, L303-306
- **Description**:
  - `isSharedAuthFailure` returns true only when `scalarCredentialWasSent`. That requires a sent header that matches `Settings.scalarAuthFingerprints`.
  - Those fingerprints are built only from resolved scalar `Authorization` and `Ocp-Apim-Subscription-Key` headers.
  - Nothing requires a credential to be configured, and the Fabric fallback is disabled. So in two configurations the fingerprint set is empty:
    - no credential is configured at all;
    - the only auth is a non-standard header in scalar `customHeaders`, such as a gateway key.
  - In those cases every row sends identical auth-related headers, so a definitive 401/403 before acceptance is necessarily shared. It is still classified as a row-level `Rejected`, and every later row in the partition is still POSTed.
  - The docs cover only two cases:
    - the stop applies "when the effective scalar credential was sent";
    - a row-level 401/403 happens "when authentication is configured per row".

    The unauthenticated case falls between them and conflicts with the contract's rule that "shared definitive pre-acceptance failures ... stop new submissions in that partition".
- **Risk**: A common misconfiguration, such as a forgotten key or a key under a custom header, sends one rejected POST per row in every partition. On large tables that can mean millions of requests. This can drive throttling on a resource shared with other workloads, plus log noise, which is exactly what the shared stop is meant to prevent. No handles are lost, hence Low.
- **Suggested Fix**:
  - Also treat 401/403 as shared when every auth-capable parameter (`authParams`: subscriptionKey, AADToken, CustomAuthHeader, customHeaders, telemHeaders) is scalar or unset.
    - Compute an `allAuthScalar` flag in `settings()` and use `(!rowCredentialWasSelected && (scalarCredentialWasSent || settings.allAuthScalar))`.
    - Keep failures row-level whenever any of these parameters is bound to a column.
  - Add stub tests:
    - No credential, concurrency 1, two rows: the first returns 401, the second returns `Rejected` / `SharedConfigurationFailure`, and the server sees exactly one request.
    - The same flow with a scalar non-standard `customHeaders` key.
  - Alternatively, explicitly document that unauthenticated configurations are not stopped.

### Issue 2: `createAnalyzer` silently targets the default `prebuilt-read` when analyzerId is unset
- **Severity**: Low
- **File**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstanding.scala`; `ContentUnderstandingParams.scala`
- **Line(s)**: ContentUnderstanding.scala L420-428, L430-434, L439-450; ContentUnderstandingParams.scala L127-128
- **Description**:
  - `createAnalyzer` builds `PUT .../analyzers/$getAnalyzerId?allowReplace=...`, and the follow-up GET uses the same ID.
  - `getAnalyzerId` falls back to the transform default `analyzerId -> Left("prebuilt-read")`.
  - `validateManagement` checks only that it runs on the driver, that parameters are scalar, and the ID's path-segment syntax.
  - So `new ContentUnderstanding().setEndpoint(...).setSubscriptionKey(...).createAnalyzer(defn, allowReplace = true)` sends a mutating create/replace request against the prebuilt analyzer ID.
  - The docs example always calls `setAnalyzerId` first (L418), and every create test sets it, so this path is untested.
- **Risk**:
  - The contract requires explicit provisioning, but a default target ID on a mutating PUT, possibly with `allowReplace=true`, is implicit.
  - The likely result is a confusing service rejection after the full definition has been sent.
  - If the service ever accepted writes to that ID, or the default changed, the call could try to replace an analyzer that other jobs share.
  - Low because the service is expected to reserve `prebuilt-*` IDs.
- **Suggested Fix**:
  - In `createAnalyzer`, add `require(isSet(analyzerId), "createAnalyzer requires an explicit analyzerId.")` before any HTTP request.
  - Optionally reject `prebuilt-` IDs for creation.
  - Keep `getAnalyzer`'s default behavior.
  - Add a test asserting `IllegalArgumentException` and zero server requests when analyzerId is unset.

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

### Driver triage and implementation (2026-11-05)
- **Issue 1: Confirmed and fixed locally; validation in progress.** CU settings now record whether all five auth-capable parameters are scalar or unset. Definitive 401/403 submission failures with no selected row credential stop the partition when either a known scalar credential was sent or this new condition holds. Existing effective-credential precedence and accepted-handle/polling behavior remain unchanged.
- **Coverage**: Added 401/403 cases with absent, nonstandard custom-header and telemetry-header authentication; each checks one POST for two rows, the triggering raw body/status, no handle, and the skipped row. Added populated/empty column cases for all five auth-capable parameters, proving that potential row-specific authentication remains row-level when no scalar credential was sent.
- **Issue 2: Confirmed and fixed locally; validation in progress.** `createAnalyzer` requires `isSet(analyzerId)` after scalar/driver validation and before request construction. No optional prebuilt-ID blacklist was introduced. `getAnalyzer` retains its default behavior. Scala and generated-Python regressions cover unset and cleared IDs with both replacement settings, zero mutating requests, and successful default retrieval. Existing explicitly named create tests remain required.
- **Red evidence**: Three of four new Scala admission tests failed for expected pre-fix behavior; column-auth preservation passed. Python rejected none of the four unset/cleared cases and recorded four PUTs, confirming the same gap through the public wrapper. Session logs: `cu-attempt20-scala-red.log` and `cu-attempt20-python-red.log`.
- **Review audit/runtime**: 81 read/search calls, no forbidden tools or prior/sibling feedback reads detected. Canonical full-input hash `ba542f91bec19506a0c21f33501d9bf568061eb058834cdc3e5c10ea4c390722`. Completed in 1433.377 seconds, with 60 model calls and only one exact repeated tool request; no runtime outlier. Original findings and verdict remain unchanged.

### Driver validation completed
- **Both issues**: Fixed and locally validated. All 113 CU Scala tests and 25 Python tests passed, including all new admission cases, existing scalar/column credential precedence tests, accepted/in-flight handle preservation, and explicit analyzer creation. No other test expectation was weakened or changed.
- **Required checks**: Cognitive main/test style and compilation, codegen/publication, actual loaded-JAR fingerprints, Black (205 unchanged) and diff whitespace checks passed. The separate admission suite keeps existing near-800-line suites within style limits and is included by the existing package-level CI test selector.
- **Artifacts**: Session `cu-attempt20-validation.log`, `cu-attempt20-python-green.log`, and the preserved red logs. Cognitive SHA256 `e3584edd245a601a5519f55d64ad9b417fc395a861fac15d1d218aa31ffc3076`; core SHA256 `8bd5e1e05d73a81831a6795d00597110a0f8133905295702e78a9f1d0054a05e`. Independent re-review remains pending.