---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 3
stage_attempt: 3
round: 1
timestamp: "2026-11-06T18:52:46Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "0d9c89cbc6f628dfa7e13de76edaa709dcdb7647bf5df056d2e676e6253a10d9"
---

I found 2 issues; verdict is ISSUES_FOUND. One is a cross-service credential-exposure widening (Medium) and the other a task-failure resubmission path (Low).

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML-cu-native\reviews\pr-2815\task-5837660-attempt-3-stage-attempt-3-review-1-claude-opus-5.5.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read the full supplied 37-file diff. It covers the CU transformer, protocol, transport, DNS, budget, parsing, header validation, logging, params and Python overrides; the shared `CognitiveServiceBase`, `Wrappable`, `DatasetExtensions`, `SimpleHTTPTransformer`, `ComplexParamsSerializer` and Python `Utils.py`; the Scala and Python tests; the docs; `pipeline.yaml`; and `website\sidebars.js`.
- [x] Checked `cognitive\src\main\scala\...\services\CognitiveServiceBase.scala` lines 330-436 (`ServiceAuthHeaders.resolve`). Credential precedence is subscriptionKey → AADToken → CustomAuthHeader → embedded `Ocp-Apim-Subscription-Key` → embedded `Authorization` → Fabric fallback. This matches `ContentUnderstanding.rowUsesColumnAuth`, so the row-credential provenance marker and the scalar fingerprints classify the selected credential consistently. The new `addContentType` removes case-variant user `Content-Type` headers for all services; this is consistent, not a regression.
- [x] Checked `CognitiveServiceBase.scala` lines 60-130 and 520-630: `getValueOpt`, `getValueAnyOpt`, `addHeaders`, `lacksExplicitAuthCredential`, `inputFunc`. CU overrides `getFabricFallbackAuthHeader` to `None` and `transferGlobalParamsToParamMap` to a no-op, so there is no OpenAI/Fabric credential fallback.
- [x] Checked `ContentUnderstandingProtocol.scala` lines 30-790:
  - Endpoint and authority validation: HTTPS only, HTTP only for literal loopback, no userinfo or fragment.
  - Operation-Location checks: same origin and effective port, expected prefix plus exactly one safe segment, and exactly one `api-version` query.
  - Submission decoding: not sent → Rejected; 401/403/429 without a handle → Rejected; ambiguous → Unknown. Running is never Succeeded.
  - The poll loop's `retainPollState` keeps the handle and the prior status on non-definitive GETs.
  - Shared-failure classification is skipped for accepted handles and Unknown.
  - Deadlines abort requests, response bodies are read with a bound, and the per-operation client has no retries or redirects.
  - Management create/get re-validates the analyzer operation path and requires the analyzer to be `ready`.
- [x] Checked `ContentUnderstanding.scala` lines 296-525: request body and URL building, snapshot pipeline (`copy(ParamMap.empty)`), temp-column reservation, validation-column nulling, error-column propagation, driver-only management (`TaskContext` check, scalar-only auth, explicit `isSet(analyzerId)`), and sanitized management logging (error class only).
- [x] Checked `core\src\main\python\synapse\ml\core\schema\Utils.py` lines 60-320 and the generated setters and getters in `core\src\main\scala\...\codegen\Wrappable.scala` lines 140-420 and 540-620.
  - Pending `_paramMap` values take precedence.
  - Column bindings stay on the JVM: `isSet`, `hasDefault` and `isDefined` return false, and `getOrDefault` raises `KeyError`.
  - Raw bytes are never passed to `_java2py`.
  - When a setter fails, the transfer does not pop the pending value.
  - A grep of the Python sources found no other `isSet` followed by `_paramMap[...]` that could hit a `KeyError`.
- [x] Checked the docs `docs\Explore Algorithms\AI Services\Content Understanding.md` (lines 240-372) and `docs\Use with MLFlow\Autologging.md` (lines 1-50) against the code: Rejected/Unknown semantics, shared stop, the retry-selection example and the `concurrentTimeout` rejection match `decodeSubmission`, `checkSharedFailure` and `rowHandler`.
- [x] Did not execute tests; the evidence listed in the review plan was considered, not reproduced. I did not read review or feedback directories, target outputs or Git metadata.

## Issues

### Issue 1: Python `extractParamMap` now reads JVM-only scalar credentials for every ServiceParam stage, so autologging can capture more secrets
- **Severity**: Medium
- **File**: core\src\main\python\synapse\ml\core\schema\Utils.py
- **Line(s)**: ~205-219 (`extractParamMap`); also `isSet` ~149-156 and `getOrDefault` ~195-203
- **Description**: The generated ServiceParam setters write directly to the JVM and pop `_paramMap`. The new `ComplexParamsMixin.extractParamMap` override then adds every JVM scalar ServiceParam value to the returned map. This includes `subscriptionKey`, `AADToken`, `CustomAuthHeader`, `customHeaders` and `telemHeaders`. Because `ComplexParamsMixin` is shared, the change affects every cognitive-service wrapper, not just Content Understanding. A credential set through `setSubscriptionKey(...)` lives only on the JVM, but it now appears in `extractParamMap()`. MLflow's pyspark.ml autologging records pipeline stage parameters through `extractParamMap()`. `explainParam` redacts these values, but `extractParamMap` returns them unredacted.
- **Risk**: Existing autologged pipelines that set keys or tokens through setters can start writing those credentials to MLflow tracking stores without any code change. The Autologging doc mentions the risk but does not stop it. The review scope also asks for impact on existing non-CU stages to be checked.
- **Suggested Fix**: Keep the scalar view for inspection, but skip the JVM augmentation in `extractParamMap` for the auth-bearing names that `explainParam` already redacts. Alternatively, insert a redaction sentinel for them, or enable the augmentation only for opted-in stages such as CU. Add a regression test that sets `subscriptionKey` through the setter and asserts the secret is absent from `extractParamMap()`.

### Issue 2: Non-IO runtime exceptions from `exchange` escape `rowHandler`, fail the task and can trigger automatic resubmission of accepted documents
- **Severity**: Low
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala
- **Line(s)**: ~365-415 (`exchangeBeforeDeadline` catch block), ~668-690 (`rowHandler`)
- **Description**: `exchangeBeforeDeadline` turns `ResponseTooLarge`, `UnknownHostException` and `IOException` into row-level `WireResponse` values. `rowHandler` catches only `ContentUnderstandingException`. Any other `RuntimeException` from `client.execute`, the custom request executor, or entity and header handling after the POST body has been written escapes the handler. Examples include an `IllegalStateException` from the connection or entity lifecycle, or a runtime exception from the custom transport. That fails the Spark task.
- **Risk**: Spark retries the failed task (`spark.task.maxFailures`) and reprocesses the whole partition. Rows whose submissions were already accepted are sent again, which creates duplicate billable analyses and loses their handles. This conflicts with the requirement that unknown acceptance is not safe to resubmit automatically.
- **Suggested Fix**: In `exchangeBeforeDeadline`, add a `NonFatal` case after the `IOException` case that excludes interruption: if the thread is interrupted, rethrow. Otherwise, abort the request and return a `transportFailure` `WireResponse` with `notSent` based on `hasStartedWriting`, so the row becomes `Unknown` or `Rejected` instead of failing the task. Add a focused test with a request executor that throws a `RuntimeException` after the write starts.

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

## Driver audit and preliminary triage

This is the human-authorized replacement for the failed Opus stage-attempt 2
slot, not a third full reviewer batch. Its actual input contains the complete
37-file snapshot 3 in Diff to Review and the exact replacement context.
The model, input hash and finalized receipt match. All 20 tool calls have
results and no feedback access was found. Two searches used exact changed
documentation paths rather than the requested view tool; they did not broaden
access beyond the supplied source. This tool-choice deviation remains visible.

**Issue 1 remains visible as a known design risk, not a regression from the
setParams correction.** Earlier review decisions already retained faithful
scalar parameter inspection and documented credential exposure in both the
CU guide and general Autologging guide. The public getters, parameter maps and
saved model metadata are deliberately not secret-redaction boundaries.

The suggested extractParamMap-only change does not prevent the cited MLflow
path. Independently rechecked canonical MLflow v2.21.3
`mlflow/pyspark/ml/__init__.py`: `_get_param_map` enumerates `instance.params`
and calls `isDefined` and `getOrDefault`; recursive pipeline collection uses
that helper. Redacting only extractParamMap would leave this exposure while
making generic parameter inspection inconsistent. No such change was made.
The broader risk is not declared eliminated by documentation.

**Issue 2 is unconfirmed.** The missing generic RuntimeException catch is
verified, but no concrete production trigger has been demonstrated. The
production client installs a fixed request executor; throwing from a synthetic
replacement would not establish reachability in that client. The CU guide
already distinguishes connector retry suppression from Spark's at-least-once
task execution and explicitly warns that unrelated task failures may replay
submissions. No blanket exception catch was added merely to hide unexpected
implementation defects. Retain the finding pending production-path evidence.

## Driver consolidated disposition (2026-11-06)

Post-review driver assessment; this does not rewrite the model verdict,
clear unresolved findings, or validate an invocation rejected by its audit.
Duplicate reports are not independent confirmations.

### s1t3-claude-opus-5.5-i1: Scalar credentials remain visible to generic logging

- Disposition: **known-risk**.
- Evidence: Documented in CU and Autologging guides. Pinned MLflow2.21.3 uses isDefined/getOrDefault, so extractParamMap-only redaction does not fix that logging path and breaks faithful inspection.
- Consolidated topic: `credential-logging`.

### s1t3-claude-opus-5.5-i2: Unexpected runtime exceptions can fail tasks

- Disposition: **unconfirmed**.
- Evidence: Selective catch verified, but no reachable production trigger demonstrated. Synthetic custom-executor failure is not reachability proof; do not blanket-catch programmer defects.
- Consolidated topic: `runtime-exceptions`.
