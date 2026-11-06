---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 20
round: 1
timestamp: "2026-11-05T07:01:42Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "14c6da36f7929e9bb2c62e713a30ba0c6297fdaf5d9b7776e8f9b009aa58a7a0"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-20-review-1-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Authentication precedence and stop gating.** I traced `settings`, `isScalar`/`isColumn` and `rowUsesColumnAuth` in `ContentUnderstanding.scala` (~110–157) against `ServiceAuthHeaders.resolve` in `CognitiveServiceBase.scala` (376–436).
  - The precedence order is subscriptionKey > AADToken > CustomAuthHeader > embedded key > embedded Authorization.
  - telemHeaders and customHeaders cannot carry auth headers because those are stripped.
  - Scalar fingerprints come from scalar-only resolution. `allAuthScalar` covers all five auth-capable parameters.
  - The mixed scalar/column cases, the missing or nonstandard header cases, and the "column binding without a scalar credential stays row-level" cases all match the contract.
- **Row-auth marker cannot be spoofed and does not leak.**
  - `addHeaders` (`CognitiveServiceBase.scala` 549–563) removes marker names that come from users.
  - CU adds the marker after `addHeaders` (`ContentUnderstanding.scala` ~328–330).
  - `HTTPRequestData.toHTTPCore` strips the marker before sending (`HTTPSchema.scala:209`).
  - `usesFabricAuth` only applies when the marker value is `"true"` (`HTTPSchema.scala:169–173`), so the CU marker value cannot trigger Fabric MWC behavior.
- [x] **Shared stop and rejection classification** (`ContentUnderstandingProtocol.scala` 483–563 and 659–685):
  - 401/403/429 with no handle stays Rejected, including after ResponseBodyReadFailed or ResponseTooLarge.
  - A contradictory, unsafe or missing handle gives Unknown and keeps the original `raw.diagnostic` ahead of the assessment error.
  - A 404 with a body-read diagnostic gives Unknown, and Unknown never trips the stop.
  - The stop is a single `AtomicBoolean` per deserialized task handler, so it is partition-scoped.
  - Poll mode and accepted handles never trip the stop.
  - `innererror` is nested only when `rawResponse` is empty and the code is one of four connector-generated codes.
- [x] **Polling** (`ContentUnderstandingProtocol.scala` 580–651):
  - `retainPollState` keeps the prior status, raw response and handle on transient, diagnostic, missing-status or non-404/410 errors.
  - A retained error takes precedence over `timeoutError`. `PollAttemptsExhausted` is used only when no error exists.
  - When `delay >= remainingMs`, the loop stops without polling early.
  - Analyze mode resumes only from a validated handle.
- [x] **Transport** (`ContentUnderstandingProtocol.scala` 363–432):
  - Connect, connection-request and socket timeouts are capped at `min(timeoutMs, remaining)`.
  - A request counts as not sent only when no status was received and writing never started (`!hasStartedWriting`); DNS failures are always not-sent.
  - Each operation gets its own client with automatic retries and redirects disabled, so POST/PUT are never replayed.
  - When the budget has expired, the result is RequestNotSent.
- [x] **DNS admission** (`ContentUnderstandingDnsResolver.scala` 26–95):
  - The budget is checked before acquiring a permit and rechecked after; an expired caller releases its permit.
  - Workers release their permit in `finally`, even after the caller times out. RejectedExecution also releases the permit.
  - Stuck threads are not replaced, because the pool is fixed-size and FutureTask captures exceptions.
  - The regression test exists in `ContentUnderstandingAdmissionSuite.scala:17–39`.
  - The raw `getMessage` fallback in `diagnostic` is unreachable, because the resolver wraps every delegate failure.
- [x] **Endpoint and handle safety** (`ContentUnderstandingProtocol.scala` 210–288):
  - HTTPS is required, except for literal loopback addresses.
  - The endpoint path must be the resource root or the analyzers path, and no query is allowed.
  - operationLocation must be same-origin, have a safe path segment and carry exactly one api-version.
  - The analyzerId pattern rejects dot segments.
  - Column values are revalidated per row through `validateOptionValue` (`ContentUnderstanding.scala` 213–226), and invalid rows are nulled before `inputFunc`.
- [x] **Planning and schema** (`ContentUnderstanding.scala` 59–64, 232–244, 353–420):
  - `transform` uses `resolverFor(dataset)`; schema-only paths use `SQLConf.get.resolver`.
  - Input bindings require an exact top-level name and exactly one resolver match.
  - Output and error collision checks use the same resolver.
  - Internal column names are unique case-insensitively.
  - Setting concurrentTimeout is rejected, and the internal SimpleHTTPTransformer does not receive one (`SimpleHTTPTransformer.scala:133`).
  - The public `DatasetExtensions` signatures (51–75) are preserved; only overloads were added.
- [x] **Analyzer management** (`ContentUnderstanding.scala` 425–466):
  - Calls are driver-only and scalar-only.
  - `createAnalyzer` requires `isSet(analyzerId)` and validates the definition JSON before any HTTP call.
  - `getAnalyzer` still works with the default ID.
  - These match `ContentUnderstandingAdmissionSuite.scala:90–110`.
- [x] **Parsing boundary.** The output parser (`ContentUnderstanding.scala` 468–500) parses only the shallow connector envelope produced by `rowHandler`. Bounded parsing of service JSON stays in `ContentUnderstandingResponseParsing`.
- [x] **Shared base changes** (`CognitiveServiceBase.scala` 226–230 and 358–361):
  - The `addContentType` case-insensitive dedupe matches the earlier exact-case overwrite behavior.
  - The new `HasCustomHeaders` accessors are additive and do not conflict with the protected `getCustomHeaders(row)` overloads.
  - `FeatureNames.AiServices.ContentUnderstanding` exists (core `FeatureNames.scala:8`).
- [x] **Python and codegen.** `core/.../Utils.py` ComplexParamsMixin and `Wrappable.scala` (isSet stub 83–92, pyInitFunc ordering 466–491, stub skip at 437) match the generic Params, defaults, copy/load and bytes/map contract.
- [x] The CU docs page is consistent with the behavior: credential persistence and logging disclosure, best-effort duration, and selecting recovery rows by handle plus status.
- [ ] I did not run any tests. The 113 Scala tests, 25 Python tests and the build and style results are taken from the supplied validation evidence. I only spot-checked that the test sources exist for the claimed regressions.
- [ ] Live Azure Content Understanding calls and external Azure CI evidence are unavailable. Under the plan, that blocks merge-ready claims but is not a code defect.