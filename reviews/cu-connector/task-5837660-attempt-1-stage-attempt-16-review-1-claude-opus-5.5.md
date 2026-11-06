---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 16
round: 1
timestamp: "2026-11-05T00:48:38Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "b64e3441b483ee3de2e1791174c2b2a8a82d78d198cce0dbc3d6b273d1960291"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-16-review-1-claude-opus-5.5.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Recovery classification (`ContentUnderstandingProtocol.scala`).** Traced `exchangeBeforeDeadline` (~374-418), `decode` (453-485), `checkSharedFailure` (522-535), `uncertainSubmission`/`decodeSubmission`/`safeSubmission` (537-579), `retainPollState`/`pollWithClient` (595-642), `execute` (644-670) and `rowHandler` (678-703). All of these match the spec:
  - A POST 401/403/429 with no handle is `Rejected`. A failed body read reports `ResponseBodyReadFailed`; an oversized body keeps `ResponseTooLarge`.
  - A contradictory handle gives `Unknown`.
  - A 404 stops the partition only when it is definitive. A 404 with a read-error diagnostic stays `Unknown`.
  - A missing or unsafe handle keeps `raw.diagnostic`.
  - Poll calls `checkSharedFailure(acceptedHandle = true)`, which never throws.
  - The last poll diagnostic takes precedence over the timeout and attempt-limit errors.
  - The shared-stop `innererror` is attached only to connector diagnostics, and only when `rawResponse` is empty.
- [x] **Operation-Location safety.** `validateAuthority` (~227-236) requires HTTPS, except for literal loopback, and rejects userinfo and fragments. `validateOperationLocation` (~264-282) requires the same origin, the CU path prefix, one safe segment and only an `api-version` query. `SubmissionLocation` (51-79) and `createAnalyzer` (747-800) never follow an unsafe handle or replay POST/PUT. Credentials therefore cannot be sent to another origin.
- [x] **Stop-state isolation.** The `AtomicBoolean` is created in `rowHandler` (680). `HTTPTransformer.clientHolder` (`HTTPTransformer.scala` 107-117, 129-140) is a `SharedVariable` with a `@transient lazy` instance (`SharedVariable.scala` 17-23), so each deserialized task gets its own stop state. Stop state is therefore isolated per partition and per action.
- [x] **Exception propagation.**
  - `HTTPClient.sendRequestWithContext` (`HTTPClients.scala` 63-67) and `AsyncHTTPClient.handle`/`SingleThreadedHTTPClient.handle` (442-457) do not catch exceptions.
  - `AsyncClient` runs futures on `ExecutionContext.global` (`Clients.scala` 55-61), and `AsyncUtils.bufferedAwait` rethrows failures (`AsyncUtils.scala` 11-19).
  - With no `concurrentTimeout`, which CU forbids, `HTTPTransformer` waits with `Duration.Inf` (110-116).
  - `ClientLifecycle.withClient` (`ContentUnderstandingProtocol.scala` 121-127) only uses `finally`.
- [x] **Parser and transport bounds.**
  - `build.sbt:55` pins spray-json 1.3.5.
  - `asObject` (439-448) catches only `JsonParser.ParsingException`, and `WireResponse.json` (156) is evaluated on every classification path.
  - `withClient` (420-432) sets no `ConnectionConfig`/`MessageConstraints`.
  - `readBounded`/`readResponseBody` (317-357) bound only the response entity.
  - `retryDelay` (308-315) parses Retry-After with `BigInt`.
  - The parameter defaults are `maxResponseBytes` 32 MiB and `maxOperationDuration` 600000 ms (`ContentUnderstandingParams.scala` 127-132, 150-152).
- [x] **Credential fingerprints and markers (`ContentUnderstanding.scala`).**
  - `settings` (~115-145) fingerprints only the scalar credentials that `ServiceAuthHeaders.resolve` selects.
  - `rowUsesColumnAuth` (~153-163) follows the key > AAD > custom-auth > embedded-header precedence.
  - `prepareRequest` (317-331) adds the row-auth marker.
  - `FailureClassifier` (`ContentUnderstandingProtocol.scala` 489-520) treats a 401/403 as shared only when the scalar credential was sent and no row credential was selected.
- [x] **Input planning and validation (`ContentUnderstanding.scala`).**
  - `activeParams`/`getVectorParamMap` (77-90) exclude inactive column bindings.
  - `validateParamSchema` requires an exact name, a single match under the resolver and the right type.
  - `validateRow`/`rowError` (259-284) turn invalid rows into row errors and send no request.
  - `buildInternalTransformer` (~387-416) checks output/error collisions with the Dataset resolver and picks case-insensitively unique internal columns.
  - `validateManagement` (~421-429) is driver-only and requires scalar values.
- [x] **Shared base change (`CognitiveServiceBase.scala`).** `addContentType` (358-361) only merges case-variant `Content-Type` entries; exact-case precedence is unchanged. The new `HasCustomHeaders` getters/col setters (226-230) match the generated wrappers.
- [x] **Python codegen.**
  - In `Wrappable.scala`: the base `isSet` (83-92), the ServiceParam setters that clear pending values (183-215) and the getters that prefer pending values (292-299) match the spec.
  - Stub generation (382-452), `DefaultParamInfo.pythonMethodArgumentType` (175-188), `PyCodegen.scala` 220 (ships `*.pyi` and `py.typed`) and `PyCodegenSuite` 218-245 were inspected.
  - All `pyAdditionalMethods` overrides call `super`.
  - `ContentUnderstandingSuite` 635-655 checks the generated source.
- [x] **Earlier reads in this review.** These are the reads recorded before this context's checks; they covered `ContentUnderstandingDnsResolver.scala`, `ContentUnderstandingTransport.scala`, `ContentUnderstandingHeaderValidation.scala` and the `Utils.py` `ComplexParamsMixin` scalar-view overrides. Conclusions:
  - DNS uses fair permits, and a permit is released only when the physical lookup finishes.
  - TLS completes before the write-started marker is set.
  - Headers are Latin-1 with no control characters except tab, token names are validated, and reserved names are blocked.
  - Bytes are never passed to pickle.
- [x] **Docs.** `docs\Explore Algorithms\AI Services\Content Understanding.md` 220-345 matches the code: shared stop, `Rejected` meanings, resumable selection by handle plus status, retry only from saved results, a best-effort duration, and the warning that there is no exactly-once guarantee.
- [ ] **Commands and tests.** Not executed; this review was read-only. The supplied local validation claims (98 CU tests, 22 offline Python tests, codegen) were not re-run independently.
- [ ] **Live CU and external Azure CI evidence.** Not available (declined or not authorized). This blocks a merge-ready claim but is not counted as a code defect.
- [ ] **Other reviewers' artifacts.** Not accessed, as required. Nothing under `reviews\cu-connector` was searched, listed or viewed.

## Issues

### Issue 1: Malformed JSON bodies can fail or hang the Spark task and lose accepted handles
- **Severity**: Medium
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala
- **Line(s)**: 156, 439-448, 453-455, 537-540, 565-572, 595-599, 678-703; related: build.sbt:55, core\...\io\http\Clients.scala 55-61, HTTPTransformer.scala 110-116, AsyncUtils.scala 11-19
- **Description**: Every classification path parses the response body through `WireResponse.json` → `asObject`. That includes `decode`, `uncertainSubmission`, `safeSubmission`, `retainPollState`, `analyzerBody` and `checkSharedFailure`. `asObject` catches only `JsonParser.ParsingException`.

  With spray-json 1.3.5, some bodies that are valid JSON grammar raise other errors:
  - An exponent that is out of range, such as `{"status":"Running","n":1e9999999999}`, throws `NumberFormatException` from `java.math.BigDecimal`.
  - Deep nesting of roughly 10^4–10^5 levels throws `StackOverflowError`. That is a few hundred KB at most, far below the 32 MiB `maxResponseBytes` default, and 1.3.5 has no depth limit.
  - A numeric literal of several MB triggers BigDecimal/BigInteger construction that grows faster than linearly, and the deadline abort cannot interrupt it.

  `rowHandler` catches only `ContentUnderstandingException`, and the HTTP client layer does not catch anything. As a result:
  - `NumberFormatException` fails the Spark task, either directly when concurrency is 1 or through `bufferedAwait` rethrowing it.
  - `StackOverflowError` is not `NonFatal`. In the synchronous path, Spark's executor may treat it as fatal and stop the JVM. In the async path, a Scala 2.12 `Future` is never completed after a fatal throwable. Because CU forbids `concurrentTimeout`, the await uses `Duration.Inf`, so the task can hang indefinitely.

  Poll bodies and accepted 202 submission bodies are parsed too, so this also happens after the service has accepted work.
- **Risk**: A response from a broken gateway, a compromised or misconfigured endpoint, or a service defect can do any of the following:
  - Discard the handles for every accepted row in the partition, which conflicts with `preserve_handles`. The Spark task retry then re-POSTs every document in the partition, causing duplicate billable work and an automatic POST replay.
  - Hang a task with no time limit.
  - Crash an executor, which also affects unrelated tasks.

  This is unlikely against the real service, but the impact is high and it bypasses the carefully bounded recovery classification.
- **Suggested Fix**:
  1. Make decoding total and bounded. Before `parseJson`, run a linear, non-recursive pre-scan that rejects nesting depth above a small limit (for example 512) and numeric tokens longer than about 100 characters, matching spray-json 1.3.6's `maxDepth`/`maxNumberCharacters`. Treat a rejected body as "not a JSON object", so the existing `InvalidResponse`/`Unknown` paths run and keep the handle.
  2. Widen `asObject` to catch `NonFatal`, and also `StackOverflowError` as extra protection.
  3. Optionally, add a final `NonFatal` guard in `rowHandler` that returns an `Unknown` row with a connector diagnostic instead of throwing.

  Upgrading spray-json would also help, but changing a dependency pin requires approval. Add regression tests for:
  - A 202 with a valid handle and an overflowing exponent (expect `Unknown`, handle polled or kept).
  - A deeply nested poll body (expect the retained state plus a diagnostic, and a successful task).
  - A mixed partition, showing that sibling handles survive.

### Issue 2: Response status/header parsing is unbounded, and Retry-After is parsed with `BigInt`
- **Severity**: Low
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala
- **Line(s)**: 308-315, 374-389, 420-432
- **Description**: `withClient` builds `HttpClients.custom()` without `setDefaultConnectionConfig`. The connection manager therefore uses `ConnectionConfig.DEFAULT`, whose `MessageConstraints` allow unlimited line length and header count. `maxResponseBytes` applies only to the response entity in `readBounded`. HttpCore buffers the status line and headers without any limit before `client.execute` returns. The only limits are the per-read socket timeout and the deadline abort, which defaults to 600000 ms.

  `retryDelay` also runs `Try(BigInt(v.trim))` on a Retry-After value of any length. That parse grows faster than linearly with the number of digits and runs on the task thread without interrupt checks.
- **Risk**: This is a defense-in-depth gap. The connector advertises bounded response memory, but a same-origin peer that keeps streaming header bytes, or sends a huge Retry-After, can exhaust executor memory or CPU. It is unlikely with the genuine HTTPS endpoint.
- **Suggested Fix**: In `withClient`, add:

  `.setDefaultConnectionConfig(ConnectionConfig.custom().setMessageConstraints(MessageConstraints.custom().setMaxLineLength(8192..65536).setMaxHeaderCount(~100..200).build()).build())`

  The resulting `MessageConstraintException` is an `IOException`. Because the status code is still 0 and the write has started, it falls into the existing ambiguous post-write classification. Before parsing Retry-After, trim it and reject values that are not short digit strings (for example, more than 19 characters), then parse with saturation. Add tests for an oversized header line and a very long Retry-After.

### Issue 3: Generated `.pyi` stubs declare `isSet(self, param: Param) -> Any` for every wrapper
- **Severity**: Low
- **File**: core\src\main\scala\com\microsoft\azure\synapse\ml\codegen\Wrappable.scala
- **Line(s)**: 83-92, 382-409, 411-418, 423-432, 434-452; related: DefaultParamInfo.scala 175-188, PyCodegen.scala 220
- **Description**: The base `pyAdditionalMethods` now adds an unannotated `def isSet(self, param):` to every generated wrapper. `pyStubAdditionalMethods` turns it into a stub line:
  - The argument type is inferred from the JVM `Params.isSet(Param[_])`, giving `Param`.
  - The missing return annotation defaults to `Any`.

  Every `.pyi` file (shipped with `py.typed`) therefore overrides PySpark's `isSet(self, param: Union[str, Param[Any]]) -> bool`. At runtime, strings still work through `_resolveParam`.
- **Risk**: This is a typing regression across all generated wrappers. Type checkers reject valid calls such as `stage.isSet("outputCol")` and lose the `bool` result, and strict checkers may flag an incompatible override. There is no runtime effect.
- **Suggested Fix**: Do one of the following:
  - Emit `def isSet(self, param) -> bool:` and type the stub argument as `Union[str, Param]`.
  - Leave `isSet` out of `pyStubAdditionalMethods` so the inherited PySpark signature applies.

  Add a `PyCodegenSuite` assertion on the generated stub line.

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

## Driver resolution update: response-boundary pass

The original review and its initial status entries above are preserved. All three
findings are now resolved locally, pending the next independent stage-1 review.

### Issue 1: bounded decoding across the complete response path

- Added package-private `ContentUnderstandingResponseParsing`: a linear,
  string/escape-aware scan limits JSON depth to 1,000 and numeric tokens to
  100 characters before spray-json 1.3.5. Scoped parsing and rendering guards
  catch `NonFatal` and `StackOverflowError`, not unrelated fatal errors.
- Reused the bounded object parser for submission, polling, analyzer GET/PUT
  responses and user-supplied analyzer definitions. Invalid definitions fail
  before provisioning. No dependency pins changed, as requested by the human.
- Replaced recursive error-code inspection with compiler-verified tail recursion
  over an explicit pending list. Failed service-error rendering produces an
  `InvalidResponse` diagnostic instead of silently dropping the error.
- Kept the shallow connector-generated output envelope unchanged: raw service
  JSON is escaped string data there. No blanket row-handler catch was added;
  decoding continues through the existing handle-aware recovery paths.
- Regression coverage includes overflowing exponents, over-limit depth and
  numeric tokens, escaped strings, a 100-character valid number, deep service
  errors, and mixed valid/invalid submissions at concurrency 1 and 2. Polling
  retains prior state/raw response/handle. Management failures retain raw
  responses without replay, while accepted PUTs with valid handles can recover
  through the management operation and final analyzer GET.

### Issue 2: bounded HTTP metadata and retry conversion

- Applied shared CU HTTP connection constraints: 64-KiB status/header lines and
  200 headers. Header parsing failures preserve the existing ambiguous
  post-write classification and do not replay POST.
- Numeric Retry-After accepts at most 19 ASCII digits before bounded conversion;
  supported large values retain saturating milliseconds. Overlong/malformed
  numeric values use the configured fallback; HTTP dates remain supported.
- Updated the saturation regression to a supported 19-digit value and added
  oversized-line, excessive-header-count and 200-digit Retry-After cases.
- Documented all accepted limits and recovery semantics in the CU guide.

### Issue 3: inherited Python isSet typing

- Excluded `isSet` from generated additional stub methods, preserving inherited
  PySpark typing while retaining the generated runtime override.
- The generated-wrapper regression checks both sides of that contract.
  Regenerated CU and `_ContentUnderstandingOutputParser` files were also
  inspected: neither stub declares `isSet`, and both runtime files retain it.

### Verification evidence

- `cu-attempt16-scala-red.log`: all three selected pre-fix regressions failed:
  the exponent failed the Spark task, oversized headers were accepted, and the
  overlong retry value did not fall back.
- `cu-attempt16-fixes-validation.log`: core main/test style passed; initial
  helper style findings were corrected without suppressions.
- `cu-attempt16-fixes-validation2.log`: cognitive main/test style, affected
  compilation, 1/1 generated-wrapper test, 108/108 CU tests, core/cognitive
  codegen and local publication passed.
- `cu-attempt16-fixes-final.log`: after the explicit rendering diagnostic and
  asynchronous coverage follow-up, cognitive style/compilation, all 108 CU
  tests (zero failed/skipped) and local publication passed again.
- `cu-attempt16-python.log`: 22/22 offline Python tests passed with actual loaded
  JAR fingerprints matching the rebuilt artifacts.
- `cu-attempt16-artifact-check2.log`: built/published JARs match; generated
  `Utils.py` is byte-identical to source; runtime/stub checks pass. The initial
  auxiliary inspection used the wrong non-underscored parser filename; it was
  corrected, without editing generated files.
- `cu-attempt16-black.log`: all 205 files unchanged.
- Core SHA-256: `8bd5e1e05d73a81831a6795d00597110a0f8133905295702e78a9f1d0054a05e`.
- Cognitive SHA-256: `5c2b5be742f5cd03b28724d712e43ba3269b2feaec47297ce624ac3ff4722b4d`.
- No live CU calls, external Azure CI, dependency changes, commit, push or ADO
  edits. Stage 1 remains open until the fresh required reviewer set clears.

## Driver decision: stage 1, attempt 17

| Field | Effective decision |
|---|---|
| Tier/source | Max/direct, driving-human selection; ordered stage/all-clean gate from review-code |
| Change size | Broad: 32 source diff paths, 378551 bytes |
| Complexity | High: distributed HTTP/recovery, concurrent state, schema resolution, shared Python/runtime typing |
| Rationale | Whole-response-boundary fixes and regression matrix are locally green; require a fresh independent broad sweep |
| Primary model | `gpt-6-astra` |
| Reviewers | Current live task catalog: `gpt-6.1-sol/max`, `grok-4.7/low`, `claude-opus-5.5/max` |
| Override | Human Grok low reasoning; unavailable Grok can be omitted only if GPT and Opus clear |
| Execution | Three independent script-owned Execute invocations, common frozen source patch, distinct session run directories |
| Bounds | Existing global controls unchanged; no new cycle/time/credit bound or safety bypass |
| Snapshot SHA-256 | `3bf71b04111e01922b446e0079606c2ea5409bd16a6caebf563773e9ccacde38` |

The two newly included diff paths are the response-parsing helper and the
generated-wrapper regression file. All eight content changes since attempt 16
are accounted for. Reviewer feedback and this driver section are excluded from
the source-only snapshot. No stage advancement is claimed.