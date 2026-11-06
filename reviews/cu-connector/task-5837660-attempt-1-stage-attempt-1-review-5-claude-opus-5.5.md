---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 1
round: 5
timestamp: "2026-11-05T20:39:23Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "038121e1db0dfe301c063b30a9d61337490e6a6d56dd302388cd9ac383e157dd"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-1-review-5-claude-opus-5.5.md
- **Issues Found**: 5
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read `ContentUnderstandingProtocol.scala` lines 500-575. `checkSharedFailure` stops the partition only when `!acceptedHandle && status != "Unknown"`. `safeSubmission` keeps a location only when `AcceptedSubmissionStatuses` (200/201/202) applies and the handle validates.
- [x] Searched the CU test suites (`ContentUnderstandingSuite`, `ContentUnderstandingRecoverySuite`, `ContentUnderstandingAdmissionSuite`, `ContentUnderstandingConcurrencySuite`, `ContentUnderstandingFuzzingSuite`, `ContentUnderstandingOutputParserFuzzingSuite`, `ContentUnderstandingStub`) for `Operation-Location` and `SC_CREATED`. Every analysis-submission fixture with a valid handle uses 202 `Accepted`. 201 appears only for analyzer PUT.
- [x] Searched the CU tests for `implicit-fabric-auth|RowAuthColumnMarker|FabricAuthMarker` and found no matches. The marker is added at `ContentUnderstanding.scala:335` and stripped generically in core `HTTPSchema.scala:209` (`toHTTPCore`). `ContentUnderstandingProtocol.scala:371` uses `toHTTPCore`.
- [x] Confirmed that every `pyAdditionalMethods` override calls `super` (`CognitiveServiceBase.scala:284/636/679`, `ContentUnderstandingParams.scala:214`, `AzureMapsTraits.scala:23`, `OpenAIPrompt.scala:168`), so the Wrappable `isSet` stub is inherited. No finding.
- [x] Reviewed `ContentUnderstandingConcurrencySuite.scala:124-139`, the deep-error test. Its assertions accept either of two statuses.
- [x] Reviewed `ContentUnderstandingSuite.scala:377`. The Retry-After test covers seconds, past HTTP dates and malformed values. 19-digit saturation and 200-digit fallback are covered elsewhere in the ConcurrencySuite.
- [x] Confirmed coverage in these areas: shared-stop partition isolation, Unknown/404 not tripping the stop, lookalike-message suppression, the ResponseBodyReadFailed/ResponseTooLarge matrix, the retry/expiry diagnostic-precedence matrix (poll/analyze/get/create), DNS capacity acquire/release, creation status mapping, explicit-analyzerId persistence (Scala and Python), Dataset resolver sessions, JSON bounds (depth 1001, 101-digit numbers), the HTTP 203/206 diagnostic matrix, the core serializer explicit/default split, and the Python pending/JVM lifecycle.
- [ ] Test execution not performed. This review is read-only. The 144/144 Scala result and the snapshot25 Python results are the caller's claims and were not verified independently.

## Issues

### Issue 1: Terminal 200/201 analysis submissions with a valid handle and a shared-failure code are untested
- **Severity**: Medium
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala; cognitive/src/test/scala/.../contentunderstanding/ContentUnderstandingAdmissionSuite.scala, ContentUnderstandingRecoverySuite.scala
- **Line(s)**: Protocol 102, 504-517, 547-561
- **Description**: The contract says an Accepted 200/201/202 submission with a validated handle stays accepted even if it is already terminal, and must not trip the shared stop. Every analysis-submission test with a valid handle uses the 202 fixture. No test sends a 200 or 201 POST that has a valid `Operation-Location` and a `Failed` body carrying `DeploymentNotFound`, `DefaultsNotSet` or `AnalyzerNotFound`. Such a test would assert that the handle is retained, that no `SharedConfigurationFailure` appears, and that later rows are still POSTed. HTTP 201 acceptance for analysis is not exercised at all.
- **Risk**: If someone changes `acceptedHandle` (for example, deriving it from the status instead of `operationLocation.isDefined`) or narrows `AcceptedSubmissionStatuses`, accepted handles could be wrongly discarded and the whole partition stopped. The current tests would not catch it.
- **Suggested Fix**: Add a parameterized public-transformer test over status ∈ {200, 201} × code ∈ {DeploymentNotFound, AnalyzerNotFound}, with scalar analyzerId and modelDeployments and at least two rows. Assert that the handle is retained, the status is `Failed`, there is no shared-stop diagnostic, and the recorded methods are `Seq("POST","POST")`.

### Issue 2: No CU test asserts that the row-auth provenance marker never reaches the wire
- **Severity**: Low
- **File**: cognitive/src/main/scala/.../contentunderstanding/ContentUnderstanding.scala; cognitive/src/test/scala/.../contentunderstanding/ContentUnderstandingRecoverySuite.scala
- **Line(s)**: ContentUnderstanding.scala 335; Protocol 106, 371, 494; RecoverySuite 488
- **Description**: CU adds `X-SynapseML-Implicit-Fabric-Auth: content-understanding-row-auth` to classify column auth, and relies on core `toHTTPCore` to strip it. Core tests cover stripping only for the generic Fabric `true` marker. No CU test checks the stub's recorded headers for this marker.
- **Risk**: If CU later builds its own `HttpRequestBase` or changes how headers are copied, the internal provenance header could leak to the service. The contract requires stripping provenance before transmission, and that regression would go unnoticed.
- **Suggested Fix**: In the column-auth tests (e.g. "column authentication failures remain row errors…" and the custom-header-column test), assert `server.requests.forall(r => !r.headers.contains("x-synapseml-implicit-fabric-auth"))`. The stub stores header names in lowercase.

### Issue 3: Future HTTP-date Retry-After is untested now that the cap has been removed
- **Severity**: Low
- **File**: cognitive/src/main/scala/.../contentunderstanding/ContentUnderstandingProtocol.scala; cognitive/src/test/scala/.../contentunderstanding/ContentUnderstandingSuite.scala
- **Line(s)**: Protocol 300+; Suite 377
- **Description**: The diff removes the old cap on HTTP-date delays. The contract says HTTP dates remain supported and valid long delays are not shortened. Tests cover only past dates (immediate retry) and numeric saturation. No test uses a future RFC-1123 date.
- **Risk**: A future-date regression would go undetected, whether it reintroduces a short cap (premature polling, more throttling) or parses the date as a fallback.
- **Suggested Fix**: Add a unit test of `retryDelay(Some(<now+120s RFC-1123>), fallback)` asserting a result ≥ ~119000 ms. Optionally add a public test with `maxOperationDuration` well below the delay, asserting `Running` + `OperationTimedOut` and no GET.

### Issue 4: The deep-error test accepts either of two outcomes, and the depth-1000 boundary is not pinned
- **Severity**: Low
- **File**: cognitive/src/test/scala/.../contentunderstanding/ContentUnderstandingConcurrencySuite.scala; ContentUnderstandingFuzzingSuite.scala
- **Line(s)**: ConcurrencySuite 124-139
- **Description**: The test asserts `Set("Failed","Unknown").contains(status)` and only that the error is non-empty. That cannot tell the bounded-decode path apart from the fallback path. Separately, tests reject depth 1001, but none shows that exactly 1000 nesting levels are accepted.
- **Risk**: An off-by-one in the depth limit, or a regression that silently switches decode paths (e.g. losing the service error code), would pass.
- **Suggested Fix**: Pin the expected status and the diagnostic code or error code for the 995-level body. Add a direct boundary test at 1000 (accepted) vs 1001 (rejected) through `ContentUnderstandingResponseParsing`.

### Issue 5: Column-auth detection through an embedded `Authorization` header is untested, and some tests depend on wall-clock timing
- **Severity**: Low
- **File**: cognitive/src/main/scala/.../contentunderstanding/ContentUnderstanding.scala; cognitive/src/test/scala/.../contentunderstanding/ContentUnderstandingRecoverySuite.scala, ContentUnderstandingAdmissionSuite.scala
- **Line(s)**: ContentUnderstanding.scala 154-163
- **Description**: `rowUsesColumnAuth` checks both `subscriptionKeyHeaderName` and `aadHeaderName` inside a `customHeaders` column. Tests cover only the embedded `Ocp-Apim-Subscription-Key`. Separately, several deadline tests assert elapsed-time bounds (e.g. less than 4000 ms with occupied abort workers) that depend on CI scheduling.
- **Risk**: A regression on the `Authorization` branch could turn row-level 401/403 into a partition-wide stop. Timing assertions may flake on loaded agents.
- **Suggested Fix**: Add an `Authorization` variant to the existing custom-header-column test. Where possible, prefer assertions on the outcome (status, diagnostic, request count) or looser bounds over tight wall-clock limits.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Addressed.
- **What changed**: New SubmissionSuite matrix covers accepted POST HTTP 200/201/202 with DeploymentNotFound and AnalyzerNotFound terminal bodies, scalar analyzer/model configuration and a later successful row.
- **Why**: Assert the retained handle/body/status/service error, absence of SharedConfigurationFailure, and two POSTs without a GET.
- **How verified**: All seven new suite tests passed in `cu-stage5-coverage-validation-retry2.log`.

### Issue 2
- **Status**: Addressed.
- **What changed**: Column-AAD and embedded-header matrices assert that every recorded request lacks the internal provenance header while retaining the exact effective credential.
- **Why**: Exercise CU's public wire path rather than relying only on generic HTTP tests.
- **How verified**: Passing SubmissionSuite assertions for 401/403 and subsequent successful rows.

### Issue 3
- **Status**: Addressed.
- **What changed**: Added a future RFC-1123 date ten years ahead, direct uncapped-delay assertion and public analyze/poll cases with a shorter operation budget.
- **Why**: Assert retained state/body/handle and OperationTimedOut with one POST or GET, without actually sleeping for the delay.
- **How verified**: Passing SubmissionSuite test; duplicates Grok issue 3.

### Issue 4
- **Status**: Partially addressed; exact-depth acceptance recommendation retained unresolved.
- **What changed**: The deep-response assertion now accepts only the service InvalidContent diagnostic with Failed, or the deliberate InvalidResponse fallback. Arbitrary non-empty errors no longer pass.
- **Why**: `asObject` and `compact` deliberately catch StackOverflowError from recursive spray-json processing. Depth 1000 is a scanner ceiling, not a guarantee that every JVM stack can decode/compact every body at that depth. Pinning one outcome or forcing acceptance would contradict that safety behavior. No parser seam/refactor was introduced merely for testing.
- **How verified**: The strengthened existing test passed in `cu-stage5-coverage-validation-retry2.log`. Exact scanner-boundary coverage remains unadded rather than claimed complete.

### Issue 5
- **Status**: Authorization coverage addressed; timing-relaxation suggestion not adopted.
- **What changed**: Added embedded Authorization and Ocp-Apim-Subscription-Key column variants over 401/403, effective-header assertions, row isolation and marker stripping.
- **Why**: Cover the real public behavior without changing auth selection. Existing deadline bounds were not loosened solely on a hypothetical scheduling concern; such changes need failure evidence and must preserve the bounded-deadline guarantee.
- **How verified**: Passing SubmissionSuite matrix. No timing thresholds changed; keep that sub-recommendation visible as unimplemented.