---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 9
round: 1
timestamp: "2026-11-04T08:42:54Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "c262dbca6b1ba15e03df26634e41d47c3f0a67590ed69578dc0fc3d547668f49"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\attempt9-opus-retry\task-5837660-attempt-1-stage-attempt-9-review-1-claude-opus-5.5.md
- **Issues Found**: 6
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read all of `cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstanding.scala` (lines 1–481). Covered the `transform` override, `settings`, `rowUsesColumnAuth`, `validateRow`/`rowError`, `prepareRequest`, `buildInternalTransformer` and `ContentUnderstandingOutputParser`.
- [x] Read `ContentUnderstandingProtocol.scala` lines 1–770. Covered `Settings`, `WireResponse`, `Budget`, `DeadlineControl`, `exchangeBeforeDeadline`, `decode`, `FailureClassifier`, `checkSharedFailure`, submission and poll decoding, `execute`/`rowHandler`, and analyzer management.
- [x] Read `ContentUnderstandingDnsResolver.scala`, `ContentUnderstandingHeaderValidation.scala` and the `ContentUnderstandingPython` codegen overrides in `ContentUnderstandingParams.scala` (lines 100–275).
- [x] Checked the shared code the diff relies on:
  - `CognitiveServiceBase.scala`: `ServiceAuthHeaders.resolve`/`addContentType` (lines 362–436), `addHeaders` (549–557), base `transform` (765–769).
  - `HTTPSchema.scala`: `toHTTPCore` strips the auth marker header (lines 166–222).
  - `HTTPTransformer.scala` and `SimpleHTTPTransformer.scala`: handler, async wait and `newCol` flow.
  - `DatasetExtensions.scala`: lines 45–77.
  - `Wrappable.scala`: ServiceParam setter `pop` (lines 185–210) and `pyInitFunc` (456–480).
  - `Utils.py` `ComplexParamsMixin`: lines 55–118.
- [x] Checked the docs (`docs\Explore Algorithms\AI Services\Content Understanding.md` lines 60–330) against the code for auth-failure, `Rejected`/`Unknown`, header and `concurrentTimeout` behavior.
- [x] Checked tests: `ContentUnderstandingRecoverySuite.scala` (resolver test at lines 158–175, shared-failure tests at 220–290, DNS test at about 687), the `pipeline.yaml` leg at lines 818–822, and the Python E2E `test_missing_shared_analyzer_fails_the_action` (lines 190–194).
- [ ] No commands, builds or tests were run by this reviewer; the review was read-only. The validation results I relied on came from the caller: style, compile, test:compile, codegen, Scala 35/35 and 8/8, and Black.
- [ ] No evidence of Python unit or E2E tests. Per the saved decision they were not run, so the generated-wrapper overrides and the core `Wrappable` setter change have no executed Python evidence.

## Issues

### Issue 1: `transform` temporarily changes the shared session's `spark.sql.caseSensitive`
- **Severity**: Medium
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstanding.scala
- **Line(s)**: 59–72
- **Description**: `transform` gets the active session's conf from `SQLConf.get` and calls `setConf(SQLConf.CASE_SENSITIVE, …)` on it, then writes back the saved value in `finally`. This conf belongs to the whole SparkSession, not to the thread. While the transform is running:
  - every other thread or notebook cell using that session sees the changed setting;
  - the `finally` block writes back the saved value, which can overwrite a change another thread made in the meantime;
  - a setting that was never set before becomes explicitly set.

  If the dataset belongs to a different session from the active one, the code changes the active session to match the dataset session. That is the case the new test covers. The change exists so that nested helpers which call `SQLConf.get.resolver` (for example `SimpleHTTPTransformer.newCol`) use the dataset's resolver.
- **Risk**: Concurrent queries in the same session can be analyzed with the wrong case sensitivity. A user's concurrent `spark.conf.set("spark.sql.caseSensitive", …)` can be silently undone. Shared sessions in high-concurrency notebooks make this race realistic.
- **Suggested Fix**: Don't change session configuration. Make the dataset's session active only for the current thread instead:

  ```scala
  val prev = SparkSession.getActiveSession
  SparkSession.setActiveSession(dataset.sparkSession)
  try { ... } finally { prev.fold(SparkSession.clearActiveSession())(SparkSession.setActiveSession) }
  ```

  `SQLConf.get` then resolves to the dataset session's conf without touching any session. Alternatively, pass the resolver explicitly to every column-naming helper. Add a test that checks the active session's conf is never written.

### Issue 2: Shared analyzer/deployment failure detection relies on error codes that have only been tested against a stub
- **Severity**: Low
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala
- **Line(s)**: 529–541 (`checkSharedFailure`), 652
- **Description**: The action fails only if the error payload contains one of these literal codes:
  - `AnalyzerNotFound`
  - `DefaultsNotSet`
  - `DeploymentNotFound`
  - `ModelDeploymentNotFound`

  These codes appear only in the stubs used by `ContentUnderstandingRecoverySuite`. The live check, Python E2E `test_missing_shared_analyzer_fails_the_action`, was not run. If the real service reports a missing analyzer with a different code (for example a generic `NotFound` with a different inner code), every row gets a row-level `Failed` result and the action does not fail. Also, an HTTP 404 on `POST …/analyzers/{id}:analyze` while `analyzerId` and `apiVersion` are both scalar is a shared condition whatever the error code, but it is not treated as one.
- **Risk**: The documented behavior that a shared analyzer or deployment misconfiguration fails the action may not hold against the live service. Jobs would then send every row to a missing analyzer and report row failures instead of failing fast.
- **Suggested Fix**: Confirm the codes against the CU REST spec or a recorded live response, and run the E2E test. Also treat a pre-acceptance HTTP 404 on the analyze POST as a shared analyzer failure when `settings.sharedAnalyzer` is true.

### Issue 3: DNS-failure message ("can be safely retried") conflicts with the `Unknown` status and is misleading during polling
- **Severity**: Low
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingDnsResolver.scala; ContentUnderstandingProtocol.scala
- **Line(s)**: DnsResolver 16–20; Protocol 411–416, 593–604
- **Description**: `DnsResolutionFailed` reports "DNS resolution failed before the request was sent; the operation can be safely retried". `exchangeBeforeDeadline` marks the response `transportFailure = true`, so `decode` and `uncertainSubmission` give it status `Unknown`. The docs say `Unknown` means "inspect the outcome before intentionally resubmitting". The two signals contradict each other.

  The polling case is worse. During analyze-mode polling, `retainPollState` keeps the accepted handle and `Running` status but attaches this error. A user can read "the operation can be safely retried" as permission to resubmit the analysis, which duplicates billable work, when they should resume polling.
- **Risk**: The guidance is inconsistent and can lead to duplicate billable submissions. Rows that provably were never sent also fall outside the documented `Rejected` resubmission workflow.
- **Suggested Fix**: Say which request failed, for example "no request reached the service", and put no resubmission advice in the shared exception message. Add the retain-handle wording on the GET path, or map a pre-send DNS failure on submission to a separate status or `Rejected`. Document DNS failures in the results and failures section.

### Issue 4: DNS lookup thread pool is unbounded and timed-out lookups keep their threads
- **Severity**: Low
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingDnsResolver.scala
- **Line(s)**: 30–33, 43–45, 60–67
- **Description**: `BudgetedDnsResolver` sends each lookup to `Executors.newCachedThreadPool`, which has no thread limit. When the budget runs out it calls `resolution.cancel(true)`, but the JVM's `InetAddress` resolution ignores interrupts. The thread therefore stays blocked until the OS resolver returns, while the next operation submits a new lookup. The JVM also makes concurrent lookups of the same host wait on each other. During a persistent DNS stall, executors build up blocked daemon threads in proportion to concurrency × partitions × retries.
- **Risk**: Thread and memory growth on executors during DNS outages. This is unlikely to cause wrong results.
- **Suggested Fix**: Use a bounded `ThreadPoolExecutor` with a small cap and a rejection policy that maps to `DnsResolutionFailed`. Or keep one in-flight future per host and reuse it.

### Issue 5: Docs leave `Content-Type` out of the custom header names the connector rejects
- **Severity**: Low
- **File**: docs\Explore Algorithms\AI Services\Content Understanding.md; ContentUnderstandingHeaderValidation.scala
- **Line(s)**: docs 90–92; validation 7, 18–19
- **Description**: `ConnectorManagedHeaders` rejects `content-length`, `content-type`, `host` and `transfer-encoding`, and `ContentUnderstandingRecoverySuite` line 542 relies on `content-type` being rejected. The docs list only `Host`, `Content-Length` and `Transfer-Encoding`.
- **Risk**: The docs don't match the code, so users get an unexpected "connector-managed header name" failure when they set `Content-Type`.
- **Suggested Fix**: Add `Content-Type` to the documented list.

### Issue 6: `pipeline.yaml` change has no recorded approval or CI evidence
- **Severity**: Low
- **File**: pipeline.yaml
- **Line(s)**: 821–822
- **Description**: The diff adds a `contentunderstanding` UnitTests leg (`PACKAGE: "services.contentunderstanding"`). The leg is needed because the suites moved out of `services.form`. However, AGENTS.md says to ask before changing `pipeline.yaml` and that such changes require CI evidence. The supplied validation context shows neither approval nor a pipeline run with this leg.
- **Risk**: The repository's process gate is skipped. If the leg's package pattern is wrong, the moved suites silently stop running in CI.
- **Suggested Fix**: Record the approval and attach an Azure pipeline (`/azp run`) result showing the new leg ran the `contentunderstanding` suites.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Resolved
- **What changed**: Removed shared `SQLConf` mutation; temporary names are checked case-insensitively and errors are assembled before output renaming.
- **Why**: Avoid cross-thread/session configuration races and case-insensitive column ambiguity.
- **How verified**: Cross-session resolver and case-variant schema regressions passed in `ContentUnderstandingRecoverySuite` (34/34) and `ContentUnderstandingSuite` (40/40).

### Issue 2
- **Status**: Resolved
- **What changed**: A shared analyzer analyze-POST HTTP 404 now fails the action independent of payload error code.
- **Why**: The action-level failure must not depend on a limited list of service error codes.
- **How verified**: The generic 404 regression passed in `ContentUnderstandingRecoverySuite` (34/34).

### Issue 3
- **Status**: Resolved
- **What changed**: Removed resubmission reassurance from DNS errors, classify pre-send DNS submission failures as `Rejected`, retain poll handles, and document the distinction.
- **Why**: Avoid suggesting a potentially billable duplicate submission and make the pre-send versus polling behavior explicit.
- **How verified**: DNS submission/polling regressions passed in `ContentUnderstandingRecoverySuite` (34/34); documentation updated and reviewed.

### Issue 4
- **Status**: Resolved
- **What changed**: Replaced the cached DNS pool with a bounded 32-thread executor and immediate overload rejection.
- **Why**: Blocking JVM DNS lookups can ignore interruption and must not grow worker threads without limit.
- **How verified**: `ContentUnderstandingConcurrencySuite` passed 2/2, including saturation coverage.

### Issue 5
- **Status**: Resolved
- **What changed**: Added `Content-Type` to the documented connector-managed headers.
- **Why**: Documentation must match case-insensitive header validation.
- **How verified**: Documentation checked against the validator and its regression test.

### Issue 6
- **Status**: Blocked
- **What changed**: No pipeline or ADO changes were made. The user declined authorization to queue Azure Pipeline validation.
- **Why**: Repository policy requires approval and CI evidence for the existing `pipeline.yaml` change.
- **How verified**: Approval was explicitly declined; no Azure validation was run. This finding remains unresolved and Max cannot advance.

#### Follow-up: 2026-11-04

- **Approval**: The user explicitly approved the local change: "please do the pipeline.yaml change".
- **Local change**: Retained the existing `contentunderstanding` UnitTests matrix entry with `PACKAGE: "services.contentunderstanding"`. No duplicate entry or unrelated pipeline change was added.
- **Local verification**: `core/testOnly com.microsoft.azure.synapse.ml.core.test.pipeline.PipelineTestCoverageSuite` passed 1/1 with JDK 11, confirming all discovered Scala test suites are claimed by the matrix.
- **Remaining gate**: The local edit approval is resolved. Azure validation was not authorized or queued, so external CI evidence remains outstanding.

## Driver Record

- **Decision**: Max, human-selected; stage 1, attempt 9. Change size is broad across Scala, Python, tests, docs, and pipeline configuration; complexity is high due to network, asynchronous polling, Spark session resolution, and concurrency interactions.
- **Reviewers**: GPT `gpt-6.1-sol` at xhigh; Grok `grok-4.7` at low per explicit human preference; Anthropic Opus `claude-opus-5.5` at xhigh. All three are distinct.
- **Stage outcome**: GPT and Grok found issues; corrected Opus retry found issues. The initial Opus dispatch was blocked because it received plan text instead of the diff; its original artifact is retained.
- **Resolution state**: All code findings above have documented fixes. The local pipeline change is now explicitly approved and its coverage guard passed. Issue 6 remains blocked on external Azure CI evidence; stage 1 is not clean and cannot advance.
- **Validation**: Style, compile, test compilation, and codegen passed; `ContentUnderstandingRecoverySuite` 34/34, `ContentUnderstandingConcurrencySuite` 2/2, `ContentUnderstandingSuite` 40/40, and `VerifyDatasetExtensions` 8/8 passed. Black passed (205 files unchanged). Python runtime tests and Azure validation remain unrun.