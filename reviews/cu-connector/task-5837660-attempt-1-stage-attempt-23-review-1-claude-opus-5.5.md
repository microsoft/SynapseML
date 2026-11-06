---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 23
round: 1
timestamp: "2026-11-05T15:57:18Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "3b28874194ebe6a8541a2f7a110dcde85e6d48278a0765ec6d8672ef83869ea5"
---

I found one Medium issue (verdict ISSUES_FOUND): an accepted analyzer creation can be reported as `Failed`. I did not execute any tests. The validation results and live CU/Azure CI evidence come from the plan; I didn't reproduce them.

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-23-review-1-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **`ContentUnderstanding.scala` (read in full):**
  - Direct `transform` uses the Dataset's own name resolver (`DatasetExtensions.resolverFor`) without changing SQL settings.
  - Schema-only planning uses the active session's resolver.
  - A configured `concurrentTimeout` is rejected.
  - Output/error name collisions are checked with the resolver.
  - Input bindings require an exact field name and exactly one resolver match.
  - `createAnalyzer` requires an explicitly set `analyzerId`; `getAnalyzer` can still read the default.
  - `settings()` builds scalar credential fingerprints and `allAuthScalar` correctly.
  - `rowUsesColumnAuth` follows the same credential precedence as `ServiceAuthHeaders.resolve`.
  - The output parser uses the Java `UDF1` API with an explicit schema (line 482), which works with legacy-UDF protection enabled.
- [x] **`ContentUnderstandingProtocol.scala`, shared stop and rejection handling:**
  - `rowHandler` creates its stop flag per task. A stop skips later unsent rows whatever their credential source. Poll mode never trips it.
  - The nested diagnostic is limited to four connector-generated codes and only when the raw response is empty.
  - The row-auth marker is stripped before sending.
  - 401/403/429 without a handle stay `Rejected`, with `ResponseBodyReadFailed` or `ResponseTooLarge`. A contradictory handle or a 404 with a body-read failure stays `Unknown` and does not stop the partition.
- [x] **Polling and management retention (`ContentUnderstandingProtocol.scala`):**
  - `retainPollState` (570-585) keeps prior state on transient failures.
  - Expiry at the top of the poll loop (601-603) keeps the prior error/status/body and adds `OperationTimedOut` only when there is no error.
  - `getAnalyzerResponse` (697-717) returns the previously dispatched failure when a retry expires before dispatch.
  - `finishCreation` (725-741) keeps `Succeeded` plus `OperationTimedOut` when the final GET expires before dispatch; a dispatched retrieval failure stays authoritative.
  - An unsafe creation handle does not replace an existing read/size diagnostic.
- [x] **DNS, budget, transport, response parsing and header validation:**
  - DNS: the budget is rechecked after acquiring capacity and expired work releases its permit. Submitted workers hold permits until the lookup finishes. Worker rejection is reported as `DnsCapacityExceeded` without counting as deadline expiry.
  - `Retry-After`: at most 19 digits with saturation, HTTP dates supported, long delays not shortened.
  - JSON: the scan is iterative and string/escape-aware, with depth ≤1000 and numeric tokens ≤100.
  - HTTP: `MessageConstraints` limit header lines to 64 KiB and headers to 200 per response.
- [x] **Shared core changes:**
  - `ComplexParamsSerializer.scala` (21-23, 44-45, 123-124): the explicit-only `paramMap` is opt-in and used only by CU. Complex params are still saved from `extractParamMap`, and `defaultParamMap` is restored as defaults.
  - `JsonEncodableParam.scala`: `ServiceParam` values round-trip as left/right JSON.
  - `DatasetExtensions.scala` (51-75): `findUnusedColumnName` now matches names with the resolver. This is more conservative for all callers.
  - `CognitiveServiceBase.scala` (226-230, 358-361): the new getters don't conflict with existing ones, and `Content-Type` is replaced case-insensitively.
  - `FeatureNames.scala:8` defines `ContentUnderstanding`.
- [x] **Python and codegen:**
  - `Wrappable.scala`: the base `isSet` override and getters prefer pending Python values; generated stubs skip `isSet`.
  - `Utils.py` `ComplexParamsMixin`: column bindings are absent from `hasDefault`/`isSet`/`isDefined`/`extractParamMap`, scalar `getOrDefault` raises `KeyError`, and `explainParam` redacts credentials.
  - The CU Python overrides in `ContentUnderstandingParams.scala` were reviewed against the UID/default/clear requirements.
- [x] **Docs (`docs\Explore Algorithms\AI Services\Content Understanding.md`, 225-470):**
  - Resumable rows are selected by handle plus `NotStarted`/`Running`/`Unknown` (268-280, 394-397).
  - Retries are selected only from saved results (336-353).
  - `concurrentTimeout` is documented as unsupported.
  - Creation-expiry wording is at 447-452.
- [x] **Tests (read only, not executed):**
  - `ContentUnderstandingAdmissionSuite.scala` 120-200: the creation-retention matrix covers only `Running` and `not-json` PUT bodies (139-142).
  - Real-shaped `creating` bodies appear in `ContentUnderstandingSuite.scala` 660-661, 740-741, 767-770 and `ContentUnderstandingRecoverySuite.scala` 267-268, but never together with expiry or transient-only polling.
- [ ] **Not done:** I did not reproduce the build/test/hash claims (134/134, 28/28, JAR hashes). The plan allows only view/grep/glob.
- [ ] **Unavailable:** live CU and Azure CI evidence. This is an external limit, not a code defect.

## Issues

### Issue 1: An accepted analyzer creation is reported as `Failed`/`InvalidResponse` when its first poll does not complete
- **Severity**: Medium
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala
- **Line(s)**: 441-454 (`decode` status and `missingStatusError`), 763-766 (`createAnalyzer` seeds `pollWithClient` from `decode` of the PUT), 579-581 (`retainPollState` keeps the previous status), 601-603 (expiry keeps the previous error); test gap at ContentUnderstandingAdmissionSuite.scala 138-161
- **Description**:
  - `createAnalyzer` builds the initial poll state with `decode(raw, Some(location))`. That decoder expects an operation object whose status is `NotStarted`, `Running`, `Succeeded`, `Failed` or `Canceled`.
  - The real create/replace PUT returns the analyzer resource with lifecycle status `creating`. The repository's own stubs model it the same way: `201 {"analyzerId":"custom","status":"creating"}` plus `Operation-Location`.
  - `decode` maps that body to status `Failed` with `InvalidResponse` ("Expected a JSON operation object with a recognized status").
  - On the normal path, the first successful GET overwrites this initial state, so the problem stays hidden. It surfaces in two cases:
    - **(a) The budget runs out before the first GET is sent.** Causes include a slow PUT, a small `maxOperationDuration`, or a `Retry-After` longer than the remaining budget. `pollWithClient` then returns the initial state unchanged; `retained.error.orElse(timeoutError)` keeps `InvalidResponse`. The thrown exception reads "returned Failed (HTTP 201)".
    - **(b) Every sent GET fails transiently (429/5xx/transport) until attempts or budget run out.** `retainPollState` copies only `httpStatus` and `error` onto the initial state, so the status is still `Failed`.
  - The clean `{"id":"create-1","status":"Running"}` body in the same test matrix yields `Running` plus `OperationTimedOut`. That is the intended rule: a clean prior state keeps its HTTP status and body and adds `OperationTimedOut`, and polling keeps the prior remote state.
  - The test matrix (Admission 139-142) does not cover the service's actual response shape.
- **Risk**:
  - For a remote creation that was accepted and is still running, the driver exception says the creation `Failed` with an invalid-response diagnostic.
  - A caller who trusts that status may call `createAnalyzer` again. With `allowReplace=true`, that replaces an analyzer that is still being created. The caller may also give up on a definition that is about to become ready.
  - This contradicts the documented creation-expiry behaviour (docs 447-452) and the acceptance rule for clean prior state.
- **Suggested Fix**:
  - Seed creation polling from the PUT in a way that understands analyzer resources. The case to cover is an accepted 200/201/202 with a validated handle, no transport/read/size diagnostic and no service `error`.
  - In that case, map `creating` (and a missing or unrecognized status) to an in-progress status such as `Running` with `error = None`, keeping `httpStatus`, the raw body and `id`.
  - Leave an explicit `failed` status and the existing malformed-body (`not-json`) policy unchanged.
  - Add regression tests to the Admission creation matrix:
    - Body `{"analyzerId":"custom","status":"creating"}` across 200/201/202, with expiry both before and during the first wait. Expect a non-`Failed` status, `OperationTimedOut`, raw body and handle kept, and only the PUT sent.
    - A transient first GET followed by attempt exhaustion or expiry. Expect the in-progress status with that GET's diagnostic.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Driver resolution: analyzer-resource creation state
- **Status**: Fixed and locally validated; no subsequent independent review has run.
- **Cause**: The attempt-23 fix introduced this regression by seeding polling with the operation decoder. Its new deadline fixtures used `Running`, while the real PUT returns a `ContentAnalyzer` with `creating`. Successful later polling masked the mismatch.
- **What changed**: A private creation-only status vocabulary maps `creating` to `Running`, `ready` to `Succeeded`, and `deleting` to `Unknown`; `failed` retains the existing failure handling. Only the validated initial creation-response decode receives this vocabulary. Analysis submissions, operation polls, management GETs, URL validation, retries, persistence, and shared-stop classification are unchanged.
- **Deliberate difference from the suggestion**: Missing and unknown statuses are not assumed to be healthy running operations. They retain `InvalidResponse`, as do malformed bodies. Existing service/read/size diagnostics retain precedence. `analyzerId` is not fabricated into an operation `id`.
- **Protocol source**: Microsoft Learn create-or-replace REST API, version 2025-11-01, documents the PUT response as `ContentAnalyzer` and its lifecycle as `creating`, `ready`, `deleting`, `failed`: https://learn.microsoft.com/en-us/rest/api/contentunderstanding/content-analyzers/create-or-replace?view=rest-contentunderstanding-2025-11-01.
- **Red evidence**: `cu-attempt24-scala-red.log`: two creation tests failed with `Failed` instead of `Running`; the analysis-vocabulary isolation control passed. `cu-attempt24-python-red.log`: the generated public wrapper reproduced the same failure against the previously validated JAR.
- **Green evidence**: `cu-attempt24-validation.log`: cognitive main/test scalastyle, compilation, all four CU suites (136/136), code generation, and local publication passed. `cu-attempt24-python-green.log`: 29/29 offline Python tests passed. `cu-attempt24-black.log`: 205 files unchanged. Built/published/actually loaded JARs matched: core `4890b2366afa1c796f2826358912142bce434d4fa2bfe60c9e65c00f67b15b6a`; cognitive `5945f669a6b4e84823741065a7067729fe6ed7c7a5a4af22a3d0ae047985a174`.
- **Pre-review scrutiny**: Traced creation through accepted PUT, first wait, operation polling, retry exhaustion, final retrieval, and exception exposure. The matrix covers HTTP 200/201/202; operation and resource shapes; every documented resource lifecycle state; explicit service error; malformed/missing/unknown status; expiry before and during the first wait; repeated 429/503; and no replay. Existing completed-creation/final-GET, successful creation, unsafe-handle, DNS, persistence, and wrapper controls remain green. Python tests use the generated wrapper and a local HTTP server, not a mocked decoder.

### Driver scope checkpoint before attempt 24
The user questioned both the number of stage-1 iterations and cumulative scope drift. No attempt-24 snapshot or reviewer was launched. This focused scope audit is not another independent correctness signoff.

The original confirmed contribution boundary in `cu-synapseml-connector-plan.md`, corroborated by session decisions, keeps the native transformer, analyze/submit/poll, URL/binary inputs, native options, explicit create/get, and normal parameter persistence. It excludes durable writers/journals, implicit provisioning, a separate SDK, and Fabric-managed integration. The current public capability boundary still follows that selection.

| Change group | Relation to agreed goal | Scope assessment |
| --- | --- | --- |
| Transformer, native request/response data, invalid-row isolation, create/get, removal of durable writers | Direct connector contribution | Retain |
| Handle retention, no submission replay, bounded HTTP/JSON/input handling, deadline and DNS lifecycle | Implements the agreed failure and resource-bound contract | Relevant, but substantially more internal complexity than the starting PR; simplification must preserve the contract rather than delete individual fixes |
| Shared Python ServiceParam behavior and wrapper generation; shared schema-name and HTTP internal-column handling | Makes normal setters, copy/save/load, schema planning and execution work for CU | Broader compatibility work affecting other stages; deserves an explicit prerequisite/change grouping rather than being treated as incidental CU-only fixes |
| Explicit/default parameter serialization | Preserves creation intent through normal persistence | Original plan discouraged shared serialization changes; the later user-selected compatibility option explicitly authorized the narrow opt-in approach. Other writers keep their default behavior |

No new analyzer-management operations, durable recovery product, SDK dependency, Fabric integration, or cluster-wide service quota controller was added. The principal expansion is implementation and shared-code impact, not new user-facing CU features. This does not prove the implementation is minimal. Before restarting the loop, make the shared compatibility scope and the retained failure contract explicit; do not blindly revert required fixes or silently change acceptance rules.

### User-selected simplification assessment
The user selected "Assess simplification and shared-code isolation first". This assessment made no further production-code changes and did not start attempt 24.

**Recommended boundary:** narrow unnecessary shared behavior changes; keep native CU capabilities and the required lifecycle protections. Do not replace shared Spark parameter handling with a CU-specific parameter framework.

| Area | Concrete option | Recommendation and evidence |
| --- | --- | --- |
| Shared column-name defaults | Keep the new explicit-resolver helper used by CU and `SimpleHTTPTransformer`, but preserve the original behavior of pre-existing `DatasetExtensions` overloads and `withDerivativeCol` | Best low-disruption scope reduction. CU calls the explicit three-argument overload and obtains its Dataset resolver separately. The changed old overloads also affect unrelated causal estimators, explainers, text featurizers, and balance-measure stages. Those default-behavior changes are not necessary to make CU's explicit calls resolver-aware. Requires focused regression validation before implementation is accepted |
| Shared Python ServiceParam behavior | Treat `Utils.py` and `Wrappable.scala` as one explicit compatibility prerequisite, with neighboring-stage controls; alternatively make a coherent opt-in interoperability layer | Prefer the explicit prerequisite over a CU-local copy or many feature flags. Generated getters/setters, `isSet`, scalar/column inspection, transfer, copy and load are coupled. The generator has `pyInheritedClasses` and wrapper hooks, but using them to isolate this behavior is a real redesign, not a surgical deletion. No proposed change to public CU setters |
| Shared headers | Separate added custom-header accessors from case-insensitive Content-Type replacement | Accessors could be CU-local; shared replacement is a distinct shared-helper fix. CU's own validation rejects connector-managed header overrides, making global replacement a candidate for separate scope rather than an assumed CU dependency. Confirm inherited-path and negative controls before deferring it |
| Explicit/default persistence | Keep the already validated opt-in writer behavior | This follows the user's later explicit choice; reverting it restores the creation-intent bug or requires bespoke provenance/loader machinery. No reason to replace it merely to avoid touching a core file |
| Budget, DNS, transport and JSON/header bounds | Preserve the current isolated helpers; consider structural cleanup only against an unchanged lifecycle table | No evidence for safely deleting these protections. DNS worker lifetime is distinct from caller timeout; request-write tracking distinguishes definitely-unsent from possibly-accepted calls; parsing bounds protect row-level handling. Removing these pieces would change the agreed failure contract. An outcome-type/state-machine rewrite now would introduce more risk and scope, not less |

The existing shared `SimpleHTTPTransformer` internal-column collision correction is a direct dependency of CU's nested pipeline; keep it small and explicitly identified rather than copying the HTTP pipeline into CU.

**Evidence anchors:** `ContentUnderstanding.scala:61-66, 387-422`; `DatasetExtensions.scala:19-23, 51-75`; `SimpleHTTPTransformer.scala:116-122`; `Wrappable.scala:83-115, 199-217, 294-301, 511-566`; `Utils.py:59-217`; `CognitiveServiceBase.scala:226-231, 358-363, 431`; the five CU-local budget/DNS/transport/header/parsing helpers. Session user decisions preserve native functionality, normal SynapseML persistence, and later opt-in explicit/default compatibility.

Current rename-aware Git diff accounting, including untracked source/tests and excluding review evidence: CU production +999/-531; shared production +174/-34; tests +3339/-902; docs/config +428/-377. These counts include deletions and test moves and are not a measure of feature growth. They are not directly comparable to snapshot patch-line counts from earlier attempts.

**Implementation is not yet authorized by this assessment.** Recommended next implementation, if selected: first narrow shared column-name default changes and separate the header dependency decision; retain all required CU behavior and existing regressions. Keep shared Python compatibility as an explicit prerequisite unless the user chooses the larger opt-in redesign. Do not resume reviews until that scope is settled and any resulting edits are validated.

### Causality trace and qualification of the narrowing recommendation
The user asked whether the proposed narrowing would undo review-driven fixes. The original finding artifacts were read directly:

- Attempt 1, GPT Issue 3: exact-case temporary-column allocation could overwrite and then drop an original case-variant input column. The reviewer explicitly included the inner HTTP pipeline. Resolver-aware allocation is a correctness fix, not cosmetic cleanup.
- Attempt 4, GPT Issue 1: a pending Python scalar could overwrite a later explicit column setter, causing each row to submit stale document data. Setter invalidation addressed that actual bug.
- Attempt 9, Grok Issue 2, followed by attempt 10, Opus Issue 1: setter invalidation exposed Python/JVM inspection inconsistency; the ensuing JVM-backed `isSet` change broke the old transfer loop's assumption that `isSet` implied a Python `_paramMap` entry. This is a documented fix-induced regression chain. Reverting one layer alone is unsafe.
- Attempt 6, GPT Issue 1: generated custom-header column setters/getters had no matching JVM methods and failed via Py4J. Removing accessors without a replacement restores that defect.
- Attempt 7, Grok Issue 1: case-variant custom Content-Type could coexist with the connector's JSON header. The resolution added both CU validation and shared case-insensitive replacement; recorded coverage included CU 71/71 and AzureSearchAuthSuite 48/48.
- Attempt 22, GPT Issue 1: save/load materialized a default analyzer ID as explicit and bypassed the new creation guard. The opt-in serializer correction follows the later explicit user compatibility choice.
- Attempt 9, Grok Issue 3, and subsequent DNS findings: bounded caller waits alone did not bound physically blocked DNS workers; the worker-lifetime protections address concrete resource behavior rather than an additional product feature.

**Revised recommendation:** do not revert any of these fixes merely to reduce scope. The earlier resolver-default and Content-Type suggestions are unproven isolation candidates, not validated safe removals. The current passing evidence applies to the current implementation; it does not transfer to a proposed revert. Restoring an old shared helper can restore its old defects for other callers even if CU keeps explicit handling.

The lowest-risk immediate scope action is to identify the shared compatibility fixes as explicit prerequisite/review units, preserving the working behavior. Any later physical isolation must demonstrate that the original finding's regression remains fixed through all dependent paths and that unaffected-stage behavior is preserved. No such counterfactual implementation or validation has been performed. No source was reverted, and no attempt-24 reviewer was launched.

### Final scope and readiness for the next user-directed cycle
- **User decisions:** Retain `transform`, analyze/submit/poll, explicit create/get, native options, standard credential persistence and the shared compatibility corrections. Do not revert fixes to reduce scope. CU owns semantic/model behavior; the connector does not need a custom semantic analyzer or inference-accuracy demonstration to complete offline connectivity evidence. Durable journals, Fabric/backend/LLM integration and runtime packaging remain deferred. Live calls explicitly declined. The user requested readiness notification before a new review cycle; no reviewers were launched.
- **Historical gate before edits:** All 69 prior Markdown review artifacts were fully read, including retries, non-results, checklists and appended resolutions. The session artifact `cu-prior-review-audit.json` inventories paths, hashes and first-header provenance, and records 18 must-preserve invariant groups plus contradictions and pending evidence. Original fixed/invalid/superseded policies were distinguished; missing attempt-17 Opus signoff was not fabricated. The parent inspected the full aggregated ledger before further edits.
- **Gap closure:** Existing Scala coverage already asserts exact rich/unknown raw-response retention. The existing generated-Python copy/save/load/request test now asserts equality with the complete returned body, including confidence, grounding source/spans, multiple content entries, false/null/Unicode and unknown nested properties. The guide now explicitly separates connector responsibility from CU semantics and distinguishes offline proof from live service evidence. No additional production code or analyzer setup was introduced during this pass.
- **Validation:** `cu-attempt24-gap-python.log`: 29/29 passed against actual loaded, hash-matched JARs. `cu-attempt24-gap-black.log`: 205 unchanged. `cu-attempt24-live-guard.log`: intentionally skipped without credentials, zero live tests. Whitespace clean. All 25 changed/new Scala production and test sources match their validated source JARs, preserving the earlier cognitive style/compile/codegen/publication and CU136/136 evidence without claiming a new Scala run.
- **Final delta from attempt 23:** Four source/test/doc paths: protocol (creation-only vocabulary), admission suite (resource lifecycle regressions), Python test (public creation regression and strengthened passthrough assertion), guide (creation and final contribution contract). No shared-code rollback.
- **Prepared snapshot:** `cu-stage1-attempt24.patch`, 36 paths, 446281 bytes, SHA256 `1d1ad85ceea641fd031f6d78cf8130ef996d765363d6cbfde635fe8f9fbb7ce4`. Context and manifest saved alongside it. Resolve reviewer models afresh and verify the manifest when the user authorizes dispatch. The historical audit and review feedback must not be supplied to independent reviewers.
- **Target update:** Read-only public GitHub API confirms PR2698 remains open against master at original head `f3a7963c217677e9b6564cc6736495e0f1bcdce4`. Current remote master was observed at `d3f3af6bd3e4b7d191f51c9eb29f9aeffe788193`, absent from the local object database; cached origin/master is `ef69a5daa1cfa2346389a54e87ff8d3366d28431`. PR metadata's recorded base `8c7143875c843c649a817cf3e8ba9c7bee23689c` is not the current target tip. Current-target integration and external CI remain outstanding. This is readiness for another local review cycle, not merge readiness.