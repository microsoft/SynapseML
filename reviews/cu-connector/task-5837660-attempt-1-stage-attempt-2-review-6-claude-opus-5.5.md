---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 2
round: 6
timestamp: "2026-11-05T21:30:37Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "3f2326ac9eb3907096908a32160f580c7c3a0b37f2ef7cc1dc342f01b5a9db3e"
---

## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-2-review-6-claude-opus-5.5.md
- **Issues Found**: 5
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read `cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstanding.scala` in full: `transform` override with the dataset resolver, `settings(resolver)`, `rowUsesColumnAuth`, scalar-only `validateRow`, `buildInternalTransformer` internal-name reservation, driver-only management, and `ContentUnderstandingOutputParser` logged as `FeatureNames.Core`.
- [x] Read `ContentUnderstandingProtocol.scala` lines 1–760: `DeadlineControl`, `exchange`/`exchangeBeforeDeadline`, `withClient`, which builds a client for each operation, `decode`, `FailureClassifier`, `checkSharedFailure`, `decodeSubmission`/`safeSubmission`, `retainPollState`, `pollWithClient`, `execute`, `rowHandler`, `getAnalyzerResponse`, and `finishCreation`. Checked them against the shared-stop, acceptance, retention and creation-mapping rules in the contract.
- [x] Read `ContentUnderstandingDnsResolver.scala` and `ContentUnderstandingTransport.scala`: expiry is rechecked after acquiring DNS capacity, capacity is released on rejection, and genuine DNS failures and worker rejections do not become deadline expiry.
- [x] Checked provenance stripping: `core\...\io\http\HTTPSchema.scala:209` (`toHTTPCore` drops the marker header) and `CognitiveServiceBase.scala:553-562` (`addHeaders` removes or re-adds the marker).
- [x] Checked logging primitives in `core\...\logging\SynapseMLLogging.scala:156-180`. A scoped grep for `logVerb(` under `cognitive\src\main\scala\...\services` found no CU management logging.
- [x] Read `ContentUnderstandingParams.scala:70-160` and `Wrappable.scala:70-120`, covering parameter docs and the generated `isSet` stub on `ComplexParamsMixin`-based wrappers.
- [x] Read the user guide `docs\Explore Algorithms\AI Services\Content Understanding.md` (lines 1–560) and the `pipeline.yaml` lines with the `contentunderstanding` UnitTests leg (`PACKAGE: "services.contentunderstanding"`). Compared the documented Retry-After, `concurrentTimeout`, DNS capacity, Rejected/Unknown and creation-mapping claims with the code.
- [ ] Tests not executed (read-only review). Relied only on the supplied validation evidence: `cu-stage6-polish-green.log` 12/12, earlier 144/144, and snapshot25 Python 29/29. No new combined run is claimed.

## Issues

### Issue 1: Every row operation builds its own HTTP client, so connections and TLS sessions are never reused
- **Severity**: Low
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala
- **Line(s)**: 420-432 (`withClient`), 629-655 (`execute`), 659-683 (`rowHandler`)
- **Description**: `execute` calls `withClient` once per row. Each call builds a new `HttpClients.custom()...build()` with its own connection manager and closes it when the row finishes. Polls within one operation share that client, but separate rows never reuse a connection. Every analyzed row therefore pays a new TCP connection, a TLS handshake and a DNS handoff to the `synapseml-cu-dns` pool. The code comment explains the no-retry reason. Neither the comment nor the user guide's "Bounds and concurrency" section mentions the per-row connection cost.
- **Risk**: Large partitions with many short operations pay setup latency and CPU on every row, and they put connection pressure on the CU front end. Users who tune `setConcurrency` cannot see this cost.
- **Suggested Fix**: Keep the no-retry, no-redirect configuration but create one pooled client per task inside the `rowHandler` closure, closed by a task-completion listener. Each operation would still get its own `ContentUnderstandingBudget`, `BudgetedDnsResolver` and `RequestConfig`. If per-operation isolation is intentional, add one sentence to the guide's bounds section saying each operation opens a fresh connection.

### Issue 2: Analyzer management calls and partition-wide shared stops produce no log events
- **Severity**: Low
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstanding.scala; ContentUnderstandingProtocol.scala
- **Line(s)**: ContentUnderstanding.scala 443-463 (`createAnalyzer`, `getAnalyzer`); ContentUnderstandingProtocol.scala 664-683 (`rowHandler` shared-stop branch)
- **Description**: `transform` is wrapped in `logTransform`. The driver-side `createAnalyzer` and `getAnalyzer` calls are not wrapped in `SynapseMLLogging.logVerb`, even though they are billable PUT/GET calls that can run for up to `maxOperationDuration`. Separately, when `rowHandler` sets `stopped` after a definitive shared authentication, analyzer or deployment failure, nothing is logged on the executor. The stop is visible only in row diagnostics.
- **Risk**: Operators cannot see analyzer provisioning, management latency or failures in the usual SynapseML telemetry stream. Executor logs also do not show why a partition stopped submitting, so diagnosis depends on saved output.
- **Suggested Fix**: Wrap both management methods in `logVerb("createAnalyzer", ...)` and `logVerb("getAnalyzer", ...)`. `ContentUnderstandingException` messages already leave out service details, so error logging is safe. In the shared-stop branch, log one warning per task containing only the connector diagnostic code and HTTP status, never headers or the body.

### Issue 3: Some parameter docs disagree with validation and retry behavior
- **Severity**: Low
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingParams.scala
- **Line(s)**: 75-76 (`stringEncoding`), 83-84 (`processingLocation`), 113-114 (`pollingDelay`); see also ContentUnderstanding.scala 230-233 and ContentUnderstandingProtocol.scala 302-307, 703-722
- **Description**:
  - The `stringEncoding` and `processingLocation` docs say "for example codePoint, utf16, or utf8" and "for example geography, dataZone, or global". `validateOptionValue` accepts only those exact values and rejects anything else with "Unsupported ...".
  - The `pollingDelay` doc says it applies "when Retry-After is absent". `retryDelay` also uses it when Retry-After is malformed or longer than 19 digits, and `getAnalyzerResponse` uses it between management GET retries. The user guide's table correctly says "absent/invalid".
  - These Scala strings become the generated Python docstrings and `explainParam` text.
- **Risk**: Python users may read "for example" as an open set and pass a new service value, only to have it rejected before dispatch. The `pollingDelay` description understates where the delay applies.
- **Suggested Fix**: Change the wording to "One of codePoint, utf16, or utf8" and "One of geography, dataZone, or global". Change the `pollingDelay` doc to "Milliseconds between polling or management GET retries when Retry-After is absent or invalid."

### Issue 4: Management GET errors tell users to resume polling, and CU row-auth provenance travels in a header named for Fabric
- **Severity**: Low
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingTransport.scala; ContentUnderstanding.scala; ContentUnderstandingProtocol.scala
- **Line(s)**: ContentUnderstandingTransport.scala 33-37; ContentUnderstanding.scala 334-336; ContentUnderstandingProtocol.scala 106, 490-496; core HTTPSchema.scala 218-221
- **Description**:
  - `ContentUnderstandingTransport.failure` decides the message from `method == "GET"`. Every GET transport failure therefore reads "HTTP polling transport failed. Retain the operation handle and resume polling." This includes `getAnalyzer` and the final post-creation analyzer retrieval, where there is no operation handle and poll mode does not apply.
  - CU marks row-credential requests by putting `RowAuthColumnMarker` in `HTTPRequestData.FabricAuthMarkerHeader` (`X-SynapseML-Implicit-Fabric-Auth`). `FailureClassifier.rowCredentialWasSelected` then reads it back through `isFabricAuthMarker`. This works: the marker is stripped in `toHTTPCore`, and `usesFabricAuth` checks for the value `"true"`. But the name describes Fabric fallback auth, while this connector explicitly opts out of Fabric auth.
- **Risk**: Users who hit a transient `getAnalyzer` failure get recovery advice that does not apply to them. Future maintainers may misread the Fabric-named marker as Fabric auth being in play, or change Fabric-marker handling without realizing CU's shared-stop classification depends on it.
- **Suggested Fix**: Pass a flag, or use a separate message, so management retrieval reports something like "HTTP analyzer retrieval transport failed; no request was replayed." For naming, add a short comment at `prepareRequest` and `rowCredentialWasSelected` saying the Fabric marker header is reused as an internal, wire-stripped provenance channel. Alternatively, add a generically named alias, such as `InternalProvenanceHeader`, that points to the same constant, so wire stripping is unchanged.

### Issue 5: The user guide's definition of `Rejected` leaves out scalar 401/403 rejections
- **Severity**: Low
- **File**: docs\Explore Algorithms\AI Services\Content Understanding.md
- **Line(s)**: 326-329 (definition of `Rejected`); compare ContentUnderstandingProtocol.scala 527-540 (`decodeSubmission`) and 664-683 (`rowHandler`)
- **Description**: The guide defines `Rejected` as "HTTP 429 throttling, HTTP 401/403 when authentication is configured per row ...". In code, `decodeSubmission` marks any POST 401/403/429 without a handle as `Rejected`, whether the credential came from a column or a scalar. A shared scalar-auth failure therefore leaves the triggering row `Rejected`, with `SharedConfigurationFailure` attached by `rowHandler`. Later text partly covers this ("For scalar authentication failures it is nested under `SharedConfigurationFailure`" and "it can be `Failed`"). The definition list still says, or strongly implies, that scalar 401/403 rows are not `Rejected`.
- **Risk**: Users who triage by status could conclude that a `Rejected` 401 means a bad per-row credential and miss a bad shared key. The retry SQL example still selects the correct rows, so recovery is unaffected.
- **Suggested Fix**: Reword it to: "HTTP 429 throttling or HTTP 401/403 without an operation handle (scalar credentials also add a nested `SharedConfigurationFailure` and stop the partition) ...". Then state that the triggering row is `Rejected` for authentication and `Failed` for analyzer or deployment failures.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Unresolved Low; retained at the final High attempt.
- **What changed**: No source change. Retained documentation of per-operation connection cost; pooling is a separate design change, not an automatic fix.
- **Why**: A client is created for each `execute`, while polls within that operation share it. Its DNS resolver captures that operation's budget. Task-level pooling needs an explicit budget, cancellation and completion-lifecycle design. The report's absolute TLS-session/handshake and billing implications are not established by this source inspection or a benchmark.
- **How verified**: Driver inspected `ContentUnderstandingProtocol.scala:420-432,629-683`. Client construction/isolation is confirmed; quantitative performance impact is not measured.

### Issue 2
- **Status**: Unresolved Low; retained observability recommendation.
- **What changed**: No logging added during final collection.
- **Why**: Management methods and partition stop do lack dedicated events, but wrapping an entire method in `logVerb` is not proven safe solely from the generic `ContentUnderstandingException` message. Other validation/transport exceptions can flow through the wrapper, and `logErrorBase` logs the exception. Any follow-up must preserve safe, bounded payloads and avoid logging arbitrary service content. PUT/GET billing is not established by this review.
- **How verified**: Driver inspected `ContentUnderstanding.scala:443-463`, protocol `rowHandler`, and `SynapseMLLogging.scala:115-180`. `logVerb` uses ordinary structured logging with no feature name, not a new certified feature event.

### Issue 3
- **Status**: Unresolved Low; parameter-documentation follow-up.
- **What changed**: No parameter/docstring changes after the final snapshot.
- **Why**: The quoted parameter strings understate the closed option sets and absent/invalid Retry-After fallback, including management GETs. Correcting them is appropriate follow-up; existing validation and fallback behavior are unchanged.
- **How verified**: Driver read `ContentUnderstandingParams.scala:75-114` and management retry handling in `ContentUnderstandingProtocol.scala:703-724`; reviewer evidence identifies the matching validators and retry helper.

### Issue 4
- **Status**: Unresolved Low; recovery-wording finding plus overlap with architecture item 7.
- **What changed**: No transport classification, marker or header stripping changes.
- **Why**: Sent, non-timeout GET transport failures use polling advice even for analyzer retrieval; the wording should distinguish those operations. Unsent and timeout branches already have different diagnostics. The Fabric-named provenance channel is the previously retained architecture recommendation, not a newly proven wire leak or authentication failure.
- **How verified**: Driver read `ContentUnderstandingTransport.scala:26-38`, analyzer management methods, and management GET exchange handling. The existing wire-stripping regression remains applicable.

### Issue 5
- **Status**: Unresolved Low; user-guide clarification.
- **What changed**: No documentation changes after the final snapshot.
- **Why**: The initial Rejected definition singles out row credentials, although later text describes scalar-auth shared stops. The initial definition should state that definitive scalar 401/403 admission rejection also keeps the triggering row Rejected. No retry-selection or classification change is proposed.
- **How verified**: Driver read the guide at lines 310-365 and protocol shared-stop handling, which copies the triggering response's status while replacing its diagnostic.

## Authorized final-polish follow-up

The user selected remediation and a new independent review cycle for these five
findings plus the sibling GPT cleanup finding only. Prior architecture and coverage
deferrals are unchanged. The original findings and historical dispositions above
are preserved.

| Issue | Local correction | Evidence before rerun |
| --- | --- | --- |
| 1 | Documented the fresh per-operation client and cross-row connection setup cost. No pooling or budget/lifecycle change; no assertion that every connection performs a full TLS handshake. | Guide's bounds section checked against existing client construction. |
| 2 | Management methods record ordinary structured outcome/timing and exception type, without exception messages, causes or service bodies. Logging failures cannot replace the result or original exception. Shared stop logs fixed code/HTTP status only on the first atomic transition. | Public management, validation-error, failing-logger and shared-rejection tests failed before the fix; added concurrent-rejection coverage. |
| 3 | Parameter descriptions now state the closed option sets and absent/invalid Retry-After fallback for polling and management GET retries. | Cognitive codegen passed; corrected class/parameter/setter docstrings verified in generated ContentUnderstanding.py. |
| 4 | Analyzer retrieval passes an internal context flag to transport diagnostics, including the post-creation GET. Poll advice remains unchanged for operation polling; unsent/timeout branches are unchanged. Added two short comments explaining the existing wire-stripped provenance channel, without implementing the broader marker refactor. | Public direct-get/creation retrieval and poll-control cases; direct-get regression failed before the fix. |
| 5 | Rejected definition explicitly includes scalar 401/403; triggering versus skipped rows and analyzer/deployment failures are distinguished. | Guide checked against existing classifier and shared-stop behavior. |

Validation passed: main/test style and compilation, 158/158 tests in seven CU
suites, cognitive codegen and corrected generated docstrings. The first five focused regressions reproduced their
expected gaps in `cu-polish-followup-red.log`. Main style passed; test style initially
flagged null Log4j fixture arguments, now replaced with a concrete filter/layout.
The independent follow-up review is pending. No source change or remediation claim
is a clean reviewer signoff.