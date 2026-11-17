---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 6
stage_attempt: 4
round: 1
timestamp: "2026-11-10T21:48:55Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "fbfebd0992888a27fe46fb1cadf9a94193332fbf2d4247eaadb1521ff53a416a"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\pr-2815\task-5837660-attempt-6-stage-attempt-4-review-1-claude-opus-5.5.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Independence.** I did not open, list or search `reviews\pr-2815` or any review run directory. Every search was limited to `cognitive\src`, `core\src`, `docs`, `website` or a named file, and I saw no prior or sibling feedback.
- [x] **Retry core** (`ContentUnderstandingThrottling.scala` 16–73):
  - `stopSubmission` only applies to POST (line 19).
  - `mark()` is taken before each attempt (52) and `refundSince(mark)` runs after the wait (65). Only throttled attempts and their waits are refunded. Earlier work and the final non-throttled attempt stay charged.
  - `Backoff` (24–41) matches the spec: base is `max(1000, pollingDelay)` and cap is `max(60000, base)`. The jitter floor is `max(base, current - current/10)` (37), and a valid header resets growth (32).
  - Each `retry` call creates a new `Backoff` (49), so backoff resets per exchange.
  - With the opt-in off, `retry` simply passes the attempt through (46–47).
- [x] **Throttle classification** (`ContentUnderstandingResponseParsing.scala`):
  - Lines 22–45: `throttled` is true for a 429 on GET, or on POST without Operation-Location.
  - Lines 117–131: Retry-After seconds saturate instead of overflowing, and past dates become 0.
  - The `ContentUnderstandingThrottlingSuite` test "opt-in throttling does not replay uncertain submissions" covers 429 with a handle, an invalid handle, 500, 408 and a disconnect.
- [x] **Protocol** (`ContentUnderstandingProtocol.scala`):
  - `exchange` (314–335): retries only when both `analysis` and `settings.retryThrottledRequests` are true. `managementGet = !analysis && GET` keeps management behavior as before.
  - `pollWithClient` (582–616): throttled GETs keep retrying after a partition stop.
  - `rowHandler` (656–683): creates a `Context` per task, short-circuits unsent rows, and nests transport and read error codes under the shared failure.
- [x] **Budget** (`ContentUnderstandingBudget.scala` 14–53): the injectable clock is used consistently by `mark`, `refundSince` and `remainingMs`. Each budget belongs to one call on one thread, so the non-atomic `started +=` is safe. `pause` keeps its interrupt checks and adds `blocking` plus ticks.
- [x] **Logging** (`ContentUnderstandingLogging.scala` 13–58):
  - The reporter logs the first warning, then at most once every 60 s, including ticks during waits.
  - Messages contain only method, retryCount, waitMs and the stage, partition and task attempt IDs.
  - Task IDs come from `AsyncClient.currentTaskContext`, a thread-local in `Clients.scala`, so they survive async workers.
  - The suite's sparse-log test checks that no secret or endpoint appears.
- [x] **New options:**
  - Checked `ContentUnderstanding.scala` 85–91, 168–190 and 317–324, and `ContentUnderstandingParams.scala` 140–161.
  - `allowInputTruncation` and the retry opt-in live in separate traits, and `retryThrottledRequests` defaults to false.
  - A null value omits the option and false sends `false`. There is no version allowlist.
  - `validateManagement` (434–442) is unaffected.
  - `createAnalyzer` (476–488) sends only the analyzer definition. `modelDeployments` appears only in analysis requests (306–307).
  - `test_ContentUnderstanding.py` 172–226 checks the query strings after copy, save and load.
- [x] **Python inspection and MLflow:**
  - `Utils.py` has no generic ServiceParam overrides left; its value conversions are at 106–140.
  - Generated wrappers still go through the JVM: `Wrappable.scala` 182–191 (defaults), 209–232 (setters) and 309–330 (getters).
  - `test_ContentUnderstanding.py` 228–309 confirms MLflow parameters omit the credentials while configured, copied and loaded stages still use them.
  - `Autologging.md` 12–23 limits the claim to the parameter logger and warns about model artifacts, Python parameter maps and generic `set`.
- [x] **Notebook** (`Quickstart - Semantic Extraction with Content Understanding.ipynb`):
  - It uses no SDK and generates a sample PDF locally.
  - It creates the analyzer with `allowReplace=False`. `modelDeployments` is validated before creation and set on the analysis transformer, not in the analyzer definition.
  - Submission writes once with `errorifexists`, then reads the Parquet back before `show`.
  - Recovery checks the endpoint origin and rejects saved handles from a different origin before polling or writing.
  - The poller does not depend on analyzer creation or the model mapping.
- [x] **Guide** (`Content Understanding.md`): lines 375–405 cover the opt-in, budget refunds, fallback policy and shared-stop scope. The example at lines 446–457 selects retries from the saved Parquet and writes with `errorifexists`.
- [x] **Candidates I ruled out:**
  - The relative `../Quickstart%20-%20…` links follow existing doc convention, and `trailingSlash: true` is set (`docusaurus.config.js` line 17).
  - The notebook is excluded from Databricks runs (`DatabricksUtilities.scala` line 258, `DatabricksUtilitiesSuite` lines 26–27) and from Synapse runs, and Fabric uses an allowlist.
  - The `managementGet` change in RecoverySuite 774–788 only changes the TransportError text.
  - `mlflow==2.21.3` is pinned in `environment.yml` (line 36).
- [x] **Observation, not filed as an issue:** `ContentUnderstandingProtocol.scala` line 50 (`val TooManyRequests = 429`) is no longer used by main code. Optional cleanup.
- [ ] **Not run:** I executed no builds, tests, codegen, Black or live CU calls (read-only review). I relied on the driver's validation evidence without reproducing it.
- [ ] **Coverage gaps:** the supplied diff was cut off after the `Settings` hunk, so I reviewed the rest from the current working-tree files. I grep-sampled the Concurrency, Admission and Submission Scala suites rather than reading them line by line.

## Issues

### Issue 1: Shared-stop relabel of definitive POST 429s ignores the retry opt-in
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala
- **Line(s)**: 332–334 (relabel), 311–312 (`SharedStopError`), 446 (`decode`)
- **Description**:
  - `exchange` replaces the response's diagnostic with `SharedStopError` whenever `analysis && raw.throttled(method) && context.stopSubmission(method)` holds. It never checks `settings.retryThrottledRequests`.
  - In default mode (opt-in false), `retry` just passes the response through, so no retry or wait was stopped. Even so, if an analysis POST is in flight when a sibling row triggers the partition stop, its definitive 429 gets relabeled.
  - `decode` builds the error from `raw.diagnostic.orElse(serviceError)` (line 446). The relabel therefore hides the service's `TooManyRequests` error and overwrites any `ResponseBodyReadFailed` or `ResponseTooLarge` diagnostic.
  - By contrast, `rowHandler` (about lines 672–677) nests those codes as `innererror`.
  - The outcome is timing-dependent. The only test of this relabel (`ContentUnderstandingThrottlingSuite` 241–260) runs with the opt-in on.
- **Risk**: Default-mode diagnostics diverge from the guide:
  - Lines 378–379 say the default "preserv[es] the behavior described above".
  - Lines 289–290 say rows already sent keep their own raw response.
  - Lines 418–421 say an unreadable 429 reports `ResponseBodyReadFailed` and an oversized one keeps `ResponseTooLarge`.

  Users following the throttling advice at line 434 could mistake a throttled row for a configuration failure. The impact is limited to diagnostics: the row stays `Rejected` with HTTP 429 and no handle, so the retry selection (449–456) and duplicate safety are unaffected.
- **Suggested Fix**:
  - Apply the relabel only when `settings.retryThrottledRequests` is true, ideally only when the stop actually suppressed a retry or wait.
  - Merge rather than replace: nest any existing diagnostic as `innererror`, the same way `rowHandler` does.
  - Add a default-mode concurrency test: one sibling gets 404 `AnalyzerNotFound` while another POST is in flight and receives a 429. Assert that the 429 row keeps its own diagnostic.

### Issue 2: The `pollingDelay` param doc misstates the 60000 ms fallback cap as exact
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingParams.scala
- **Line(s)**: 118 (code: `ContentUnderstandingThrottling.scala` 25–26, 35–38)
- **Description**:
  - The doc says "the first fallback and delays of at least 60000 are exact." Read naturally, that means fallback delays of 60000 ms or more are not jittered, which is false.
  - With `pollingDelay=1000`, the base is 1000 and the cap is 60000. Once growth reaches the cap, the lower bound is `max(1000, 54000)`, so waits fall anywhere between 54000 and 60000 ms.
  - Waits are exact only when `pollingDelay >= 60000`, because then the base equals the cap. The Markdown states this correctly (397–399 and 499); the param doc does not.
- **Risk**: This text becomes the generated Python docstring for `setPollingDelay` and `getPollingDelay`. Users may expect exact 60 s waits at the cap. This is a documentation problem only.
- **Suggested Fix**: Reword to something like "Downward jitter (up to 10%) never goes below the starting delay; the first fallback is exact, and a pollingDelay of at least 60000 gives a constant fallback." Optionally add a `Backoff` test asserting that capped samples fall between 54000 and 60000.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending driver fix
- **How verified**: Pending

### Issue 2
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending driver fix
- **How verified**: Pending

## Driver Resolution Update (supersedes the pending entries above)

- Issue 1: implemented. `exchange` requires the retry opt-in before relabeling a
  stopped POST 429; a shared helper nests the original read diagnostic as
  `innererror`. New deterministic protocol cases cover retry off/on with readable,
  truncated and oversized bodies, preserving rejection and HTTP status.
- Issue 2: implemented. Parameter documentation now distinguishes configured
  `pollingDelay >= 60000` from jitter at a 60000 ms cap. The transient-failure
  attempt accounting and opt-in duration exception are explicit again.
- Validation is recorded below after the new offline run; the original reviewer
  verdict remains ISSUES_FOUND until a new independent review returns.

### Stage-attempt-4 collection

Lenses 1, 2, 4, 5 and 6 completed with owner exit 0, persisted `issues_found`,
`reviewExitCode: 0`, exact runtime model `claude-opus-5.5`, and no transport
errors. Their tool-call audit found no prior/sibling feedback reads. Lens 2's
disclosed root `scalastyle*.xml` glob returned configuration/result paths only;
it did not expose feedback. The other completed lenses kept searches scoped.

Lens 3 was stopped at the user's direction before a valid response after
prolonged repeated source exploration. Its partial events and dispatching
receipt remain unchanged in the session run directory. It is incomplete, not
clean, and does not count as a sixth signoff. The frozen 26-file stage-4 input
was verified unchanged before these fixes began. All five original reports
remain intact. Stage attempt 5 will review a new frozen snapshot.

### Validation of the stage-5 fixes

- Aggregate `compile`, `Test / compile`, `scalastyle`, `Test / scalastyle`, and
  `cognitive/codegen` passed on the updated tree.
- All 198 CU Scala tests passed in one run (10 suites, no failures or skips),
  including the new diagnostic, callback, sharing and async-context cases.
- All 24 DatabricksUtilitiesSuite tests passed, including exclusion using the
  actual notebook and the shared generated-filename transformation. The extra
  `*WrappableSuite` selector matched no suite; it is not counted as coverage.
- Changed Python test formatting and `git diff --check` passed.
- Python notebook/bridge/MLflow/shared regressions passed: 97 tests and 248
  subtests, with seven live CU tests skipped, in 400.47 seconds. Both actual
  notebook mapping variants passed, along with recovery and cancellation.
- Stage-5 source snapshot: 30 files, 189135 bytes, diff SHA-256
  `e8a250115b19d37849920264b542e4545559c7da654c917a4675b00283fd47e1`.
  The snapshot was verified unchanged during validation.
- Retained logs: session `files\cu-loop-s5-scala.log` and
  `files\cu-loop-s5-python.log`. No live CU calls,
  resource provisioning, commit, push or ADO changes occurred.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "Primary estimate; no human or repository tier override",
    "models": "Active review-code Medium complementary-family rule, freshly resolved from the current task catalog",
    "stageBehavior": "Six concurrent independent lenses; human explicitly requested a fix/review loop until clean",
    "safetyStop": "No new timeout or budget; no safety controls disabled",
    "delivery": "Human iteration-mode instruction: no commit or push without explicit approval"
  },
  "changeSize": "Twenty-six source, test and documentation files relative to PR head; most volume is tests/docs. Localized CU options/throttling/budget changes plus shared Python parameter-inspection changes.",
  "cyclomaticComplexity": "Moderate: definitive-admission retry eligibility, cancellation, partition shared-stop, fallback backoff and refund accounting interact with existing asynchronous state machines.",
  "rationale": "Six complementary lenses cover behavioral, architectural, concurrency, correctness, testing and documentation concerns. Supported CLI update and serialization probe passed; each owning runner must still validate transport and identity. Failed or contaminated results cannot count as clean.",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": "User: get into a fix review loop until everything is clean and give me a report when you are done. User selected fallback-only adaptive backoff and HasAllowInputTruncation. Existing iteration-mode instruction overrides automatic commit/PR.",
  "reviewAttempt": 6,
  "stageAttempt": 4,
  "sourceHead": "41b54ec47d514aac82494c70ed2963a8bc69ff5d",
  "target": "master",
  "artifactDirectory": "reviews/pr-2815",
  "uncertainty": "The synthetic CLI transport probe does not establish full review transport success. Offline checks do not establish live CU compatibility or approval of matching packaged artifacts."
}
