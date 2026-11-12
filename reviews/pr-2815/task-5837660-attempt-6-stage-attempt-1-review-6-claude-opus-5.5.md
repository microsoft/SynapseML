---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 6
stage_attempt: 1
round: 6
timestamp: "2026-11-10T19:14:42Z"
outcome: "failed"
reviewer_model: "claude-opus-5.5"
input_sha256: "b9d2457e8743d09fb0e9ef63dc41e2950c29f6f6058e8151d30c23e3f88c1771"
---

## Runner Failure

Invalid CLI event on line 10: Expecting ',' delimiter: line 1 column 32220 (char 32219)
Invalid CLI event on line 554: Expecting ',' delimiter: line 1 column 3994 (char 3993)

## Original Response

## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\pr-2815\task-5837660-attempt-6-stage-attempt-1-review-6-claude-opus-5.5.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Budget refund is correct.** In `cognitive/.../contentunderstanding/ContentUnderstandingBudget.scala` L16-32, `elapsed` is captured before the first attempt. `started` is reset only after a throttling wait, so the final non-throttled attempt still uses the budget. `pause` (L37-56) checks for cancellation every 50 ms and rethrows `InterruptedException`. `checkInterrupted` (`ContentUnderstandingProtocol.scala` L271-278) handles both thread interrupts and Spark task cancellation. Cancellation reaches async workers through the `AsyncClient` thread-local owner (`core/.../io/http/Clients.scala` L50-57).
- [x] **Only the right requests are retried.** `exchange` (Protocol L338-356) retries only HTTP 429: any GET, or a POST only when `raw.location.isEmpty`. The `createAnalyzer` PUT (L765) is excluded by its method. Management GETs pass `managementGet = true` (L622 when an analyzer is defined, L717), so they are never retried. Absorbed 429s stay inside `exchange`, so the `remaining` count in `pollWithClient` (L603-635) does not drop. This matches the param docs (`ContentUnderstandingParams.scala` L107-109, L165-168) and the guide (L367-386, L462-482).
- [x] **Default behaviour is unchanged.** `retryThrottledRequests -> false` (Params L149-177) and `Settings.retryThrottledRequests = false` (Protocol L109). When disabled, the wrapper makes exactly one attempt. `TransientStatuses` still includes 429 (L61).
- [x] **`allowInputTruncation` path is correct.**
  - It is a `ServiceParam[Boolean]` URL param (Params L138-147).
  - Only non-poll `activeParams`/`getVectorParamMap` include it (`ContentUnderstanding.scala` L84-97), so poll mode ignores it.
  - `BooleanType` is enforced in validation (L168-200).
  - `analyzeUrl` adds the lowercase value only when present (L317-323).
  - A null cell becomes `None` because `getValueOpt` uses an erased `getAs[T]` (`CognitiveServiceBase.scala` L101-106), so it is omitted rather than sent as `false`.
  - `ContentUnderstandingOptionsSuite` and the Python test (`test_ContentUnderstanding.py` ~L170-218) cover GA omission, true/false, clearing, null rows and String-column rejection.
- [x] **Python credential handling is consistent with `docs/Use with MLFlow/Autologging.md` L12-19.**
  - `core/.../schema/Utils.py` L150-247 no longer overrides generic inspection.
  - `_transfer_params_from_java`/`_transfer_params_to_java` keep service values on the JVM.
  - `explainParam` redaction is kept.
  - The autolog test (L220-300) asserts that the key and token are absent from MLflow params, and that configured, copied and loaded stages still authenticate.
  - `mlflow==2.21.3` is pinned in `environment.yml` L36, so the new top-level import is satisfied.
  - The `finally` block turns autologging off and restores the tracking URI.
- [x] **Guide examples write once and read saved Parquet.** Lines L68-69, L96-97, L243-253, L320-343, L429-438 and L618-624 all write once and read Parquet before extracting or displaying. The Scala snippet (L640-649) does a single write.
- [x] **Notebook flow is correct.** It covers config (L46-83), a generated PDF, `createAnalyzer(allowReplace=False)` (L214), submit with `errorifexists` then read, poll with union/write/read (L284-318), and display from saved `results` (L338-381). The code-point `substring` matches `codePoint` encoding. The sidebar entry is in `website/sidebars.js` L39.
- [x] **CI notebook discovery checked.**
  - `nbtest/DatabricksUtilities.scala`: notebook discovery uses an exclusion list (L250-267). The notebook is uploaded unchanged as JUPYTER (L375-389). No CU environment variables are set (L452-462, L507-524). A failed or timed-out run throws (L599-612).
  - `nbtest/SynapseTests.scala` L49-72 is also exclusion-based.
  - `tools/ci/e2e_impact.py` L39-63 sends docs `.ipynb` and `cognitive/src/main` changes to `ALL_SUITES`.
  - `pipeline.yaml` L74-77 and L230-259 run Databricks CPU E2E by default.
  - grep: no `CONTENT_UNDERSTANDING_*` references in the pipeline, nbtest, tools or `.github`. Only the new notebook uses `getpass`/`input(`; the other AI Services notebooks use `find_secret`.
- [x] **No throttling logs exist.** `ContentUnderstandingLogging.scala` L11-36 only logs cleanup and shared-stop events. A grep of the CU package found no logging on the throttling path. The test helper `ContentUnderstandingFixtures.withLogs` (Stub L23-42) is available for log assertions.
- [x] **New Scala suites reviewed.** In `ContentUnderstandingThrottlingSuite`, about 8 s of real `Retry-After` waits are acceptable, and the interruption test synchronises on a latch and cleans up in `finally`. `ContentUnderstandingOptionsSuite` was also reviewed.
- [ ] I ran no builds, tests or black (read-only review). I relied on the driver's validation evidence and only hand-checked notebook cells for black formatting.
- [ ] The live CU contract (preview `allowInputTruncation`, 429 and `Retry-After` behaviour) can't be checked offline. I treat it as service-contract uncertainty, not a defect.

## Issues

### Issue 1: New notebook is picked up by the Databricks CPU notebook E2E and will fail or hang in CI
- **Severity**: High
- **File**: `docs/Explore Algorithms/AI Services/Quickstart - Semantic Extraction with Content Understanding.ipynb`; `core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/DatabricksUtilities.scala`; `core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/SynapseTests.scala`
- **Line(s)**: notebook L46-83 (`input()` L57/L64, `getpass.getpass` L59), L75, L214, L395; DatabricksUtilities L250-267, L452-462, L507-524, L599-612; SynapseTests L49-72
- **Description**:
  - `DatabricksUtilities.CPUNotebooks` runs every `docs/**/*.ipynb` that is not explicitly excluded, and this notebook is not excluded.
  - The harness uploads the notebook unchanged and runs it as a non-interactive job with no `CONTENT_UNDERSTANDING_*` variables.
  - The configuration cell then falls back to `input()`/`getpass.getpass()`. In a job these fail; an empty answer raises the cell's `ValueError`; otherwise the run blocks until the 20-minute timeout. Either way `runNotebook` throws.
  - `e2e_impact.py` sends both this path and `cognitive/src/main/**` to `ALL_SUITES`, and `testDatabricksE2E` defaults to true. So `DatabricksCPUE2E` runs on this PR, and after merge on every PR whose changes are not on the skip list.
  - The exclusion-based Synapse harness would also pick it up if run.
  - Other AI Services notebooks use `find_secret`. This one needs a resource and storage supplied by the user. If credentials were injected, each run would create a billable `synapseml-purchase-order-<run_id>` analyzer that is never deleted (L395).
- **Risk**: One Databricks CPU partition will fail every time, keeping the full build red for this PR and later PRs. There would also be pressure to wire in shared secrets, which would leak analyzers onto a shared resource.
- **Suggested Fix**:
  - In `DatabricksUtilities.CPUNotebooks`, add `.filterNot(_.getAbsolutePath.contains("Semantic Extraction with Content Understanding")) // Requires a user-supplied CU resource and creates an analyzer`.
  - Add `"SemanticExtractionwithContentUnderstanding"` to `SynapseTests.excludedNotebooks`.
  - No `pipeline.yaml` change is needed; the Fabric list only includes named notebooks.
  - Keep the loopback notebook test (`test_ContentUnderstanding.py` ~L519-617) as the offline validation.

### Issue 2: Unlimited opt-in 429 recovery leaves no log or result trace
- **Severity**: Medium
- **File**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingBudget.scala`; `.../ContentUnderstandingProtocol.scala`; `.../ContentUnderstandingLogging.scala`; `docs/Explore Algorithms/AI Services/Content Understanding.md`
- **Line(s)**: Budget L16-32; Protocol L338-356; Logging L11-36; ResponseParsing L89-96; guide L367-386, L484-490
- **Description**:
  - With `setRetryThrottledRequests(True)`, throttled POST/GET requests are retried indefinitely inside `exchange`, with no log at any level and no counter.
  - The output row shows nothing either, because absorbed 429 responses never reach `safeSubmission` or `retainPollState`.
  - `Retry-After` is honoured up to `Long.MaxValue` ms, and HTTP dates are honoured too.
  - Guide L484-490 says "Detailed diagnostics remain in the output row or exception response", which is not true for absorbed throttling.
- **Risk**: A persistently throttled or quota-exhausted resource, or a proxy sending a very long `Retry-After`, makes tasks look hung with no executor evidence. Operators can't tell throttling apart from a hang, can't size concurrency, and may kill and rerun jobs. That repeats billable submissions for rows that were not saved. Long throttled tasks are also more likely to trigger Spark speculation, which reruns the partition.
- **Suggested Fix**:
  - Add a sanitised, rate-limited `ContentUnderstandingLogging.throttled(method, retryCount, waitMs, cumulativeWaitMs)`. It should include no URLs, headers, bodies or keys, and should add Spark task IDs when available, like `sharedStop`.
  - Call it from the retry loop on the first retry, then periodically (for example at power-of-two counts, or at most once a minute per operation), at WARN.
  - Test it with `withLogs` in `ContentUnderstandingThrottlingSuite`, asserting the event appears and contains no secrets or URLs.
  - Mention throttling events in guide L484-490.
  - Optionally, note in the opt-in section that long waits can trigger speculation, and recommend disabling it for CU actions.

### Issue 3: `pollingDelay` doc omits its role as the 429 fallback, and retries can hot-loop or fire in lockstep
- **Severity**: Low
- **File**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingParams.scala`; `.../ContentUnderstandingProtocol.scala`; `docs/Explore Algorithms/AI Services/Content Understanding.md`
- **Line(s)**: Params L114-121; Protocol L280-287, L352-355; guide L479 (and L381-382)
- **Description**:
  - Opt-in recovery waits for `retryDelay(raw.retryAfter, settings.pollingDelay)`, so `pollingDelay` is now also the fallback wait for analysis POST/GET 429s.
  - The param doc and the `setPollingDelay` table row list every other use of `pollingDelay` but not this one; only the throttling paragraph mentions it.
  - `pollingDelay` accepts 0 (`gtEq(0)`), and a valid `Retry-After: 0` is honoured as 0. With unlimited retries, either case gives back-to-back requests to a throttled endpoint.
  - Concurrent workers that get the same `Retry-After` all retry at the same moment (no jitter).
- **Risk**: Users who set `pollingDelay=0` for fast polling unknowingly remove the throttle back-off. That wastes executor CPU, churns connections, and adds load that can prolong throttling.
- **Suggested Fix**:
  - Update the `pollingDelay` param doc and guide row L479 to say it is also the opt-in 429 fallback when `Retry-After` is missing or invalid.
  - Within opt-in recovery only, consider a minimum fallback (for example `max(pollingDelay, 1000)`) or capped exponential back-off when `Retry-After` is absent or zero.
  - Consider adding small positive jitter on top of `Retry-After`; waiting longer still honours it.
  - Add a stub test with no `Retry-After` and `pollingDelay=0` that asserts a bounded request rate.

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

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "Primary estimate; neither human nor repository selects a tier",
    "models": "common:review-code Medium complementary-family rule, resolved against current task tool catalog",
    "stageBehavior": "common:review-code Medium six concurrent independent lenses",
    "safetyStop": "No new timeout, credit, or iteration bound; user prohibits introducing timeouts",
    "delivery": "Human iteration-mode instruction: no commit/push without explicit approval"
  },
  "changeSize": "Thirteen source/test/documentation files relative to PR head, including three untracked additions. Tracked diff is 602 additions and 188 deletions; most volume is tests and documentation. Production changes are localized to four CU Scala files and removal of four Python inspection overrides.",
  "cyclomaticComplexity": "Moderate additions to existing flows: cancellable tail-recursive 429 retry, elapsed-budget refund, GET/POST eligibility, optional Boolean scalar/column query values. Existing transport, request deadlines, task ownership, and management state machines are reused rather than redesigned. Shared Python inspection has cross-connector compatibility implications.",
  "rationale": "Six independent lenses are justified by authentication, budget/cancellation, persistence and notebook interactions. Scope is the incremental uncommitted review fixes, not a new whole-connector implementation. Medium provides complementary review without treating documentation/test volume as a new high-complexity state machine.",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": "SynapseML AGENTS.md and .github/skills/code-review/SKILL.md require checks and evidence but specify no tier/model override. Existing user iteration-mode policy overrides the skill's automatic commit/PR step.",
  "sourceHead": "41b54ec47d514aac82494c70ed2963a8bc69ff5d",
  "target": "master",
  "artifactDirectory": "reviews/pr-2815",
  "uncertainty": "Source-level offline review cannot establish live CU contract compatibility; live validation remains blocked on endpoint and secure credentials."
}
