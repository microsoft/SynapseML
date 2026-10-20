---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 74
stage_attempt: 1
round: 5
timestamp: "2026-10-20T01:09:36Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "ea0e55c9ed7d39551b31cdf7bf22038de381adc12215dfb916f65d4f62b5fb70"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-74-stage-attempt-1-review-5-claude-opus-5.5.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **CIHelpers really has no test-results publisher.** In `pipeline.yaml` (lines 112-126), CIHelpers installs only `pytest pyyaml` and runs `python3 -m pytest tools/ci/tests/ -q`. It has no `PublishTestResults` step; the first one belongs to a later job at line 401. This matches the aggregate-only premise in `references/ci-evidence.md` ("Jobs without published test results"). The copyable local command in `references/ci-triage.md` (lines 105-116) keeps `-v -rs`.
- [x] **The streaming deadline test is sound.** I traced `test_streaming_response_stops_at_the_original_deadline` (`tools/ci/tests/test_collect_ci_evidence.py` 1087-1180) against `DeadlineReader`, `DeadlineHTTPSHandler` and `AzureClient.get` (`collect_ci_evidence.py` 126-242):
  - The real `HTTPConnection` is already bound to the socket pair, so `connect()` is skipped and the socket keeps its 2-second timeout. In the stall case, only the per-receive `settimeout(0.2)` can end the read before 1.5 seconds, which matches the reported 2.015-second mutation failure.
  - In the expire case, the clock jumps after the first byte, so `remaining_timeout()` raises before the body finishes.
  - `fileno() == -1` shows that closing the response closes the socket descriptor through `DeadlineReader.close`. The sender thread is joined and its errors are checked.
- [x] **Both timeout outcomes are pinned by exact message.** `test_http_timeout_errors_report_deadline_or_query_timeout` (1263-1300) covers a wrapped `URLError(TimeoutError)` from `open` and a plain `TimeoutError` from `read`, each with the deadline expired and unexpired, against the classifier at lines 229-237.
- [x] **The two new readiness tests are not vacuous.** I traced them (`test_get_pr_readiness.py` 513-567) through the harness (from line 21, plus `snapshot()` at 253-306) and `Get-PrReadiness.ps1`: classic `checks[].context` extraction at line 362, missing/unproven checks at 468-474, `classicProtection` and `ruleRequiredChecks` output at 667-668, and `ConvertTo-Json -Depth 12` at 758, which is deep enough for the nested protection object.
  - SUCCESS clears every gate; None, SKIPPED and NEUTRAL each fail a different field.
  - The ruleset, classic and none cases pin both policy fields exactly.
  - The harness fails the test if the script posts a CI trigger or review request that was not allowed.
- [x] **The readiness skip guard matches the docs.** `skipUnless(shutil.which("pwsh") or TF_BUILD)` (`test_get_pr_readiness.py` 249-251) matches ci-triage.md: missing `pwsh` fails in Azure Pipelines and skips locally.
- [x] **Every new watcher branch has a test.** I read all of `tools/ci/tests/test_watch_azure_pipeline.py` and `monitor()` in `watch_azure_pipeline.py` (98-275):
  - merge verified only once: `test_merge_verification_uses_pinned_revisions_once`
  - `EvidenceError` reported as error or timeout depending on the deadline: `test_truncated_azure_response_finishes_with_error_or_original_timeout`
  - agent-loss events reported once per attempt: `test_agent_loss_surfaces_before_job_or_build_is_terminal`
  - Azure still running while GitHub has finished, and GitHub/Azure agreement or disagreement: `test_timeline_results_wait_for_retry_and_distinguish_agreement`
  - wait-flag reset, the 600-second boundary and query/collection latency: `test_timeline_completion_allows_one_check_update_before_the_deadline`
  - missing GitHub check: `test_absent_aggregate_can_be_observed_but_never_passes`
- [x] **Trusted-guidance tests stay tied to the docs.** `test_documented_bootstrap_rejects_shadow_imports_before_execution` reads its Python flags from `references/trusted-guidance.md`. The verifier's error paths map to named tests, except the index-conflict check (`verify_trusted_guidance.py` 126-127). I did not raise that gap because file contents are still compared byte for byte (lines 128-155).
- [x] **The untested paths in the findings are confirmed by search.** Searching `tools/ci/tests` for "failed or returned invalid JSON", "no valid merge parent", "no test name" and `URLError(` finds no assertion of those messages. The only `URLError` fixture wraps a `TimeoutError` (line 1273).
- [ ] **No tests, mutations, Black or link checks were executed.** This was a read-only review. The author's reported results are not reproduced here: 3 tests and 10 subtests on Windows, the Linux streaming pass, the four mutations, and the earlier 145-test run.
- [ ] **No hosted CI covers the pending patch.** Azure build 241706733 ran on predecessor head `0487209b131885d5b06b7036300acdc4ce37b09d`. The new head still needs CI and remote review.
- [ ] **Some files were not re-read in full.** The diff display was truncated, so I reviewed the manifest scripts and tests from their on-disk files. The two external-contributor docs, `readiness-gates.md`, `review-gauntlet.md` and `loop-control.md` were checked only through the visible diff and targeted searches. In `collect_ci_evidence.py`, timeline summarization (lines 328-546) was not re-read line by line.
- [ ] **GitHub Actions workflows were not inspected.** They are outside the allowed search scope, so I cannot confirm whether any non-Azure runner executes `tools/ci/tests` without `pwsh`, where the readiness suite would skip. Per the independence rules, I did not open `reviews/`, other reviewers' artifacts or session evidence.

## Issues

### Issue 1: No test asserts the generic Azure failure message, so a failure mislabeled as a timeout would go unnoticed
- **Severity**: Low
- **File**: `tools/ci/tests/test_collect_ci_evidence.py`; code under test: `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`
- **Line(s)**: tests 564-589 and 1238-1261; code 229-237
- **Description**: `AzureClient.get` sorts transport, protocol and JSON failures into three sanitized messages:
  - "deadline expired"
  - "query timed out"
  - "Azure evidence query failed or returned invalid JSON."

  Tests pin the first two exactly. Tests do reach the third: `IncompleteRead`, `BadStatusLine` and `LineTooLong` at 1238-1261, and an empty non-timeline body at 564-574. But those tests only check that an `EvidenceError` is raised or that private text is absent. The generic message appears in no test under `tools/ci/tests`. No test uses a non-timeout `URLError`, because the only `URLError` fixture wraps a `TimeoutError`.
- **Risk**: A change that labels every failure "query timed out" would keep the suite green. Examples: widening the `isinstance(error, TimeoutError)` check to `OSError`, or dropping the `error.reason` check for `URLError`. That breaks the stated contract that other errors stay generic. TLS, DNS, connection-refused or invalid-JSON failures would then look like transient timeouts, steering CI triage toward waiting or re-running instead of fixing configuration or authentication.
- **Suggested Fix**:
  1. In `test_protocol_and_truncated_body_errors_are_sanitized`, assert `"Azure evidence query failed or returned invalid JSON."` exactly for each protocol error and for the `BadStatusLine` raised from `open`.
  2. Add cases where `open` raises `URLError(ConnectionRefusedError("private"))` and where the body is `b"private not-json"`. With the deadline unexpired, assert the same generic message and no private text.

### Issue 2: The `verify_merge` structure check and returned-SHA check have no negative tests
- **Severity**: Low
- **File**: `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`; `tools/ci/tests/test_collect_ci_evidence.py`
- **Line(s)**: code 272-284; tests 171-183 and 1359-1374
- **Description**: The merge-provenance tests cover only reversed parents and the correct order. No test supplies any of these:
  - a commit that is not a dict (`None`, `[]`)
  - `parents` that is not a list
  - parent entries that are not dicts
  - a commit whose `sha` differs from `--merge-sha` while the parents are in the right order

  "GitHub returned no valid merge parent evidence." appears in no test. Deleting lines 274-279, or the `commit.get("sha") != args.merge_sha` comparison on line 281, leaves every test passing.
- **Risk**: Without the structure check, a malformed `gh api` response raises `AttributeError`. Neither helper catches it:
  - The collector's `main` catches only `EvidenceError` (line 813).
  - The watcher's `monitor()` catches only `EvidenceError` (line 158), and its `main` catches only `MonitorError` and `KeyboardInterrupt` (lines 370-373).

  Both would crash with a traceback instead of printing their final JSON result; the watcher would print `started` but never `finished`. The documented workflow reads that final JSON. Without the SHA comparison, nothing ties the returned commit to the requested merge commit.
- **Suggested Fix**:
  1. Extend `test_wrong_source_branch_revision_repository_or_merge_parents_fails` with responses `None`, `[]`, `{"sha": MERGE, "parents": {}}` and `{"sha": MERGE, "parents": [BASE, HEAD]}`, each asserting "no valid merge parent evidence".
  2. Add `{"sha": "d" * 40, "parents": [{"sha": BASE}, {"sha": HEAD}]}`, asserting the parent-mismatch message.
  3. Optionally, copy the entry-point test at 1359-1374 with `command_json` returning `None`, and assert the final error JSON.

### Issue 3: The malformed-field test never checks a malformed test-result name
- **Severity**: Low
- **File**: `tools/ci/tests/test_collect_ci_evidence.py`; `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`
- **Line(s)**: tests 494-544 (field matrix at 495-506) and 431-434; code 637-639, 646-653 and 666-678
- **Description**: `test_malformed_test_scalars_emit_sanitized_final_error_json` covers the result fields `id`, `outcome` and `state`. It omits `automatedTestName` and `testCaseTitle`, which are what the "Test result has no test name." check validates. The only name test (431-434) is the positive `testCaseTitle` fallback. No test supplies a missing, empty or non-string name, and the error message appears in no test. Deleting lines 638-639 leaves the suite green.
- **Risk**: Without the check, a list or dict name is copied into `runs[].results[].name` and printed in the collector's JSON output. That leaks the response content that the neighboring `assertNotIn("private diagnostic", ...)` checks exist to keep out. A missing name becomes the identity `<suite>::None`. `references/ci-evidence.md` promises that malformed nested Azure fields produce sanitized error evidence, and no test would catch this regression.
- **Suggested Fix**: Add `("result", "automatedTestName")` to the matrix, reusing its `["private diagnostic"]` and `{"private diagnostic": 1}` values. Add one case where both name fields are absent or empty. Assert the final `error` JSON, the exact "Test result has no test name." message, and no private text.

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
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "pinned target sequential six-lens policy",
    "bounds": "Explicit user autonomous continuation at20:48 after two coverage findings; one additional bounded test-fix/precommit cycle, no review waiver or unlimited renewal"
  },
  "changeSize": "17 source/test/documentation paths; 5958 added and 209 removed lines; pending follow-up only four docs and two tests",
  "cyclomaticComplexity": "Moderate evidence-provenance, pagination, retry, readiness, deadline and filesystem interactions; follow-up changes no executable behavior",
  "rationale": "Review the full evidence contract and focused timeout/classic-policy regressions, with retained full manifest and named local proof",
  "primaryModel": "gpt-6-astra",
  "reviewers": [
    "claude-opus-5.5"
  ],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/SKILL.md at d34f762a785ee8a7c7c624806f8c112256168833; explicit user continuation on 2026-10-19",
  "reviewAttempt": 74,
  "completedPriorPrecommitPasses": 72,
  "consumedFinalCiQualifiedPasses": 1,
  "completedFinalCiQualifiedPasses": 0,
  "roundOrder": [
    5,
    6,
    1,
    2,
    3,
    4
  ],
  "verifiedTaskId": null
}

## Driver dispositions

The original `ISSUES_FOUND` verdict and all three coverage observations above
are retained. These are **nonblocking coverage suggestions**, not defects found
in the frozen implementation. Each proposed failure requires deleting or
widening a guard that is present and correct. The task requires proof of the
changed behavior and focused regressions, not mutation coverage of every
defensive branch or exact diagnostic. No source change was made during this
triage, and no finding is being represented as a newly committed regression.

Additional direct validation exercised 13 reported negative cases against the
same frozen patch. All passed; the source tree remained
`87aae225c470b7b854598440a715bd29c85e4a98`.

### Issue 1: Nonblocking; current error classification verified

`AzureClient.get` checks `TimeoutError` and a wrapped `TimeoutError` specifically
before emitting the generic sanitized message. A connection-refused `URLError`
and invalid JSON with an unexpired deadline both returned exactly
`Azure evidence query failed or returned invalid JSON.`, with no diagnostic
payload. Existing protocol tests already check the error boundary, sanitization
and response closure. Pinning every generic-message path in the repository
suite would be additional hardening; no current timeout misclassification was
reproduced.

### Issue 2: Nonblocking; current provenance guards verified

The structure check precedes all parent access, and both returned commit SHA
and ordered parents are compared before Azure queries. The collector entry
point was exercised with null/list commits, non-list parents, scalar parent
entries and the wrong returned SHA with correctly ordered parents. Every case
returned the expected final error JSON and exit code 1, without constructing an
Azure client. No traceback or provenance bypass was reproduced. The existing
suite covers the positive ordered merge and the reversed-parent rejection
through both the helper and entry point.

### Issue 3: Nonblocking; current name validation verified

The name type/nonempty check runs before copying the name into output rows.
Missing names, empty names, list/object primary names and list/object fallback
names were exercised through the collector entry point. All six returned
`outcome: error`, exit code 1 and `Test result has no test name.`, with no
diagnostic payload in the JSON. No malformed name was published. The proposed
matrix expansion is optional additional permanent coverage, not a correction
to the current validation.

### Review provenance and continuation

The full supplied diff and receipt hashes were verified, as were the recorded
model and source-only tool accesses. The review's disclosed reading and
execution limitations remain visible. The supplementary cases are local
validation, not hosted CI or new committed tests. Continue the remaining five
lenses on the unchanged patch; this disposition neither resets the attempt
counter nor substitutes for the final CI-qualified review.
