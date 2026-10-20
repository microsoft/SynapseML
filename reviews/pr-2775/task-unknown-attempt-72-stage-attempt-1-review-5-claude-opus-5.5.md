---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 72
stage_attempt: 1
round: 5
timestamp: "2026-10-20T00:13:22Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "3dd415d9c81e8bbced8c4dcc054a47e7781a1c85c380cbfcbaa118f7470faad1"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-72-stage-attempt-1-review-5-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Watcher outcomes vs tests.** I read `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py` in full and matched every outcome of `monitor()` and `main()` to a test in `tools/ci/tests/test_watch_azure_pipeline.py`.
  - In `test_agent_loss_surfaces_before_job_or_build_is_terminal`, each (identifier, attempt) pair reports agent loss once. The test sees attempts [1, 2], four 600 s sleeps, and ends at t=2400 as `azure_completed`/`pending`.
  - In `test_timeline_completion_allows_one_check_update_before_the_deadline`, a start at 6599 waits once for the GitHub check and a start at 6600 does not. Query or collection latency returns 124 and keeps the last Azure evidence.
  - Also tested: a GitHub/Azure disagreement returns `attention_required`. The merge is verified only once, with pinned SHAs and a clipped timeout. An `EvidenceError` becomes `timeout` at the deadline and `error` before it.
- [x] **Collector deadline handling.** I checked `DeadlineReader`, `DeadlineHTTPSHandler` and `AzureClient.get` in `collect_ci_evidence.py` (lines 126-242) against `test_streaming_response_stops_at_the_original_deadline`.
  - The test uses a real `HTTPConnection`/`HTTPResponse` over `socket.socketpair()`. Headers are parsed before the body is sent, and the clock moves past the deadline mid-body.
  - Without the per-`readinto` check, the expiry subtest would return `{"value": []}` and fail.
  - `reader.fileno() == -1` only holds once `DeadlineReader.close()` closes the detached `SocketIO`. urllib's `h.sock.close()` alone leaves the socket open while the makefile reference exists.
  - The four cases in `test_http_timeout_errors_report_deadline_or_query_timeout` reach the deadline recheck for both opening (`URLError(TimeoutError)`) and reading (`TimeoutError`), at times 99.5 and 100.
- [x] **Collector entry point and auth limits.** `main()` uses a 600 s deadline and calls `client.remaining_timeout()` before printing.
  - `test_final_collection_must_finish_before_the_original_deadline`: finishing at 699 passes; 700 and 701 return the exact error object.
  - `test_expired_deadline_does_not_fetch_credentials` is also covered.
  - `test_client_requests_only_allowlisted_evidence_with_bounded_authentication` checks the endpoint allowlist before any token or request, a token timeout of 20 (120 − 100), and the exact query URL.
  - The redirect test confirms no follow-up `open` call.
- [x] **Readiness harness.** `tools/ci/tests/test_get_pr_readiness.py` (lines 249-300) runs the real `Get-PrReadiness.ps1` under `pwsh` with a fake `gh`. For every fixture it asserts that the `/azp run` comment and the Copilot review request happen exactly when expected, with exact arguments. `skipUnless(which("pwsh") or TF_BUILD)` makes a missing `pwsh` a failure in Azure Pipelines, as `ci-triage.md` states.
- [x] **Doc-driven isolation tests.** `test_documented_bootstrap_rejects_shadow_imports_before_execution` and `test_documented_credential_helpers_ignore_untrusted_pythonpath` read the actual command flags from `trusted-guidance.md`, `ci-evidence.md` and `ci-triage.md`. They run the helpers with a fake `json.py` on `PYTHONPATH`, so docs that drop the isolation flags fail a test. No test parses the CIHelpers pytest command.
- [x] **No-publisher premise.** In `pipeline.yaml` (lines 112-126), the `CIHelpers` job runs `python3 -m pytest tools/ci/tests/ -q` with no `PublishTestResults@2` task and no JUnit output; the publishers from line 401 onward belong to other jobs. `SKILL.md` (lines 264-266) and `loop-control.md` (lines 254-255) link to `ci-evidence.md#jobs-without-published-test-results` (lines 243-271) and agree with it.
- [ ] **Tests not executed.** This was a read-only review. I did not reproduce the supplied Windows/Linux pytest results or inspect the predecessor's hosted build logs.
- [ ] **Partial file coverage.** The diff was cut off in the prompt, so I read the remaining sources directly by path. I assessed `Get-PrReadiness.ps1` and `verify_trusted_guidance.py` from their diff hunks and tests rather than re-reading them in full.
- [ ] **Not counted as defects.** The behavior below is correct; only an extra assertion could be added.
  - The streaming test swaps in a plain `HTTPConnection`, so the TLS `context` argument (default certificate verification) is correct on inspection but not pinned by a test.
  - The collector tests don't assert the "deadline expired" message for a non-timeout protocol/JSON error after expiry. The four timeout cases exercise the same recheck line.
- [ ] **`reviews/` and other reviewers' artifacts not read**, per the independence rules.

## Issues

### Issue 1: The CIHelpers local-validation command hides the test names its linked contract requires
- **Severity**: Low
- **File**: `.github/skills/synapseml-pr-loop/references/ci-triage.md`
- **Line(s)**: 105-112, with the command on line 111. The contract is in `.github/skills/synapseml-pr-loop/references/ci-evidence.md`, lines 250-252 and 154-155.
- **Description**: The CIHelpers paragraph says to apply the contract for jobs without published results, then gives `python -B -m pytest <four suites> -q -rs` for local validation. That contract requires the named regressions to run "with names and skip reasons visible, for example `pytest <test-node-ids> -v -rs`".
  - `-q` hides the per-test result lines, and `-rs` only adds a grouped skip summary by location and reason.
  - The documented command therefore gives counts only: no PASSED line per test and no name for each skipped test.
  - The PR's own local proof used `-v -rs`, so the documented command matches neither the contract nor actual practice.
  - No test parses this line; the doc-driven tests only read the collector, watcher and trusted-script commands.
- **Risk**: CIHelpers is the job this contract was written for.
  - An agent that copies the command will record output like "N passed" as named local proof. `ci-evidence.md` rejects exactly that: "An increased count alone cannot prove that regression was discovered."
  - A regression that was never collected or was shadowed, for example by a duplicate unittest method name, would not show up by name.
  - At best, the agent has to rerun the suites to get acceptable evidence.
- **Suggested Fix**: Change `-q -rs` to `-v -rs` on line 111; no test reads this line. Alternatively, add the node-ID form (`python -B -m pytest <test-node-ids> -v -rs`) and say that the saved output must show the outcome of each required test.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
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
    "stageBehavior": "pinned target PR-loop sequential six-lens policy",
    "bounds": "explicit user continuation at 2026-10-19T19:48 after the reported blocker; one bounded documentation fix/publication cycle, not a counter reset or review waiver"
  },
  "changeSize": "17 source, test, and documentation paths; the only new follow-up is four existing documentation files",
  "cyclomaticComplexity": "Moderate provenance, pagination, retry, readiness, deadline and filesystem interactions; follow-up changes no executable behavior",
  "rationale": "Review the full patch after making the named-evidence contract usable for jobs without result publishers, without weakening published-result, authorization or CI-only validation gates",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/SKILL.md at d34f762a785ee8a7c7c624806f8c112256168833; explicit user continuation after the bounded stop",
  "reviewAttempt": 72,
  "completedPriorPrecommitPasses": 70,
  "consumedFinalCiQualifiedPasses": 1,
  "completedFinalCiQualifiedPasses": 0,
  "roundOrder": [5, 6, 1, 2, 3, 4],
  "verifiedTaskId": null
}

## Driver disposition

Issue 1 is retained as a nonblocking command-convenience suggestion, not an
unresolved evidence-gate defect. The four-suite command is labelled "For local
validation"; it does not claim that its aggregate output is named regression
proof. The immediately preceding link requires the separate nonpublishing-job
contract, whose local-evidence step explicitly requires visible names and skip
reasons and supplies `pytest <test-node-ids> -v -rs`. Running only the quiet
suite command therefore cannot satisfy that contract.

These commands serve different scopes: the existing quiet command checks the
four helper suites, while the named command records the required regressions.
The latter need not rerun all four suites. Using `-v` for the whole suite, as
the driver did here, is another valid way to collect both kinds of evidence,
not the only permitted workflow. A copied aggregate count remains insufficient
under the explicit linked rule, so the suggested mistaken readiness claim
would violate the contract rather than be authorized by it.

No source change is needed to enforce the gate. The supplied named Windows and
Linux results already meet it. The original `ISSUES_FOUND` verdict is preserved;
this disposition does not turn quiet output into named evidence or claim a
literal clean reviewer verdict. The frozen source fingerprint is unchanged.

### Final disposition after independent documentation review

The preceding disposition is superseded, not removed. A second independent
lens identified the same ambiguity in the copyable command immediately after
the contract link. Although the linked gate rejects count-only proof, making
the provided command satisfy it directly is clearer and avoids unnecessary
extra runs. The driver accepted that documentation correction and changed
the existing command from `-q -rs` to `-v -rs`.

This matches the named Windows validation already completed and does not alter
test selection or executable behavior. Attempt 72 remains incomplete and is
not final evidence; its original findings and dispositions are retained.
