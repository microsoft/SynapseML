# Review Round 5 — Testing & Coverage

## Summary
- Round: 5
- Theme: Testing & Coverage
- Mode: manual prompt fallback
- Requested model: claude-opus-5.5 (max)
- Issues: 3
- Verdict: CHANGES REQUIRED

## Evidence Checklist
- [x] `tools/ci/tests/test_collect_ci_evidence.py:168` — A documented skip cannot excuse a named `--expected-test`, and a `NotExecuted` result blocks.
- [x] `tools/ci/tests/test_collect_ci_evidence.py:220` — The leaf Phase/Stage conditional gates are enforced and can't supply attempts. A parent cycle raises `EvidenceError`.
- [x] `tools/ci/tests/test_collect_ci_evidence.py:373` — A retry during collection sets `collectionStable` and `validationPassed` to false and exits with code 1.
- [x] `tools/ci/tests/test_watch_azure_pipeline.py:538` — An agent loss is printed once while the run is in progress. The sleeps are `[600, 600]` and the run ends as `azure_completed`.
- [x] `tools/ci/tests/test_watch_azure_pipeline.py:643` — An active Azure retry outranks a stale GitHub FAILURE. The result is `attention_required` after one 600 s sleep.
- [x] `tools/ci/tests/test_get_pr_readiness.py:313` — A behind or stale `baseRefOid` still gives `evidenceComplete`, but the remote gates stay blocked. The trigger fires only when the aggregate check is missing.
- [x] `pipeline.yaml:125` runs `python3 -m pytest tools/ci/tests/ -q`, so these test suites gate CI.

## Issues

### Issue 1: No tests check that `-RunPipeline` refuses to trigger CI
- **Severity:** High
- **File:** `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`, `tools/ci/tests/test_get_pr_readiness.py`
- **Lines:** Get-PrReadiness.ps1:97-102 and 496-501, which run before `gh pr comment … /azp run` at 504-508; test_get_pr_readiness.py:57, 153, 270, 313
- **Risk:** The test helper at :153 requires exit code 0. The fake `gh pr comment` at :57 throws unless the test expects a trigger. So only two successful paths run: :270 (aggregate present, no trigger) and :313 (aggregate missing, trigger). The suite still passes if you delete either set of guards:
  - the `evidenceComplete` and `OPEN` checks at 496-501
  - the parameter checks at 97-102 (exactly one PR, both expected SHAs, no `-WaitForReview`)

  A regression could then post a secret-bearing `/azp run` for a moved head or a closed PR.
- **Minimal fix:**
  - Let the helper accept nonzero exit codes.
  - Make the fake comment record a marker instead of throwing.
  - For each blocking case, assert a nonzero exit, the guard's message, and no marker. Cases: expected-head mismatch, live branch moved, CLOSED PR, missing expected SHA, and `-WaitForReview`.

### Issue 2: No test covers results that look green but aren't success
- **Severity:** Medium
- **File:** `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`, `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`, `tools/ci/tests/test_collect_ci_evidence.py`
- **Lines:** Get-PrReadiness.ps1:384-389 and 452; collect_ci_evidence.py:311 and 522; test_collect_ci_evidence.py:198
- **Risk:** Readiness treats SKIPPED or NEUTRAL required checks as unproven. The collector requires an exact `succeeded` result for jobs (:311) and builds (:522). No test uses SKIPPED, NEUTRAL, `succeededWithIssues`, or `partiallySucceeded`; the collector tests use only succeeded, skipped and failed. If either check were loosened to "not failed", the tests would still pass, and skipped or warning-laden evidence would count as passing.
- **Minimal fix:** Add four cases:
  - a SKIPPED aggregate check-run, expecting `evidenceComplete` false
  - a NEUTRAL ruleset status, expecting `evidenceComplete` false
  - a `succeededWithIssues` job, expecting `jobGatesPassed` false
  - a `partiallySucceeded` build, expecting `validationPassed` false and exit code 1

### Issue 3: Redirect blocking is tested on the handler alone, not on the client's opener
- **Severity:** Medium
- **File:** `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`, `tools/ci/tests/test_collect_ci_evidence.py`
- **Lines:** collect_ci_evidence.py:65 and 101; test_collect_ci_evidence.py:317, 341, 348
- **Risk:** The test at :317 calls `NoRedirect().redirect_request` on its own. The client tests replace `client.opener.open` (:341, :348). So no test checks that the client is built with `build_opener(NoRedirect())`. If that line were changed back to `build_opener()`, the tests would still pass. urllib's default `HTTPRedirectHandler` would then follow 3xx responses. It also copies the `Authorization` header set through `Request(headers=…)` at :101 to the redirect target. That leaks the bearer token and breaks the fixed-project guarantee.
- **Minimal fix:** Either assert that `AzureClient().opener.handlers` contains a `NoRedirect` and no plain `HTTPRedirectHandler`, or send a fake 302 through the real opener and expect it to be refused.

## Publication record

- PR: microsoft/SynapseML#2775
- Source publication commit: `66d08e29cc9baf9d74a71f69ce7f59c6b9f1d299`
- Reviewed input manifest SHA-256: `e30c896294c0217401bfb9b79d1186d36e8373488d8a5a4db99c57a589109609`
- Reviewed diff SHA-256: `152561506548ba7824135deea64a9854b5c4e04fc6f2756040b0647a48db6c13`
- Original artifact SHA-256: `9b56fee09e1276a1a5e10ffb535aef59287b8a3927a3e86de6e4d25f05db4b31`
- Dispatch mode: manual prompt fallback; no observed CLI receipt is claimed.
