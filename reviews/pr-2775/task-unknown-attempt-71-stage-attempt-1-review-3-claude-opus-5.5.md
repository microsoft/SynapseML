---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 71
stage_attempt: 1
round: 3
timestamp: "2026-10-19T23:01:49Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "9a2151a28eb1e2a0f261f058d0b564d20853fff444281020f8c9de6c5028acbb"
---

I found no defects in this robustness pass, so the verdict is CLEAN with 0 issues. The review is below; the runner writes the artifact.

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-71-stage-attempt-1-review-3-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Response deadline and cleanup** (`.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py` 119–166).
  - `DeadlineReader` wraps the stream returned by `HTTPResponse.fp.detach()`.
  - Before every `readinto` (headers, chunk sizes and body), it calls `settimeout(remaining_timeout())`. `close()` closes the original stream.
  - I traced both expiry paths:
    - An `EvidenceError` during `begin()` hits `getresponse`'s `except: response.close()`.
    - An `EvidenceError` during `read()` exits the `with` block, which releases the socket reference and fully closes the socket.
  - `context=self._context` keeps the default verified TLS context on Python 3.10–3.12+.
  - `NoRedirect` refuses a redirect before credentials can be forwarded.
  - No timer or worker thread was added.
- [x] **Deadline-aware error mapping** (`collect_ci_evidence.py` 180–242).
  - `remaining_timeout()` raises at ≤0 before any credential fetch or request (test `test_expired_deadline_does_not_fetch_credentials`).
  - Transport, protocol and JSON errors recheck the deadline first. They then map `TimeoutError` or `URLError(reason=TimeoutError)` to the sanitized timeout message, and everything else to the generic one.
  - `HTTPError` reports only its status code.
  - A timeline 404 or empty body returns pending, never passing, because `jobGatesPassed` requires `bool(jobs)` at line 543.
- [x] **Streaming regression test** (`tools/ci/tests/test_collect_ci_evidence.py` 1087–1170).
  - It uses real `HTTPConnection`/`HTTPResponse` objects over `socket.socketpair()`. The body is sent only after `response_started`, and the clock is injected.
  - It is deterministic: the expired case must raise after at most 2 of the 12 body bytes.
  - It cannot hang CI: sockets have 2-second timeouts, the sender is a daemon thread, `join(timeout=3)` is bounded, and `is_alive` is asserted.
  - `reader.fileno() == -1` proves the socket was actually closed.
- [x] **Collector provenance** (`collect_ci_evidence.py` 244–269, 328–544, 556–741, 789–818). Edge cases are fail-closed:
  - pagination raises on duplicate IDs;
  - `totalTests != len(results)` catches truncated result pages;
  - a test-run attempt newer than the timeline raises;
  - a retried job with a missing current publication raises;
  - a test named by `--expected-test` cannot be skip-exempted;
  - timeline parent cycles and malformed count, order or issue fields raise;
  - the highest attempt is kept per type and identifier;
  - `collectionStable` is required, the deadline is rechecked at the end, and `mergeReady` is always `False`.
- [x] **Watcher state machine** (`.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py` 97–277, 370–385).
  - Every return path rechecks the deadline.
  - `wait_for_check_update` allows one 600-second confirmation poll, only if it starts strictly before the deadline, and resets when Azure is not complete.
  - The timeout result carries the last collected Azure evidence.
  - The exit map covers all nine outcomes and matches `references/ci-triage.md` 38–75 (`azure_completed`/`attention_required` exit 4, `timeout` exit 124).
- [x] **Guidance verifier** (`.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py` 40–179).
  - It skips relative PATH entries and any inside the current directory, and runs git with `GIT_*` variables removed, `--no-replace-objects` and `core.fsmonitor=false`.
  - It enforces the HEAD pin, regular files only, an index matching the pinned tree with no conflicts, no untracked or ignored files, and no symlinks. CRLF is tolerated only for text files.
  - All `REQUIRED` resources and the root `CODEOWNERS` exist.
  - The tests import the helpers with `dont_write_bytecode` patched (test_collect line 32, test_watch line 31, test_trusted_guidance line 22). Test runs therefore cannot leave `__pycache__` that would trip the untracked-file check.
- [x] **Readiness policy fields** (`.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1` 305–369, 559–569, 664–668).
  - Policy collection failures go to `collectionErrors`, and `$rulesComplete` then sets `humanApproval` to `unknown`.
  - `ruleRequiredChecks` and `classicProtection` are exported. This matches the exempt-diff interpretation in `references/readiness-gates.md` 125–137 and `SKILL.md` 279–287.
  - The Azure check must match exactly, including case.
  - The `-RunPipeline` guards match `references/ci-evidence.md` 94–128.
- [x] **CI coverage**: in `pipeline.yaml` lines 112–126, the `CIHelpers` job runs `python3 -m pytest tools/ci/tests/ -q`, so the four changed test files run in Azure. The supplied hosted evidence (383 passed, 2 Windows-only skips) is consistent with this.
- [x] **Anchors resolve**:
  - `SKILL.md#7-run-and-triage-full-ci` and `#validation-only-handoff`
  - `ci-evidence.md#comment-trigger-gate`, `#github-snapshot` and `#azure-merge-and-named-tests`
  - `readiness-gates.md#review-and-validation` and `#user-value-and-scope`
  - `loop-control.md#artifact-retention`, `#state-and-cost-discipline` and `#evidence-invalidation`
  - `ci-triage.md#waiting-for-azure-pipelines` and `#targeted-infrastructure-retry`
- [x] **Examined and rejected as defects** (each is fail-closed or documented):
  - Connect and TLS setup use the budget computed when the request opens, so a slow connection can overrun the deadline by a bounded amount. The final deadline recheck still turns that into an error.
  - The duplicate-trigger race and the ambiguous workflow association for reused fork branches are both documented.
  - Timeline warnings block `jobGatesPassed` and require a manual disposition; this is documented.
  - Quoted Windows PATH entries are skipped, which produces a clear error.
- [ ] **Not executed**: this was a static review. I ran no tests or commands; runtime results come from the supplied local and hosted evidence.
- [ ] **Truncated inline diff**: the prompt's diff was cut off after the `SKILL.md` stage-8 hunk. I reviewed the files on disk instead, but could not hash-check them against the frozen index tree or the source-diff fingerprint.
- [ ] **Live Azure formats**: I could not compare the test-run `phaseName.jobName` key with a real timeline Job `identifier`. A mismatch fails closed with "lacks matching job-attempt provenance".
- [ ] **Native CLI behaviour**: `gh`/`az` process behaviour (Windows batch process-tree cleanup, PowerShell native stderr) was not exercised.
- [ ] **Test files only partly read**: I inspected the key regression sections of the four `tools/ci/tests` files, not every test case line by line.