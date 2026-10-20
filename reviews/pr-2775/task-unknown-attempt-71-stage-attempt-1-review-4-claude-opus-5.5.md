---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 71
stage_attempt: 1
round: 4
timestamp: "2026-10-19T23:22:40Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "05c699d85494c1094e320c79715600f692214497f5bbe99e4719b4e1f0e95620"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-71-stage-attempt-1-review-4-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Response deadline and transport** (`.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`, `DeadlineReader`, `DeadlineHTTPSHandler`, `AzureClient.get`):
  - The original raw `SocketIO` is detached before `begin()`.
  - Every raw receive, including headers and chunk framing, first calls `remaining_timeout()`. That call raises when the remaining time is zero or less, so `settimeout` never gets a zero or negative value.
  - Closing the response closes the original stream. Once urllib has called `h.sock.close()`, this releases the socket's file descriptor.
  - `https_open` passes only `context=self._context`, so standard certificate checks stay on.
  - `NoRedirect` replaces the default redirect handler.
  - `HTTPError` is caught before `URLError`. Other errors recheck the deadline before choosing between "deadline expired", "query timed out" and the generic message.
  - No timer or worker thread was added.
- [x] **Pagination and identity checks**:
  - `list()` advances with `$skip=len(values)` and `$top=100`, and stops on a short page.
  - It rejects bool, blank and duplicate IDs.
  - `verify_merge` requires the parents in the order base, head, plus the exact merge SHA.
  - `verify_build` pins pipeline definition 17563 using a bool-safe exact int check. It also checks the repository, `refs/pull/N/merge`, `sourceVersion`, trigger reason, queue time and status values.
  - Both `parse_args` functions limit IDs to the int32 range and lowercase full SHAs.
- [x] **Timeline, job and test gates**:
  - `summarize_timeline` keeps the highest attempt per `(type, identifier)`, detects parent cycles, and skips only containers that are ancestors of selected jobs. It adds ancestor issues and takes `max(listed, count)` for each issue kind.
  - `collect_build` blocks any job that is not completed, has issues, shows agent loss, or did not succeed (unless the skip is explicitly allowed).
  - `collect_tests` maps `phaseName.jobName` to timeline Job attempts and rejects attempts newer than the timeline. Older-attempt publications are kept only as diagnostics.
  - Each job/suite and job/test pair must be published in the current attempt. Allowed skips cannot exempt `--expected-test`.
- [x] **Final deadline boundary**: `main` sets `deadline = monotonic + 600`, re-collects the build to set `collectionStable`, always sets `mergeReady=false`, and calls `remaining_timeout()` before printing. `test_final_collection_must_finish_before_the_original_deadline` runs at 699, 700 and 701, so the exact-deadline case fails as intended.
- [x] **Regression tests** (`tools/ci/tests/test_collect_ci_evidence.py`):
  - The socket-pair streaming test patches `HTTPSConnection` with `debuglevel=0` and empty proxies. It asserts `deadline expired` before `body_finished` is set, and `reader.fileno() == -1`.
  - The 2×2 timeout test covers failures while opening and while reading, both before and at expiry. It asserts the sanitized message and `close()`.
  - The expired-deadline test proves credentials are not fetched.
- [x] **Watcher** (`.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`):
  - The merge is verified once, with a `gh` timeout bounded by the deadline. Each poll creates a fresh `AzureClient(deadline)`.
  - `allow_skipped_job` is read through a `getattr` default.
  - Agent loss is reported once per (identifier, attempt).
  - The one confirmation wait needs strictly more than 600 seconds left, and it resets while Azure is not completed.
  - Every returned outcome has an exit code.
  - `parse_build_id` enforces HTTPS, the port, no embedded credentials, a trusted path and the int32 range.
- [x] **Readiness helper** (`.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`):
  - The parameter guards match the docs.
  - `Get-RestEvidence` records failed calls, empty output and invalid JSON.
  - `rulesComplete` is captured before workflow collection.
  - Required check names come from the exact, case-sensitive Azure aggregate, branch rules and classic protection. Prefix matches are added to the same set.
  - `humanApproval` handles both singular and plural code-owner keys, and treats non-integer approval counts conservatively.
  - `-RunPipeline` requires complete evidence, an open PR and a missing aggregate (case-sensitive match).
- [x] **Guidance verifier** (`.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py`):
  - It strips `GIT_*` variables and runs Git with `--no-replace-objects` and `core.fsmonitor=false`.
  - It compares the tree with stage-0 index entries.
  - Its untracked-file listing has no exclude, so ignored files count too.
  - Symlink and regular-file checks run before any read.
  - CRLF tolerance applies only to blobs without NUL bytes.
  - `trusted_executable` skips the current directory and its subdirectories, and expands PATHEXT on Windows.
- [x] **Docs and links**:
  - Every new cross-file anchor resolves to a heading in the reference files, including `#7-run-and-triage-full-ci`, `#comment-trigger-gate`, `#review-and-validation`, `#evidence-invalidation` and `#waiting-for-azure-pipelines`.
  - The exempt-diff interpretation cites real output fields: `ruleRequiredChecks[].context` and `classicProtection.required_status_checks`.
  - The documented 15-second cleanup bound matches the three 5-second timeouts.
- [x] **Pipeline consistency**: in `pipeline.yaml`, `CIHelpers` runs `python3 -m pytest tools/ci/tests/ -q` as a top-level job. The only nested `stages:` key is a matrix leg name, so the `phaseName.jobName` mapping holds. Because the diff changes `tools/ci/tests`, it is correctly not CI-exempt.
- [ ] **Tests not run here**: this review was read-only. Execution evidence is the supplied Azure build 241706733, the CIHelpers run (383 passed, 2 skipped) and local Windows/Linux runs; I did not re-run them.
- [ ] **Truncated diff**: the prompt's diff was cut off. I reviewed the remaining changes from the current named files at the frozen checkout and did not recompute the tree or patch hashes. The Authorization header value was redacted by the viewer; its `Bearer` format was confirmed from the supplied patch text.
- [ ] **Live Azure response shapes unverified**: the `Phase.Job` identifier format, non-null `warningCount`/`errorCount` on pending records, and test-run `$top` caps of at least 100 were not checked against live API output. If they differ, the code fails closed.
- [ ] **TLS path untested**: the socket-pair test uses plain `HTTPConnection`, so the TLS path is correct by construction only.
- [ ] **Human approval**: still outstanding and outside this engineering review.