---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 70
stage_attempt: 1
round: 4
timestamp: "2026-10-19T20:20:04Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "3df62ba71c6d69a9ede284896eee7bec16a8d6c38d9330af38819f4705baa8f8"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-70-stage-attempt-1-review-4-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Deadline-bounded HTTPS reads** (`.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py` lines 119–167). I traced `DeadlineHTTPSHandler.https_open` against the standard-library request path.
  - The `connection(host, timeout=, context=)` and `response(sock, method=)` factories match how the library calls them at the default debug level of 0.
  - The original buffered socket stream is detached before any byte is read, so no buffered data is lost.
  - Status line, headers, chunk framing and body reads (including read-to-end) all go through `DeadlineReader.readinto`. That method sets `settimeout(remaining_timeout())` before every receive.
  - Closing the response closes the socket stream, which performs the socket close that urllib deferred earlier. The detached original wrapper skips close when garbage-collected, so nothing is closed twice.
- [x] **Error classification** (`AzureClient.remaining_timeout` and `get`, lines 180–242).
  - `remaining_timeout` raises when `remaining <= 0`. That error, raised from `opener.open(timeout=...)` or from a read, is not caught by the `(URLError, OSError, ValueError, HTTPException)` clause, so it surfaces as the deadline error.
  - `HTTPError` is handled before `URLError`, and a 404 on the timeline endpoint returns `None`.
  - A truncated response (`IncompleteRead`) and invalid JSON both recheck the deadline first. A `TimeoutError`, direct or wrapped in `URLError`, then reports "timed out"; anything else gets the generic sanitized message.
  - The header is `****** confirmed by grep because the file viewer masks it. `tools/ci/tests/test_collect_ci_evidence.py` line 1214 asserts the fixture value.
- [x] **Collector evidence logic** (lines 244–823).
  - `list()` pages with `$skip=len(values)` and `$top=100`, stops on a short page, and rejects duplicate IDs.
  - `summarize_timeline` keeps the latest attempt per job, skips Stage/Phase containers that have child jobs, and detects parent cycles.
  - `collect_build` (line 502) works out internally whether an empty timeline is allowed, so the watcher's two-argument call at `watch_azure_pipeline.py` line 157 is correct.
  - `collect_tests` keys test runs by `phaseName.jobName`. That matches the timeline identifiers because `pipeline.yaml` defines top-level `jobs:` at line 111 and has no stages.
  - `collect_tests` rejects test attempts newer than the timeline and requires each job's current attempt to publish its results. `--allow-skipped` can never exempt an `--expected-test`.
  - The final `remaining_timeout()` check at line 810 turns finishing at or after the deadline into an error, as `references/ci-evidence.md` states.
- [x] **Watcher deadline boundaries** (`watch_azure_pipeline.py` lines 97–386).
  - `remaining <= 0` and `monotonic() >= deadline` express the same cut-off the collector uses.
  - `wait_for_check_update` requires more than 600 s to remain, so the extra confirmation poll starts strictly before the deadline, as `references/ci-triage.md` lines 50–64 say.
  - `timeout_result["azure"]` is updated only from polls that finish before the deadline.
  - Exit codes match the documentation.
  - Kickoff timestamps are truncated to microseconds with `[:6]`, never rounded up. The build-ID length and range checks have no off-by-one.
  - The boundary rows in `tools/ci/tests/test_watch_azure_pipeline.py` lines 594–691 match these comparisons: elapsed 0 vs 7200, and 7199 vs 7200/7201.
- [x] **Launch flags vs imports.** The watcher imports `collect_ci_evidence`, and the collector imports `verify_trusted_guidance`, from their own directory.
  - The documented `-E -s -B` commands (`ci-triage.md` line 31, `ci-evidence.md` line 144) keep the script directory importable while ignoring `PYTHONPATH`.
  - `-I -B` is used only for the verifier, which imports nothing but the standard library (`trusted-guidance.md` line 18).
  - `tools/ci/tests/test_trusted_guidance.py` parses exactly these documented lines.
- [x] **Guidance verifier** (`scripts/verify_trusted_guidance.py`).
  - The checkout path is fully resolved first, so the symlink scan cannot misfire on directories above the checkout root.
  - `ls-files --others` without `--exclude-standard` also reports ignored files.
  - CRLF differences are tolerated only for text files (blobs without NUL bytes).
  - `--no-replace-objects`, `core.fsmonitor=false` and removing `GIT_*` environment variables block replace-ref, fsmonitor-hook and environment redirection.
  - PATH entries that are relative, equal to the current directory, or inside it are skipped.
  - Every rejection asserted in the tests uses a message the code actually produces.
- [x] **Readiness script** (`scripts/Get-PrReadiness.ps1`).
  - Parameter guards are at lines 101–110, and paged GraphQL queries stop if the cursor fails to advance (lines 228–236).
  - Empty, `null` or invalid REST pages add a collection error, so partial output fails closed (lines 240–274).
  - The test harness (`tools/ci/tests/test_get_pr_readiness.py` lines 1–310) enforces the exact GitHub calls: `--jq tojson` paging, `&state=all&`, compare using immutable SHAs, and a single `/azp run` comment body.
- [x] **Documentation consistency.**
  - The step 7 scope exception in SKILL.md (any `tools/` change disqualifies) matches the loop-control acceptance rows and the external-contributor references. This PR changes `tools/ci/tests/`, so it is correctly not exempt.
  - All referenced anchors resolve to real headings, including `#comment-trigger-gate` (`ci-evidence.md` line 94), `#azure-merge-and-named-tests` (line 130) and `#targeted-infrastructure-retry` (`ci-triage.md` line 200).
  - `ci-evidence.md` lines 205–216 describe the same issue-blocking rules that `collect_build` applies.
- [x] **Test inventory matches the reported counts.** There are 45 test methods in the collector tests and 42 in the watcher tests, 87 in total. Exactly two are Windows-only (`skipUnless(sys.platform == "win32")` at lines 906 and 934). That fits the reported 87 on Windows and 85 plus 2 skipped on Linux; I checked structure only.
- [x] **Considered and not reported:**
  - Connect, TLS handshake and send are each limited by the timeout captured when the request opens (at most 60 s), not re-clipped per operation. Any overrun still fails the final deadline check, and the docs only claim per-read limits.
  - `collectionStable` compares job lists in order, so reordered records would fail closed rather than pass.
  - Null issue counts fail closed.
- [ ] **Not executed:** I ran no tests, Black, PowerShell, git or helpers, because this round is read-only. All run results are the caller's reported evidence.
- [ ] **Not hash-verified:** the diff in the prompt was cut off after the SKILL.md gauntlet hunk. I reviewed the remaining files from the working tree and could not compare them with the frozen index tree or the source-diff SHA-256.
- [ ] **Partial reading and no live API calls:** I did not read every test body in `test_get_pr_readiness.py` and `test_watch_azure_pipeline.py` line by line. The Azure API shapes I relied on (job names, `$top` caps, record order) come from documentation, not live responses.