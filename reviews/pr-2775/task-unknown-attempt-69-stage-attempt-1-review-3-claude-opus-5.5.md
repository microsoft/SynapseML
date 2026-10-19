---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 69
stage_attempt: 1
round: 3
timestamp: "2026-10-19T17:30:30Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "5ff36a68e2a6b90302ddea5057cbf20495836408d72dda8d733b6c50de0f9059"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-69-stage-attempt-1-review-3-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **HTTPS deadline enforcement** (`.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`, lines 126–166 and 180–237):
  - `DeadlineReader.readinto` calls `settimeout(remaining_timeout())` before every raw socket receive. That covers the status line, headers, chunk-size lines and the body reads.
  - `remaining_timeout` raises when the budget reaches zero, so the socket never receives a timeout of 0, which would make it non-blocking.
  - `EvidenceError` subclasses `Exception` directly, so the `OSError`/`ValueError` handler in `get` does not catch it.
  - On expiry, the socket is closed by whichever cleanup runs: `response.close()` inside `getresponse`, `h.close()` in urllib's `do_open`, or the `with` block's exit. Each one closes the detached `SocketIO`, which releases the last reference, so the descriptor actually closes.
  - Each TLS read is limited by `_ssl`'s own deadline. On Python 3.10–3.14, a proxy `CONNECT` response goes through the keyword-only response factory and closes without closing the socket.
- [x] **Streaming regression test** (`tools/ci/tests/test_collect_ci_evidence.py`, lines 1087–1168):
  - It uses a real urllib opener, `HTTPConnection` and `HTTPResponse` over `socket.socketpair()`. Only three things are patched: `HTTPSConnection` (with `debuglevel=0`), `getproxies` and `time.monotonic`.
  - Patching `monotonic` does not affect `Event.wait`, `join` or `sleep`, because those use C-level clocks.
  - The expired row asserts that the body did not finish and that `fileno() == -1`. The `finally` block closes the reader and joins the sender with a 3-second limit. If the fix regresses, the test fails instead of hanging.
- [x] **Overruns fail closed**:
  - Connect and handshake time is limited only by the budget left when the connection opens, and DNS lookup is not limited at all. Even so, the next receive and the final `remaining_timeout()` check in `main` turn any overrun into an error, never a pass.
  - `list` (lines 239–263) rejects pages that are not lists, invalid IDs and duplicate IDs.
  - When a Windows `.cmd`/`.bat` command times out (lines 39–70), the cleanup kills the whole process tree. If it cannot confirm the tree is gone, it reports an error.
- [x] **Watcher** (`watch_azure_pipeline.py`, lines 100–102, 116, 153–162, 213–256 and 335):
  - The wall-clock kickoff budget is converted once into a monotonic deadline. Every `gh` call and every `AzureClient` built per poll uses that same deadline.
  - The extra poll runs only when more than 600 seconds remain, and sleeps never run past the deadline.
  - A kickoff time in the future is rejected, so restarting the watcher cannot extend the deadline.
- [x] **`-RunPipeline` guards** (`Get-PrReadiness.ps1`, lines 109–110, 301–303 and 610–648):
  - It requires both expected SHAs and refuses to combine with `-WaitForReview` or `-RequestReview`.
  - It throws when evidence is incomplete or stale, or when the PR is not open.
  - It posts only when the `microsoft.SynapseML` check is absent, matched case-sensitively.
  - It records whether the POST succeeded separately from whether the receipt could be parsed. Because strict mode is off inside that function, a malformed receipt cannot abort the script after a successful POST.
- [x] **Executable resolution and git calls** (`verify_trusted_guidance.py`, lines 40–69 and 72–93):
  - `trusted_executable` passes absolute candidate paths to `shutil.which`, so Windows never searches the current directory implicitly. PATH entries that are relative or inside the working directory are skipped.
  - `git()` removes `GIT_*` environment variables, disables fsmonitor and replace objects, and has a time limit.
- [x] **Azure scope exception is consistent across docs**:
  - Checked `SKILL.md` lines 205–217 and 279–283, `ci-evidence.md` lines 94–128, `readiness-gates.md` lines 100–103 and 125–126, the external-contributor `SKILL.md` lines 39–45, and `ci-delegation.md` lines 25–27. They agree on the exception's boundary and on how exempt diffs are handled.
  - The `CIHelpers` job in `pipeline.yaml` (lines 112–124) is the only hosted runner of `tools/ci/tests`. `ci-triage.md` lines 105–113 names the local substitute and says a local skip is not a pass.
  - Every new cross-document anchor resolves to an existing heading.
- [ ] **Tests not run**: this was a read-only review, so I ran no tests, linters or probes. I did not reproduce the supplied pytest, Black or TLS evidence.
- [ ] **Live API responses not checked**: the actual shapes of Azure and GitHub responses (null timeline counts, server-side `$top` limits, comment receipts) cannot be confirmed offline. For every deviation I could identify, the code fails closed.
- [ ] **Truncated diff**: the supplied diff was cut off partway through `SKILL.md`. I reviewed the rest by reading the named source, test and reference files directly. I did not open `reviews/` or any prior review artifacts.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and current task catalog",
    "stageBehavior": "pinned target PR-loop sequential six-lens all-clean policy",
    "bounds": "informed user resumption after the reported budget blocker; one additional fix/publication cycle, no reset of cumulative counters"
  },
  "changeSize": "17 source, test, and documentation paths; 5789 added and 207 removed lines",
  "cyclomaticComplexity": "Moderate coupled provenance, pagination, retry, readiness, deadline and filesystem interactions",
  "rationale": "Review the complete validation-tooling patch and pending transport/policy follow-up; no product algorithms or CI configuration changes",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/SKILL.md at d34f762a785ee8a7c7c624806f8c112256168833; user's explicit 2026-10-19 resumption",
  "reviewAttempt": 69,
  "completedPriorPrecommitPasses": 68,
  "completedFinalCiQualifiedPasses": 0,
  "roundOrder": [3, 5, 6, 1, 2, 4],
  "verifiedTaskId": null
}
