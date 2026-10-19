---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 37
stage_attempt: 1
round: 3
timestamp: "2026-10-18T00:13:16Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "99128b68e46e9ee653331c73d64347ce2b52f9670a5dac9ac4fac41507227d8b"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-37-stage-attempt-1-review-3-claude-opus-5.5.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read the watcher in `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`:
  - imports and bytecode guard (13–27), `parse_build_id` (47–75), `query_pr` (78–95)
  - deadline and poll loop in `monitor` (98–257), including conversion of `EvidenceError` to `MonitorError` (136–152), agent-loss de-duplication, `azure_completed`, `replaced` and `attention_required` paths
  - `parse_kickoff` (260–269), `parse_args` bounds (272–314), and the `main` exit-code map
- [x] Read the collector in `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`:
  - `command_json` uses the trusted resolver and redacts stderr (39–56)
  - `AzureClient`: refuses redirects, enforces the deadline, sets timeline 404 to pending, and checks for repeated IDs during `$skip`/`$top` pagination (64–164)
  - `verify_merge`/`verify_build` (167–219), `summarize_timeline` (222–394; checks attempt selection, parent cycles and issue counts), `collect_build` (397–447)
- [x] Read the verifier in `.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py`:
  - `trusted_executable` (40–65), `git()` (68–88; clears `GIT_*` environment variables, uses `--no-replace-objects`, disables fsmonitor)
  - `verify()` (91–150) runs only `rev-parse`, `ls-tree`, `ls-files` and `cat-file blob`, so no filters or hooks run. It also rejects symlinks and tolerates CRLF.
  - `main` handles failures fail-closed.
- [x] Read `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`:
  - `ValidateRange`/`ValidatePattern` parameters (46–96)
  - guards that refuse `-RunPipeline`, `-RequestReview` and expected-SHA combinations (100–108)
  - `Invoke-PagedQuery` cursor-advance guard (182–239), `Get-RestEvidence` paged and empty handling (241–276)
  - bounded `-WaitForReview` loop (699–738)
- [x] Cross-checked documented behavior so documented failures were not reported as defects:
  - `references/ci-triage.md` 6–60 (kickoff source, `azure_completed` meaning "missing or lagging", nonzero query errors)
  - `references/trusted-guidance.md` 1–70 (working-directory exclusion at 39–44; host Python separate from PR environments)
  - `SKILL.md` 226–300 (explicit queueing against `refs/pull/<n>/merge` at 242–248)
- [x] Checked tests and CI interpreter:
  - `tools/ci/tests/test_watch_azure_pipeline.py` 515–536 uses only whole-second kickoff values.
  - `tools/ci/tests/test_trusted_guidance.py` 225–305 covers the working directory as the checkout and decoys, but not a working directory above a tool directory.
  - `pipeline.yaml` 112–126: the `CIHelpers` job runs the image's default `python3` with no `UsePythonVersion`, while `environment.yml` pins Python 3.11.8.
- [x] I did not execute any commands or tests. The pass counts, skips and Black results cited above are the caller's claims and were not reproduced.
- [x] Independence: I did not open, search or list `reviews/`, session state, or other reviewers' artifacts.

## Issues

### Issue 1: Trusted tool lookup fails with a misleading error when run from a filesystem root or a parent of tool directories
- **Severity**: Low
- **File**: `.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py`, also reached through `collect_ci_evidence.py` `command_json` and `watch_azure_pipeline.py` `query_pr`
- **Line(s)**: 42, 51–65 (exclusions at 56 and 62, error at 65); `collect_ci_evidence.py` 39–53; `references/trusted-guidance.md` 39–44
- **Description**: `trusted_executable` skips every absolute PATH entry that is the working directory or below it. It also rejects any resolved launcher below it. This is meant to keep checkout-owned decoys out, but the rule applies to any working directory, not just a checkout.
  - **Working directory is a filesystem root** (`/`, common in containers, or `C:\`): every PATH entry is below it, so `git`, `gh` and `az` can never be resolved.
  - **Working directory is the home directory** (the default for new terminals): user-scoped installs are skipped. This includes `~/.local/bin` (pipx/pip `az`), Scoop shims, WinGet `Links` and conda `bin`. Symlinked launchers that resolve under home are also rejected.

  All three helpers report the same text, "Cannot resolve git from trusted absolute PATH entries.":
  - the verifier as its JSON `error`
  - the collector as `EvidenceError`
  - the watcher as `outcome: error`

  That text suggests the tool is missing from PATH, not that the working directory disqualified it. The docs describe the exclusion in general terms but do not warn against these locations, and no test covers a working directory above a tool directory.
- **Risk**: The failure is fail-closed, so nothing can pass incorrectly. However, bootstrap, evidence collection and the watcher all stop before doing any work, with a misleading diagnosis. That invites unsafe workarounds such as adding relative PATH entries, copying tools, or writing a temporary replacement script, which `SKILL.md` forbids.
- **Suggested Fix**: Keep the security rule and make the failure diagnosable.
  - Reword the error to name the exclusion without printing the workstation path, for example: "Cannot resolve {name} from absolute PATH entries outside the current working directory; run from the checkout or a directory that does not contain host tool installations."
  - Add one sentence to `trusted-guidance.md` warning against running helpers from a filesystem root or from a directory that contains user-scoped tool installs.
  - Add one test where the working directory is the parent of the only PATH entry and assert the new message.

### Issue 2: The watcher rejects Azure `queueTime` kickoff values with fractional seconds on Python 3.10 and earlier
- **Severity**: Low
- **File**: `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`, with `references/ci-triage.md` and `tools/ci/tests/test_watch_azure_pipeline.py`
- **Line(s)**:
  - `watch_azure_pipeline.py` 260–269 and 278
  - `ci-triage.md` 17–25
  - `collect_ci_evidence.py` 209 and 427
  - `SKILL.md` 242–248
  - `test_watch_azure_pipeline.py` 515–536
- **Description**: The documented input path breaks on older Python:
  - **Docs:** `ci-triage.md` says to pass Azure's `queueTime` to `--kickoff-at` for manually queued runs. `SKILL.md` directs explicit queueing when the PR trigger filter is wrong.
  - **Collector output:** the new collector emits the raw REST value (line 427). `verify_build` only checks that it is a non-empty string (line 209).
  - **Azure format:** the REST API returns timestamps with up to 7 fractional digits and trims trailing zeros, for example `…:19.0779547Z` or `…:19.42Z`.
  - **Parser:** `parse_kickoff` calls `datetime.fromisoformat`. Before Python 3.11, that function accepts only 0, 3 or 6 fractional digits. These values therefore fail with "Kickoff must be an ISO 8601 timestamp within the supported UTC date range." (exit 2). The parser's own `.replace("Z", "+00:00")` exists only for pre-3.11 compatibility, so older interpreters are clearly expected.
  - **Interpreter:** `trusted-guidance.md` requires a host Python separate from the project environment pinned in `environment.yml`. On Ubuntu 22.04 and WSL that host `python3` is 3.10, the same interpreter the `CIHelpers` job uses.
  - **Tests:** kickoff tests use only whole-second values, so CI cannot catch this.

  The parser may predate this PR, but this PR's collector output and the documented manual-queue path now feed it.
- **Risk**: On common host interpreters, the watcher cannot start with the documented input, and the error wrongly says the timestamp is not ISO 8601. An operator may then type a later time, such as when the build was noticed. `ci-triage.md` forbids that because it silently extends the 120-minute window. The operator may also skip the watcher entirely.
- **Suggested Fix**:
  - In `parse_kickoff`, drop the fractional seconds before parsing, for example `value = re.sub(r"(\d\d:\d\d:\d\d)\.\d+", r"\1", value)`. Truncating moves the kickoff earlier by less than a second, so it can only shorten the window, never extend it.
  - Add parse tests for 7-digit and 2-digit fractions, such as `2026-09-21T01:50:00.1234567Z` and `2026-09-21T01:50:00.42Z`. CI's Python 3.10 would then exercise them.

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

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 4258 added and 163 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 37,
  "completedPriorPasses": 36,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}

## Driver resolution - superseded attempt

The original feedback above is unchanged. Original artifact SHA-256 values:

- Round 1: `49ff8cb1ad9b06f58e36d0821201754057fa689e3464de5f6fd89e3d94ea32f0`
- Round 2: `47d5723ef509776c00f7cc3f0b7f09fddd14509d70508117d5634a9bfa811a32`
- Round 3: `decd379460cede2dcaf8b2601d269b29026e410f11aa2d1d74a94b7f8fe009f6`

The reviewed diff SHA-256 was
`fbe5918eed4d8a9270c631ebdd694674b27c26dfd5c6ea7892bbfc7f80513421`;
the attempt-37 manifest records its complete source scope.
The target and merge base were `0543bf7e5266c587207ae8c28c3b696deeed3ed1`;
the published predecessor was `a96e0370dab63a22a73f56c24ec7441cd36d3622`.
All three transports used the observed `claude-opus-5.5` model and read-only
source, policy, and test tools, without prior or sibling feedback. Prepared
inputs contain the complete diff. Both recorded message forms preserve all
5,005 diff lines, except for one platform-masked header expression.

**Issue 1: Fixed.** Trusted tool lookup still excludes the current directory
and its descendants, but its error now explains that exclusion and how to
choose a usable working directory or PATH. The bootstrap reference warns about
filesystem roots and directories containing host tools. A real installed Git
fixture verifies the parent-directory failure and that the error omits its path.
The new diagnostic assertion failed before this change.

**Issue 2: Fixed.** The watcher normalizes Azure fractional seconds to six
digits before parsing, padding or truncating without rounding up. It preserves
microseconds and timezone conversion rather than discarding the entire fraction.
The new public `parse_args` regression covers seven-digit and two-digit UTC
fractions plus a timezone offset. All three cases failed before the fix on real
Python 3.10.20 with argparse exit 2; they pass afterward. This host's default WSL
Python is 3.12.3, not 3.10, so the compatibility check used an isolated 3.10.20
installation rather than inferring coverage from the default interpreter.

Validation after both fixes:

- Windows Python 3.14.6:
  `python -B -m pytest -q tools/ci/tests/test_trusted_guidance.py tools/ci/tests/test_watch_azure_pipeline.py tools/ci/tests/test_collect_ci_evidence.py -rs --tb=short`
  passed: 89 tests, 183 subtests; one POSIX-only launcher test skipped.
- Linux Python 3.12.3, from `tools/ci/tests`:
  `python3 -B -m pytest -q test_trusted_guidance.py test_watch_azure_pipeline.py test_collect_ci_evidence.py -rs --tb=short`
  passed: 90 tests and 183 subtests, without skips.
- Isolated Linux Python 3.10.20, from `tools/ci/tests`:
  `python3.10 -B -m unittest -q test_trusted_guidance test_watch_azure_pipeline test_collect_ci_evidence`
  passed all 90 tests. The resolved isolated interpreter was invoked directly;
  no global interpreter, dependency pin, or CI definition changed.
- `python3 -m black --check --required-version 22.3.0 .github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py .github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py .github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py tools/ci/tests/test_collect_ci_evidence.py tools/ci/tests/test_get_pr_readiness.py tools/ci/tests/test_trusted_guidance.py tools/ci/tests/test_watch_azure_pipeline.py`
  passed for all seven changed Python files.

The readiness helper and its tests did not change after their previous full
Windows/Linux runs. Rounds 1 and 2 were clean, but these fixes end the pass; none
of its rounds qualify the revised source. Current-head Azure validation and the
final CI-qualified pass remain outstanding.
