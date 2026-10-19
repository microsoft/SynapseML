---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 51
stage_attempt: 1
round: 1
timestamp: "2026-10-18T16:18:38Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "64f94e5453394c5579f39db93fa270726e8871c436fe58a23680a57c769ff79a"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-51-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Diff scope.** I reviewed the supplied merge-base-to-index diff across the external-contributor `SKILL.md` and `references/ci-delegation.md`, plus the PR loop's:
  - `SKILL.md`
  - `references/ci-evidence.md`, `ci-triage.md`, `loop-control.md`, `readiness-gates.md`, `review-gauntlet.md` and `trusted-guidance.md`
  - `scripts/Get-PrReadiness.ps1`, `collect_ci_evidence.py`, `watch_azure_pipeline.py` and `verify_trusted_guidance.py`
  - the four `tools/ci/tests/test_*.py` helper suites

  All searches stayed within `.github/skills/`, `tools/ci/` and the directly named `pipeline.yaml`.
- [x] **`.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`.**
  - Lines 87–111: the expected SHAs must be 40-hex. Expected revisions apply to one PR only. `-RunPipeline` requires both SHAs and refuses `-WaitForReview` and `-RequestReview`.
  - Lines 541–569: the revision checks, including the expected head/base, feed `evidenceComplete`.
  - Lines 610–648: the trigger fails unless evidence is complete, then requires an OPEN PR. It posts only when the exact `microsoft.SynapseML` aggregate is absent (case-sensitive `-cnotin`). It posts once, and a malformed receipt never turns a successful POST into a retry.
  - This matches the comment-trigger gate in `references/ci-evidence.md` lines 94–124. The conservative complete-collection gate is kept.
- [x] **`.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`.**
  - `AzureClient.get` (lines 126–173) has an endpoint allowlist, refuses redirects and keeps the token in memory only. Endpoints with non-numeric IDs are rejected.
  - `verify_merge` and `verify_build` (lines 203–256) require ordered [base, head] parents, definition `17563`, the expected `refs/pull/N/merge` and the expected source version.
  - `collect_tests` (lines 487–672):
    - unknown job-attempt provenance blocks requested runs;
    - an attempt newer than the timeline raises an error;
    - required job/suite and job/test publications must come from the current attempt;
    - allowed skips cannot exempt `--expected-test`.
  - `main` (lines 720–749) sets `validationPassed` only when the build completed and succeeded, the job gates pass, the tests pass and collection is stable. I found no false-pass path.
- [x] **`.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`.**
  - URLs are trusted only for an HTTPS host/project allowlist with no credentials, port 443 and a valid int32 build ID (lines 47–75).
  - The deadline is anchored to kickoff and capped at 120 minutes (lines 98–118 and 315–318).
  - Timeline mode verifies the merge once and reports each agent loss once per job attempt (lines 134–175).
  - `azure_completed` and `attention_required` exit with code 4 (lines 190–251 and 355–367).
  - This matches `references/ci-triage.md` lines 36–56.
- [x] **`.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py`.**
  - `GIT_*` variables are stripped, with `--no-replace-objects` and `core.fsmonitor=false` (lines 72–92).
  - Tree, index, untracked/ignored-file, symlink and working-byte checks with CRLF tolerance are at lines 95–156.
  - `trusted_executable` excludes the cwd and its descendants and rejects relative PATH entries (lines 40–69).
  - All REQUIRED paths exist, including `CODEOWNERS` and `contributor-safety.md`, confirmed by glob.
- [x] **Comment-trigger routes.** Every route links `ci-evidence.md#comment-trigger-gate`:
  - PR loop `SKILL.md` lines 205–227 and 266–277;
  - `references/ci-triage.md` lines 16–17;
  - `references/readiness-gates.md` lines 96–102;
  - external `references/ci-delegation.md` lines 69–73.

  These include authorized requeues. I found no route that triggers without the gate.
- [x] **Anchors.** All 19 intra-skill `.md#anchor` links resolve to existing headings, including:
  - `#user-value-and-scope`, `#artifact-retention`, `#evidence-invalidation`, `#validation-only-handoff`
  - `#waiting-for-azure-pipelines`, `#azure-merge-and-named-tests`, `#targeted-infrastructure-retry`, `#evidence-required-before-triggering`

  The no-bloat contract is defined once, in `readiness-gates.md` lines 20–29. `SKILL.md` line 150 links to it.
- [x] **Gauntlet move.** `references/review-gauntlet.md` keeps every requirement removed from step 8 of `SKILL.md`: the explicit diff and manifest, reviewer independence and collision checks, six clean rounds on one frozen patch, and pre-PR draft handoff. Its runner and retention additions agree with `loop-control.md` lines 53–69 and its acceptance table.
- [x] **CI wiring.** In `pipeline.yaml` lines 112–126, the CIHelpers job runs `python3 -m pytest tools/ci/tests/ -q` with only pytest and pyyaml installed. The four helper suites import only the standard library (checked by grep). The diff changes no pipeline or workflow definitions.
- [x] **Tests.**
  - `test_get_pr_readiness.py` lines 249–251 run when `pwsh` is present or `TF_BUILD` is set, so a missing `pwsh` fails in CI and skips locally, as `ci-triage.md` documents. Lines 829–935 cover unsafe-trigger refusals and receipt handling.
  - `test_trusted_guidance.py` lines 118–211 read the interpreter flags from the docs and run the helpers only with `--help`, with no network calls.
  - The `test_collect_ci_evidence.py` test names cover retries, provenance, publication, issue and pagination guards.
- [x] **Edge cases judged fail-closed and not reported:**
  - a null timeline `warningCount`;
  - malformed `git` output raising an uncaught `ValueError` (nonzero exit);
  - a lost PID message if `kill()`/`wait()` also times out after `taskkill` fails;
  - a server-side `$top` cap below 100, which is speculative;
  - clock skew against the "kickoff cannot be in the future" check.
- [ ] **Not executed by this reviewer.** No tests, CI or helpers were run; this was a read-only review. The supplied local results are the author's evidence. Two things remain unverified until a current-candidate Azure run exists:
  - the hosted runtime of the CIHelpers job against its 15-minute timeout (the author's local Linux run took about 494 s for the four suites);
  - current-candidate Azure CI itself.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 4986 added and 189 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 51,
  "completedPriorPasses": 50,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}

## Subsequent disposition

Round 5 identified two public-path coverage gaps. Extending the existing tests
invalidated this pass. This original clean round is retained as historical
evidence, not counted toward the changed patch's review.
