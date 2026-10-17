# Review Round 1 — Broad Sweep

## Summary
Round: 1
Theme: Broad Sweep
Mode: manual prompt fallback
Requested model: claude-opus-5.5 (max)
Issues: 0
Verdict: CLEAN

I reviewed all 17 files in the manifest and the full `review.diff`, and checked them against the frozen worktree sources under `.github/skills` and `tools/ci/tests`. I also read `pipeline.yaml` to confirm the timeline identifier format. I found no change that is wrong or unsafe, and none that loosens a readiness gate.

## Evidence Checklist
- [x] `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1:97-102`: `-RunPipeline` requires exactly one PR and both the expected head and base SHAs, and it refuses `-WaitForReview`. Lines `:434-447` re-read the PR and the live base-branch tip right before triggering, then compare both to the expected SHAs. `prBaseRefSha` is only informational. Lines `:496-505` post the trigger only when evidence is complete, the PR is `OPEN`, and the exact `microsoft.SynapseML` aggregate check is absent.
- [x] `Get-PrReadiness.ps1:306-352`: rulesets are fully paginated (`--paginate --jq tojson`) and the classic `/protection` endpoint is also checked. Workflow runs are paginated and compared against `total_count`, and the newest run per workflow/event wins. `action_required` and waiting approvals are reported. Line `:573` sets `complete` from `remoteGatesClear` and keeps the legacy alias. Lines `:606-611` show the Azure hint only when the aggregate check is missing.
- [x] `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py:20,52-65`: `API_ROOT` is fixed to the project GUID, and the `NoRedirect` opener makes redirects raise, so credentials stay on the Azure DevOps host. Line `:142` (`verify_merge`) requires the parents in order `[base, head]`. Line `:157` (`verify_build`) checks the repository, build id, `refs/pull/N/merge`, that `sourceVersion` is the merge SHA, and that the reason is `pullRequest|manual`.
- [x] `collect_ci_evidence.py:183` (`summarize_timeline`):
  - Records are keyed by `(type, identifier)` and only the latest attempt is kept.
  - Only leaf containers are counted, and parent cycles are rejected.
  - `pipeline.yaml` has jobs only, so the timeline's `Phase.Job` identifiers match the collector's `{phaseName}.{jobName}` key.
- [x] `collect_ci_evidence.py:340` and `:508`:
  - `:340` (`collect_tests`) takes attempts only from `Job` records.
  - A missing `jobReference` is marked not-current and reported as an error; there is no assumed "attempt 1".
  - An allowed skip cannot excuse a missing expected test.
  - `:508` (`main`) collects the build a second time to confirm it is stable, and leaves `mergeReady` false.
- [x] `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py:150-191`: the merge is verified once, the build is re-collected on every 600-second poll, and agent-loss events are de-duplicated by `(identifier, attempt)`. In `:206-217`, `:231-233`, `:249-256` and `:261-267`:
  - If the GitHub aggregate check is missing or pending while Azure has completed, the result is `azure_completed`.
  - A running Azure build overrides a stale GitHub result.
  - A mismatch between them gives `attention_required`.
  - Both `azure_completed` and `attention_required` exit with code 4.
- [x] `.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py:38` runs git with `--no-replace-objects` and with `GIT_*` variables removed from the environment. `verify` (`:48` onward) checks:
  - The repository toplevel equals the checkout, and `HEAD` equals the expected SHA.
  - Only regular blobs are accepted.
  - The index matches the tree (`ls-files -sz`, `:75`).
  - Untracked and ignored files are rejected (`ls-files --others`, `:84`).
  - No ancestor directory is a symlink.
  - File contents are read with `cat-file`, which skips git filters.
- [x] `.github/skills/synapseml-pr-loop/references/review-gauntlet.md` matches the text removed from `SKILL.md` line for line. The only difference is the link `references/loop-control.md` → `loop-control.md`, which is correct for the file's new location.

## Publication record

- PR: microsoft/SynapseML#2775
- Source publication commit: `66d08e29cc9baf9d74a71f69ce7f59c6b9f1d299`
- Reviewed input manifest SHA-256: `e30c896294c0217401bfb9b79d1186d36e8373488d8a5a4db99c57a589109609`
- Reviewed diff SHA-256: `152561506548ba7824135deea64a9854b5c4e04fc6f2756040b0647a48db6c13`
- Original artifact SHA-256: `9cd643cc2fd7a331c1c17db37e7e82f3efdcc307a9d46c5998053b9be0f4d95d`
- Dispatch mode: manual prompt fallback; no observed CLI receipt is claimed.
