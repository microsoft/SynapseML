# Review Round 4 — Detailed Correctness

## Summary
- Round: 4
- Theme: Detailed Correctness
- Mode: manual prompt fallback
- Requested model: claude-opus-5.5 (max)
- Issues: 0
- Verdict: CLEAN

## Evidence Checklist
- [x] `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py:379-390,421`: If a test run is missing its `jobReference` or points to an unknown job, the collector marks it as not from the current attempt (`currentAttempt: false`) and records the error "lacks matching job-attempt provenance". Results from an older retry attempt can't satisfy the test gate.
- [x] `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py:516-529`: `validationPassed` is true only when the build has completed and succeeded, the job gates and tests pass, and the `collectionStable` recheck holds. `mergeReady` is always `False`. The exit code follows `validationPassed`.
- [x] `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py:101`: The `f"******"` in the Authorization header is only the tool masking the value. A grep for `Bearer` matches this line, so the source builds the header from the token as intended. This isn't a bug.
- [x] `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py:206-218,231-233,248-256`: While the Azure build is still running, the watcher keeps waiting even if the GitHub check shows an old failure. If the GitHub check is missing or behind after Azure finishes, it returns `azure_completed` (exit code 4, `:374-375`), not success.
- [x] `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py:258-264`: When the GitHub conclusion disagrees with the Azure result, the watcher returns `attention_required` instead of a success or failure verdict.
- [x] `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py:46-74`: `parse_build_id` either returns an integer within the int32 range or raises `MonitorError`. So `max(matching)` at `:206`/`:221` never compares against `None`, and duplicate build IDs are rejected at `:202-203`.
- [x] `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1:97-101,496-505`: `-RunPipeline` only posts `/azp run` when all of these hold:
  - exactly one PR is given;
  - both expected head and base SHAs are provided;
  - `-WaitForReview` is not set;
  - the evidence is complete;
  - the PR is `OPEN`;
  - the exact `microsoft.SynapseML` check is absent.

  This rules out repeated pipeline queuing on each poll.
- [x] `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1:434-446`: The script re-reads the PR head, base, `baseRefName` and state, and compares the head and base against the expected SHAs. Any drift between collection and decision is recorded as an error, so the evidence isn't treated as complete.
- [x] `.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py:35-38,60-69,75,84,89-98`: The guidance check:
  - removes `GIT_*` environment variables and runs git with `--no-replace-objects`;
  - accepts only regular files in the commit tree, with no symlinks or submodules;
  - requires the index to match the tree exactly, with no merge-conflict entries;
  - rejects untracked files, including gitignored ones, because `--others` is used without an exclude;
  - rejects paths that go through a symlinked parent directory;
  - tolerates CRLF line endings only for text files (no NUL bytes).
- [x] `.github/skills/synapseml-external-contributor-review/references/contributor-safety.md`: This required guidance file exists in the tree, so the list of required paths resolves.
- [x] `pipeline.yaml:125`: The CIHelpers job runs `python3 -m pytest tools/ci/tests/ -q` on ubuntu-22.04 with Python 3.10, so the new tests run in CI. The scripts under review don't use any Python 3.11+ features.

## Publication record

- PR: microsoft/SynapseML#2775
- Source publication commit: `66d08e29cc9baf9d74a71f69ce7f59c6b9f1d299`
- Reviewed input manifest SHA-256: `e30c896294c0217401bfb9b79d1186d36e8373488d8a5a4db99c57a589109609`
- Reviewed diff SHA-256: `152561506548ba7824135deea64a9854b5c4e04fc6f2756040b0647a48db6c13`
- Original artifact SHA-256: `054af33e001d73bd0c39b7773d8fb3e8e7195c79c071d6c9dcd3247908773574`
- Dispatch mode: manual prompt fallback; no observed CLI receipt is claimed.
