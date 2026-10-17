# Review Round 3 — Edge Cases & Robustness

## Summary
- Round: 3
- Theme: Edge Cases & Robustness
- Mode: manual prompt fallback
- Requested model: claude-opus-5.5 (max)
- Issues: 0
- Verdict: CLEAN

No significant issues found in the reviewed changes.

## Evidence Checklist
- [x] `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py:516-529` — After the first collection, the script reads the build a second time and compares status, result and jobs to set `collectionStable`. `validationPassed` requires a completed and succeeded build, cleared job gates, passing tests and a stable collection. `mergeReady` is always `False`. A retry that starts during collection therefore can't produce a green result; `tools/ci/tests/test_collect_ci_evidence.py:373` covers this.
- [x] `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py:331,392,485` — Any job that didn't succeed blocks validation, including jobs that only `succeededWithIssues`. A skipped job is allowed only through an exact `--allow-skipped-job` match. A `totalTests` count that differs from the number of returned results is an error. The tests also cover these cases:
  - repeated pages fail (`test_collect_ci_evidence.py:298`)
  - an unknown or missing job reference never supplies current results (`:244`)
  - an earlier attempt can't fill in a test missing from the current attempt (`:154`)
- [x] `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py:101` — The Authorization header is built in-process, and the `******` in the displayed source is only display redaction. HTTP errors are reduced to their status code, and transport or protocol errors are sanitized. CLI stderr is never printed; `test_collect_ci_evidence.py:310,329` covers both behaviours.
- [x] `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py:114-263,372-375` — `monitor()` handles the edge cases correctly:
  - It stops with `superseded` when the head or PR state changes (`:148`).
  - It reports each agent loss once per (identifier, attempt) (`:176-182`).
  - A missing GitHub check while the Azure build has completed gives `azure_completed`, never a pass (`:210`, `:251`).
  - A conclusion mismatch gives `attention_required` (`:263`).
  - Both of those outcomes exit with code 4.
  - `test_watch_azure_pipeline.py:577` covers the missing-check case. `:643` covers a same-build retry that is still running while an old GitHub failure exists: the watcher keeps polling instead of stopping.
- [x] `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1:96-101,448-450,496-504` — The parameter guards hold:
  - Expected SHAs are allowed only for exactly one PR.
  - `-RunPipeline` requires both the head and base SHAs and can't be combined with `-WaitForReview`.
  - The CI trigger throws if evidence is incomplete or stale, or if the PR isn't open.
  - It triggers only when `microsoft.SynapseML` isn't already present.
  - `complete` equals `remoteGatesClear`, which depends on `evidenceComplete`, so a failed fresh re-read blocks readiness.
- [x] `.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py:35-38,67,78-84` — The verifier blocks the ways untrusted content could slip into the trusted guidance:
  - It strips inherited `GIT_*` environment variables and runs git with `--no-replace-objects`.
  - It accepts only regular blob modes (`100644`/`100755`).
  - It rejects index entries that aren't at stage 0.
  - It rejects untracked or ignored files under the guidance paths, so they can't add to the guidance (`tools/ci/tests/test_trusted_guidance.py:63`).
- [x] `pipeline.yaml:111` — The pipeline uses top-level `jobs:` (one implicit stage), so the Job identifiers in the timeline match the job references on the test runs. This mapping is how current test attempts are tied to jobs in `collect_ci_evidence.py`.

## Publication record

- PR: microsoft/SynapseML#2775
- Source publication commit: `66d08e29cc9baf9d74a71f69ce7f59c6b9f1d299`
- Reviewed input manifest SHA-256: `e30c896294c0217401bfb9b79d1186d36e8373488d8a5a4db99c57a589109609`
- Reviewed diff SHA-256: `152561506548ba7824135deea64a9854b5c4e04fc6f2756040b0647a48db6c13`
- Original artifact SHA-256: `33c4539419b95d7e9eba38ff26b1a4aea9a735d955ddb34ec75f45ef3e3c8420`
- Dispatch mode: manual prompt fallback; no observed CLI receipt is claimed.
