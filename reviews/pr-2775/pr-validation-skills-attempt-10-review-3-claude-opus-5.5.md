# Review Round 3 — Edge Cases & Robustness

## Summary
- Round: 3
- Theme: Edge Cases & Robustness
- Mode: manual prompt fallback (not Execute, no observed CLI receipt)
- Requested model: claude-opus-5.5 (max)
- Issues: 0
- Verdict: CLEAN

I reviewed all 17 files in the patch against the worktree and found no high-confidence edge-case or robustness defect. On every failure path I traced, the scripts either fail closed or report an explicit "unknown" state. None of them reports a gate as clear when it isn't.

## Evidence Checklist
- [x] `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1:93-98`, `:496-511`: before `RunPipeline` triggers, it checks that:
  - there is exactly one PR and both SHAs are present;
  - `WaitForReview` is not set;
  - evidence is complete;
  - the PR is OPEN;
  - the aggregate check is absent.
- [x] `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1:431-447`: head and base are re-checked fresh. A branch that is behind base only blocks `remoteGatesClear`; it is not reported as a collection error.
- [x] `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py:20`, `:52-65`, `:79`, `:107-111`: the API root is fixed and the endpoint must pass `fullmatch`. Redirects are refused. HTTP, URL, OS, and `HTTPException` errors are sanitized.
- [x] `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py:214`: a parent cycle in the timeline raises `EvidenceError` instead of looping forever.
- [x] `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py:432`, `:495`, `:503`: an allowed skip cannot exempt an expected test. SHA inputs must be 40 hex characters. Blank suite and job names are rejected.
- [x] `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py:150-190`, `:204-272`: the watcher:
  - keeps waiting while Azure is still active, even if GitHub shows a stale failure;
  - exits with code 4 for `azure_completed` and `attention_required`;
  - measures its deadline from the original kickoff;
  - counts each agent loss once per (identifier, attempt) pair.
- [x] `.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py:60-69`, `:84`, `:89-93`, `:98`, `:122`:
  - only regular files are allowed in the tree, so symlinks and submodules are rejected;
  - untracked and ignored files are rejected (`ls-files --others` without exclude flags);
  - paths that go through a symlinked parent directory are rejected;
  - CRLF line endings are normalized, and nothing else is;
  - `main` handles `OSError`.

## Publication record

- PR: microsoft/SynapseML#2775
- Source publication commit: `66d08e29cc9baf9d74a71f69ce7f59c6b9f1d299`
- Reviewed input manifest SHA-256: `dd627e6928a8fca3712f03b636772fd1a86133df66dad20fa84456f479878d81`
- Reviewed diff SHA-256: `e1ccbd8f666192d9a59f46862c4cf55988be271ccf0a9a3b894c64091916a980`
- Original artifact SHA-256: `bfe006d9677673d6c72240842ad99c08251b5252d54001f80c0480e7f33b394b`
- Dispatch mode: manual prompt fallback; no observed CLI receipt is claimed.
