# Review Round 5 — Testing & Coverage

## Summary
- Round: 5
- Theme: Testing & Coverage
- Mode: manual prompt fallback (not Execute/observed CLI receipt)
- Requested model: claude-opus-5.5 (max)
- Issues: 0
- Verdict: CLEAN

## Evidence Checklist
- [x] `pipeline.yaml:112-125`: The `CIHelpers` job runs `python3 -m pytest tools/ci/tests/ -q` on ubuntu-22.04, which already has `pwsh`. The new PowerShell-backed readiness tests therefore run in CI and are not skipped.
- [x] `tools/ci/tests/test_get_pr_readiness.py:58` and `tools/ci/tests/test_get_pr_readiness.py:158-162`: The fake `gh pr comment` route writes the trigger file before it throws. `snapshot()` then checks that the file exists only when `allowTrigger` is set and no error is expected, so an unexpected CI trigger fails the test.
- [x] `tools/ci/tests/test_collect_ci_evidence.py:170`, `:239`, `:366`: Regression tests cover three cases:
  - a documented skip cannot waive a required named test
  - leaf containers are gated but cannot supply test attempts
  - protocol errors and truncated-body errors are sanitized
- [x] `tools/ci/tests/test_watch_azure_pipeline.py:538`, `:643`: Agent loss is reported before the job or build reaches a terminal state. A stale GitHub failure does not stop an active retry of the same build.
- [x] `tools/ci/tests/test_trusted_guidance.py:90`: Ignored bytecode cannot add to trusted guidance.
- [x] All 17 worktree files match the manifest blobs once CRLF line endings are normalized to LF, so the line numbers above match the reviewed patch.

## Publication record

- PR: microsoft/SynapseML#2775
- Source publication commit: `66d08e29cc9baf9d74a71f69ce7f59c6b9f1d299`
- Reviewed input manifest SHA-256: `1d7987ebe5de682ada6cfd4c0f66db85d028ebd8e1418f424f0f9b69d54d6636`
- Reviewed diff SHA-256: `df7ac7c309f23bf644e55765296cb4a92594b70392e178c7040cf8616bf65733`
- Original artifact SHA-256: `5a08e3f0944cb83e803779ab6f0f213a8c1c3bd68186248d4174760a7648fbf7`
- Dispatch mode: manual prompt fallback; no observed CLI receipt is claimed.
