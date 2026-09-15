# Round 4: detailed correctness

## Review summary

- **Round**: 4
- **Theme**: Detailed correctness, data flow and types
- **Mode**: Direct, sequential; no agents
- **Model / effort**: `gpt-6-astra` / `max`
- **Artifact**: `reviews/pr-2628/pr-2628-attempt-3-review-4-gpt-6-astra.md`
- **Reviewed HEAD**: `9dea133c0820d9e27e68ed04661b18d948575e65`
- **Target**: `master`, resolved from the supplied base and local `upstream/master`
- **Dirty delta**: 10 tracked files, 757 insertions and 58 deletions against HEAD. Four implementation files, four test files, `scripts/release/README.md` and `reviews/pr-2628/README.md`. Three existing untracked Round 1-3 reports were excluded from historical re-review.
- **Delta SHA256**: `a66752b7244dded9e1b83fda48a5ab94c5dc283f7437af8601d724c9d1ea7daa`
- **Issues found**: 0
- **Verdict**: CLEAN

The fingerprint covers `git diff --no-ext-diff --no-textconv --binary HEAD --`, before adding this report.

## Evidence checklist

- [x] Read `AGENTS.md`, the applicable branch rules and the current dirty diff. Traced changed release code into its immediate callers and ledger validation, without auditing unrelated runtime code.
- [x] `scripts/release/release_ops.py:2518-2566,2973-3024`: the timeout rollback preserves the root ledger object through `clear`/`update`, carries forward the saved revision and recomputes its checksum on save. An offline probe using the real `StateStore` observed revisions `1,2,3,4` and statuses `planned -> unknown -> planned -> planned`, with one root object, zero submissions, a matching final report, successful ledger reload and released locks. Selected regressions also exercised equality with the deadline, grouped operations and later continuation.
- [x] `scripts/release/release_ops.py:3097-3197`: 21 explicit empty/whitespace `--state` variants returned exit `2` without probes or JSON output. Omitted state remained valid for preflight without changing an existing ledger. Focused tests confirmed bounded final sleep, retained pending run IDs, and exit `2` with no report after a policy-read failure followed by safe continuation.
- [x] `scripts/release/release_matrix.py:595-617` and `scripts/release/release_guard.py:67-79`: file, stdin and Maven admission share duplicate-member rejection at nested and root levels. Read errors retain the input path. Offline probes distinguished invalid base64, malformed JSON, invalid JSON encoding and duplicate members; a UTF-8 BOM file retained its plan identity.
- [x] `scripts/release/release_matrix.py:815-843`: validation precedes exclusive file creation. Existing bytes survive a second generation attempt. A persistence failure returns exit `2`, emits no success stdout and explicitly marks the retained output unusable. Preserving that file is deliberate; the failure path does not unlink a pathname it cannot safely reclaim.
- [x] `scripts/bump-version.py:232-256,638-654`: both discovery and the post-write sweep use the same repository-relative ancestor exclusion. Two successive invocations of the actual bump script preserved release fixtures byte-for-byte while updating a live `core/release/runtime.py` pin.
- [x] Ran 25 selected cases from `scripts/release/test_release_ops.py`, `scripts/release/test_release_plan.py`, `scripts/release/test_release_guard.py` and `scripts/test_bump_version.py`: 25 passed, with one unknown-`slow`-mark collection warning. Additional offline probes supplied the alias/revision, whitespace-state and encoding traces above.
- [ ] Native pushed-head previews and CI remain pending. No network, live queue, full-suite, installation, commit or push operation was performed in this round.

Clean Round 4: zero confirmed actionable findings. This is not publication-path evidence or a verdict on the remaining review rounds.
