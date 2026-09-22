## Review summary

- Round: 1, bounded broad sweep only
- Theme: Correctness, security, logic, and job-wait exception handling
- Mode: sequential
- Model: gpt-6-astra
- HEAD: `dd33c2401ec14558af9afd9aac39c4d87c257e57`
- Target context: master, `e6f83069b117793e79264306e177a2c612cc5541`
- Scope: Combined staged and unstaged delta from `git diff HEAD`, including review relocation metadata
- Artifact: `reviews\pr-2732\task-job-wait-errors-attempt-1-review-1-gpt-6-astra.md`
- Issues found: 0
- Verdict: CLEAN

## Reviewed working-tree snapshot

| Repository-relative path | Git blob hash |
| --- | --- |
| `core\src\test\scala\com\microsoft\azure\synapse\ml\nbtest\FabricNotebookTests.scala` | `3bacdd33338c02c9e652f6970fecd130c9fb30ae` |
| `core\src\test\scala\com\microsoft\azure\synapse\ml\nbtest\FabricTestArtifactTrackerFailureTests.scala` | `85e013a489af379b76b41bf4a30221327feb5647` |
| `docs\Reference\Developer Setup.md` | `c53a82eab40ba32c5227b3c4caf52691e5052bf1` |
| `reviews\pr-2732\task-cleanup-polling-30s-attempt-1-review-6-claude-opus-5.md` | `92c66eccbed1c714f0705ab5971be2c5155e83ee` |

## Evidence checklist

- [x] Read current `AGENTS.md` and followed its numbered-PR artifact rule. Inspected the combined HEAD delta rather than overlooking staged renames or unstaged source.
- [x] Traced the shared protected final guard: the by-name body is evaluated inside `try`, successful values retain their type, and `InterruptedException` restores the current thread's interrupt flag before rethrowing the same instance.
- [x] Verified ordinary failures receive the notebook name and original throwable as cause through `NonFatal`. Excluded VM, thread-death, linkage, and control throwables escaping the guarded body are not wrapped.
- [x] Verified both call sites use the guard. Smoke retains its existing `Await.ready` and success assertion; notebooks retain `Await.result`. Monitor calls, timeout expressions, submission, and cleanup ordering are unchanged.
- [x] Inspected all three new tests: successful result, ordinary/assertion/failed-future causes, real interrupted `Await` identity and restored flag, and four excluded throwable categories.
- [x] The interrupted-wait test clears its thread flag in `finally`. The fixture overrides lazy Fabric access to throw, so these cases require no live Fabric connection.
- [x] Confirmed the new cases execute under the existing CI-selected `FabricTestArtifactTrackerSuite` through its failure-tests mixin. The completed green log lists all three there.
- [x] Inspected `master-job-wait-red.log`: 13 selected tests ran, 11 passed, and two failed because the old catch-all produced `RuntimeException` instead of the expected interruption/fatal throwable.
- [x] Inspected completed `master-job-wait-green.log`: JDK 11 core compile and Test/compile succeeded; both style checks reported zero errors and warnings; 53 tests passed across two completed suites with zero failures, canceled, ignored, pending, or aborted cases.
- [x] Checked the rename-aware review diff. All 18 relocated reports retain findings and resolution history; 12 have identical content and six update only their Artifact self-path metadata.
- [x] Checked the polling Round 6 correction: it now distinguishes per-item waiting from total run duration and preserves the original misleading paragraph as an explicitly historical quote with a resolution note.
- [x] Documentation matches the guarded exception policy. No protected CI definitions, dependency pins, credential handling, or network destinations changed. The focused source/document diff passes whitespace checking.

## Conclusion and boundary

No concrete defect found in this bounded delta. The shared guard gives smoke the existing notebook exception policy without changing waiting or monitoring semantics, and the tests exercise interruption rather than merely asserting constants or wrapper text.

Validation claims come from inspected parent-produced logs. This reviewer wrote only this report and performed no source edits, staging, commits, agent dispatch, cloud calls, or live Fabric tests. No new port validation is claimed.

The Gemini-family gate remains unavailable following confirmed backend 400 failures. Existing approval blockers remain unresolved by this review. This is a Round 1 result only, not a full gauntlet pass or readiness declaration.
