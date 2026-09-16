# Master Spark 4 branch guides, attempt 1, round 5

## Review summary

- Round: **5 only**, Testing & Coverage, sequential, `gemini-3.8-flash` (high reasoning).
- Verdict: **CLEAN** for test specification accuracy, verification guidance, and documentation-test alignment. Issues found: **0**.
- Scope: Verification of test expectations, branch guidance alignment with actual test behavior, and landed vs candidate test contract separation.
- No agents/factories launched; no product/doc/test source edits; no staging, commits, pushes, or cloud calls.
- Only this review artifact is created; prior review artifacts remain intact.

## Snapshot

| Item | Value |
| --- | --- |
| Worktree | `C:\Users\singhrana\Documents\SynapseML\.worktrees\branch-context-20260916` |
| Branch | `docs/spark4-branch-context-20260916` |
| Target master and HEAD | `1305587a4afe92d27c8e28894b90e38020252e04` |
| Staged index tree (`write-tree`) | `e4be5ce43f28be90f7346d817feccc3eed00841a` |
| Working tree delta vs index | 0 files (working tree matches staged index) |
| Target objects described | `ecec8dd58b7a07ebc24d816e321a85ff5dc19d57` (4.0), `06897e5b27e28d84ce7ffa33e93d7f756992d0f2` (4.1) |
| Snapshot check timestamp | `2026-09-16T10:25:00Z` |

## Evidence checklist

- [x] **Test Expectation & Coverage Alignment**: Inspected `.github/skills/synapseml-branches/references/branch-spark4-common.md:100-135`. Testing guidance accurately reflects Spark 4 test requirements: warning that Scala 2.13 collection mismatches fail at runtime with `ClassCastException`, and advising that newly imported service paths be tested with actual Spark DataFrames rather than static immutable collections.
- [x] **Landed vs Candidate Test Contract Separation**: The guide correctly specifies that typed `SAR.ItemAffinity` (fixing `UnboundRowEncoder`) is present on landed Spark 4.0 target `ecec8dd58b`, while landed Spark 4.1 target `06897e5b27` continues using `Seq[Row]`. Both qualify `col("sarUserFactors.flatList")` against `DetectAmbiguousSelfJoin`. It does not invent unverified test expectations for 4.1.
- [x] **Default Guard Test Scope**: Clarification accurately restricts landed `safeGetDefault` behavior to Python wrappers while explicitly identifying the unguarded R lookup (`RWrappable.rParamArg:648`) as a pre-existing upstream condition. Test instructions explicitly direct preserving the Python guard for newly imported stub generation paths.
- [x] **Assertion Preservation Guidance**: Verified guidance regarding `VerifyTrainClassifier.scala:121`: instructs preserving the valid vector fixture rather than restoring master's `Double.NaN` or weakening the classification assertion.
- [x] **AGENTS.md & Repository Rules Compliance**: Shared files (`AGENTS.md`, `CONTRIBUTING.md`) remain untouched and free of branch-specific version numbers or runtime paths. Version-specific test matrices are strictly confined to the branch skill references (`branch-spark4p0.md`, `branch-spark4p1.md`).
- [x] **Documentation Integrity**: Verified `git diff --check HEAD` returns zero whitespace or formatting issues. Local links and anchors resolve consistently.

## Testing & coverage assessment

1. **Test Verification Guidance**: The documentation provides unambiguous instructions for validating Spark 4 port merges, preventing false-positive compile-only passes and ensuring runtime behavior is verified.
2. **Regression Guarding**: Clarified boundaries around `safeGetDefault` and SAR representations protect future syncs from either dropping necessary adaptations or hallucinating unneeded refactorings.

## Findings and limitations

- **Verdict:** **CLEAN** (0 documentation-test misalignments or inaccurate test criteria).
- **Limitations:**
  - Guide statements describe code behavior and test contracts verified on local checkouts; remote Azure Pipelines CI has not yet run for the port PR candidates.
  - Documented Fabric E2E disabled status reflects actual repository policy; master baseline `236185691` failure during Fabric provisioning confirms remote Fabric testing is currently unavailable.
