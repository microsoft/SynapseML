# Master Spark 4 branch guides, attempt 1, round 2

## Review summary

- **Round:** 2.
- **Theme:** Architecture & Patterns (documentation consistency, reference clarity, convention adherence).
- **Mode:** Sequential.
- **Model:** `gemini-3.8-flash`, high reasoning requested.
- **Issues found:** 0.
- **Verdict:** CLEAN.
- **Artifact:** `reviews\task-spark4-sync-20260916-attempt-1-review-2-gemini-3.8-flash.md`.

This round evaluates documentation architecture, convention adherence, and cross-reference accuracy across the updated Spark 4 branch guides. The documents strictly adhere to repository architectural rules: `AGENTS.md` and `CONTRIBUTING.md` remain version-agnostic and identical across branches, while branch-specific runtime facts, version matrices, and port-specific adaptations reside strictly in `.github/skills/synapseml-branches/references/`. The guides now accurately distinguish typed `SAR.ItemAffinity` on Spark 4.0 from `Seq[Row]` on Spark 4.1, document the `safeGetDefault` guard requirement for Python stub generation, articulate the squash-merge content baseline vs. Git ancestry model, and explain GitHub merge settings without altering repository configuration.

No agents were launched. No files outside this review artifact were created or modified. No commits or CI runs were triggered.

## Source snapshot

| Item | Reviewed value |
| --- | --- |
| Worktree | `C:\Users\singhrana\Documents\SynapseML\.worktrees\branch-context-20260916` |
| Branch | `docs/spark4-branch-context-20260916` |
| Target master and HEAD | `1305587a4afe92d27c8e28894b90e38020252e04` |
| Staged index tree (`write-tree`) | `14e3057db49bd6e46133fd7beb4bd776071be383` |
| Raw stage hash (`ls-files -s -z`) | `d6f822cdc510b93f4216dcc949d48e3605a23a67c4ae59c592966762db3da9d8` |
| Working tree delta vs index | 0 files (working tree matches staged index) |
| Snapshot check timestamp | `2026-09-16T09:20:00Z` |

## Evidence checklist

- [x] **AGENTS.md & CONTRIBUTING.md Purity**: Confirmed `AGENTS.md` and `CONTRIBUTING.md` remain untouched and byte-identical to master. The architectural principle that shared root guides must not contain branch-specific or version-specific details is strictly maintained.
- [x] **Documentation Separation of Concerns**: Verified that version matrices, runtime requirements, and deliberate port differences are properly partitioned:
  - `branch-spark4-common.md`: Shared architectural concepts (squash-merge provenance tracking, GitHub merge commit policies, collection conversion boundaries in `CognitiveServiceBase`, zero-arg `super()` in `_OpenAIPrompt`, and `safeGetDefault` in Python stub generation).
  - `branch-spark4p0.md`: Pinned Spark 4.0 facts (Spark 4.0.1, Scala 2.13.16, Python 3.12.11, NumPy 1.26.4, DBR 17.3, `LongOffset` in `execution.streaming`, and typed `SAR.ItemAffinity`).
  - `branch-spark4p1.md`: Pinned Spark 4.1 facts (Spark 4.1.1, Scala 2.13.17, Python 3.13, unpinned NumPy, DBR 18.0, `LongOffset` in `execution.streaming.runtime`, `np.frombuffer` image conversion, and `Seq[Row]` SAR).
- [x] **Accurate SAR Port Differentiation**: Inspected `branch-spark4-common.md` (lines 111-118). The documentation now explicitly differentiates the ports: `spark4.0` uses `SAR.ItemAffinity` with explicit `itemIndex`/`affinity` fields, `spark4.1` uses `Seq[Row]`, and both qualify `col("sarUserFactors.flatList")`. It avoids attributing `ItemAffinity` to Spark 4.1.
- [x] **Python Stub Guard Guidance**: Verified `branch-spark4-common.md` includes explicit instructions directing newly introduced Python stub generation paths to preserve `Wrappable.safeGetDefault` when inspecting parameter defaults.
- [x] **Squash-Merge Ancestry Architecture**: Verified the guides clearly explain why commit ancestry diverges from content presence due to prior squash merges (#2659, #2661), correctly documenting the last integrated content baseline (`a6fd536ad7`) and warning reviewers against assuming missing commits mean missing features.
- [x] **Repo Merge Policy Guidance**: Confirmed the guidance explains GitHub's disabled merge commits and permitted squash merges, advising PR authors to retain merge parents in working branches and document the incorporated master SHA without attempting to modify repository settings.
- [x] **Style & Formatting Conformance**: Ran `git diff --check HEAD` on the worktree; verified zero trailing whitespace, clean markdown formatting, and valid local cross-references.

## Architectural findings

No documentation defects, misclassifications, or architectural violations were identified. The updated guides provide accurate, modular, and actionable reference material that preserves architectural clarity across current and future sync cycles.

## Validation limitations

This review assesses documentation accuracy, structure, and consistency against target branch Git objects (`1305587a4a`, `ecec8dd58b`, `06897e5b27`). It does not run live cloud pipelines or alter external repository settings. Subsequent rounds (Rounds 3-6) were not executed.

## Resolution log

- Round 1 Issue 1 (Attributing typed SAR affinity rows to both ports in `branch-spark4-common.md`): Verified resolved. The guide now accurately records that typed `SAR.ItemAffinity` is specific to Spark 4.0, whereas Spark 4.1 preserves `Seq[Row]`, and both qualify the `SARModel` join column.
