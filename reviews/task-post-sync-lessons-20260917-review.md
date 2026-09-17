# Post-sync guidance review

## Scope and evidence

Direct, single-agent review of three documentation changes:

- `.github/skills/code-review/SKILL.md`
- `.github/skills/synapseml-branches/references/branch-spark4-common.md`
- `.github/skills/synapseml-pr-loop/references/ci-triage.md`

The six themes below were reviewed directly. This is not an independent
multi-model review, and it does not clear required maintainer or CI gates.
No product code, dependency pins, pipeline definitions, or release tooling
are changed by this patch.

The landed source trees were compared with the reviewed PR sources:

| Target | Landed commit | Reviewed source | Tree comparison |
| --- | --- | --- | --- |
| master | `cd45147c70` | `7c1bf9eb56` | Identical |
| spark4.0 | `0e23141685` | `1fc510fbeb` | Identical |
| spark4.1 | `2ac299ddd6` | `0994105f11` | Identical |

Sources: [#2719](https://github.com/microsoft/SynapseML/pull/2719),
[#2718](https://github.com/microsoft/SynapseML/pull/2718), and
[#2720](https://github.com/microsoft/SynapseML/pull/2720).

## 1. Completeness

Cold Spark startup, modern no-SDK import blocking, async loop ownership,
fail-fast cleanup, review/build provenance, retry accounting, and post-merge
follow-ups are covered. No open finding.

## 2. Consistency

Testing rules live in the existing code-review checklist, CI diagnostics in
the triage reference, and landed branch facts in the branch reference.
Cross-links avoid duplicating these rules. Root contributor guides and active
branch scope remain unchanged. No open finding.

## 3. Edge cases

Finding: a general cancellation rule could incorrectly change the contract of
a best-effort batch API.

Resolution: the checklist explicitly applies cancellation and sibling draining
to fail-fast batches. Supported nested-loop paths and optional dependencies
are qualified rather than imposed on every API. The final text was reread.

## 4. Correctness

Landed commits and source-tree equality were verified through GitHub and Git.
The old configuration snapshot is explicitly historical. Import blocking
distinguishes an exception from a finder declining to handle an import.
No open finding.

## 5. Validation coverage

The complete diff passed `git diff --check`. The edited skill retains matching
frontmatter and stays under 500 lines. Both new cross-reference files and
heading anchors were checked. No executable behavior changed, so no Spark
runtime pass is claimed for this documentation patch.

## 6. Hardening and clarity

Infrastructure guidance now distinguishes setup failures from post-test
publication failures. It preserves required publication, review, and replay
gates, and requires approval before protected tooling changes. No credentials,
private implementation details, or machine-local evidence paths are included.
No open finding.

## Revision: durable branch guidance

User feedback: branch references should support future sessions without needing
an update after every PR. Historical evidence above remains in this audit
record, not in the branch guidance.

Removed PR/commit/build chronology, benchmark anecdotes, copied pin matrices,
and repeated procedures. Preserved branch-specific compatibility and runtime
boundaries, with live source links and shared testing/CI references.
The skill and reference template now explicitly reject running incident logs.

Direct review covered completeness, consistency, edge cases, correctness,
validation, and clarity. All seven branch-skill documents now total 309 lines,
down from 735. Checks found no fixed PR/build/commit/date references, verified
25 local links and anchors, and passed `git diff --check`.
No executable code, runtime pins, root contributor guides, or CI tooling changed.

### Review clarification

The automated review requested a broader JDK source map, qualification of Fabric
support, and preservation of the primary-runtime replay and SAR boundaries.
The source map now links all Java templates, including the separate CLI setup.
The master reference explains duplicate replay coverage and suite selection.
The Spark 4.1 reference distinguishes runtime availability from branch support
and keeps its Row representation separate from the Spark 4.0 encoder workaround.

Verified against the Java templates, branch pipeline/workspace configuration,
and both ports' SAR implementations. Direct six-theme review found no further
documentation issue. These are decision rules, not configuration snapshots;
no runtime or pipeline change is implied.
