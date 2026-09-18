# Post-sync guidance review

The first sections record the documentation-only revisions. The later CI and
Fabric test changes are reviewed in the final section.

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
validation, and clarity. The initial cleanup reduced the seven branch-skill
documents from 735 to 309 lines, before the review clarifications below.
Checks found no fixed PR/build/commit/date references, verified
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

The JDK source map also links the pipeline directly because replay selects its
JDK outside the Java templates. Measurements are revision snapshots, not a
running inventory: after clarification, the seven guides total 313 lines and
all 26 local links and anchors resolve. The earlier 309-line result describes
the initial cleanup only.

## Revision: CI and Fabric lifecycle repair

Direct, single-agent review across six themes. No independent multi-model
review or cloud-runtime pass is claimed.

| Theme | Evidence and disposition |
| --- | --- |
| Correctness | Completed and failed notebook operations release their owned SJD before a worker starts the next notebook. The shared store remains suite-scoped. Failed deletions remain tracked for final cleanup. |
| Architecture | Changes reuse the existing tracker, notebook concurrency limit, and final cleanup. No production API, dependency pin, runtime selection, or branch enablement changes. |
| Failure paths | The original job exception survives cleanup failure, with the latter suppressed. Cleanup failure after successful work fails visibly. Already-deleted artifacts retain the existing handling. |
| Replay | Only Markdown under `reviews/` gains an exclusion. Executable review files and mixed changes still replay. Strict conflict handling remains. The obsolete prerequisite list was drained after checking the integrated backports. |
| Coverage | All 82 pipeline regressions passed, including five scratch-Git path cases and three retry contracts. Core compilation, test compilation, both Scala style tasks, and 15 lifecycle/naming tests passed on JDK 11. Pinned Black passed. |
| Hardening | Credential reads and coverage publication retry twice but still fail after exhaustion. TLS verification and required coverage remain enabled. Cleanup is limited to IDs created and tracked by the running tests. |

An isolated index replayed the actual three-file Fabric repair onto the current
Spark 4.1 target without prerequisites. All resulting file blobs match the
master repair. This proves patch application, not full runtime compatibility.

The fixture-capacity regression runs six jobs with room for only one store and
one job. Other regressions cover failed jobs, failed cleanup, retained cleanup
work, and exception identity. The earlier red stage for the new tracker API
was a test-compilation failure, not a runtime baseline.

No open finding in this direct review. Branch-specific compilation and fresh
CI remain required. Early deletion cannot guarantee capacity in an already
saturated shared workspace, and bounded retries cannot repair a persistent
certificate or service configuration error.
