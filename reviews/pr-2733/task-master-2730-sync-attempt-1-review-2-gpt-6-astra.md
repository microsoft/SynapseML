# Round 2 architecture and patterns review

## Review summary

| Field | Evidence |
| --- | --- |
| Target | microsoft/SynapseML#2733, base `spark4.0` |
| Scope | Only the nine-file staged import of microsoft/SynapseML#2730 |
| Baseline / reviewed HEAD | `d08aefee223d5fd7024a78dcd5e19a60b323967d` |
| Common merge base | `0a7fdafaa33ff4785dadc8d7eebee68efde110fb` |
| Master / MERGE_HEAD | `681bd96990c421de3b91d2b1bf8f8f470764199d` |
| Staged source tree | `ce3d1c2567a6d8def64fb7d926b4b41556302fc8` |
| Round / attempt / actual model | 2 / 1 / `gpt-6-astra` |
| Mode | Sequential, explicitly authorized GPT fallback |
| Findings / verdict | **0 / CLEAN** |
| Artifact | `reviews\pr-2733\task-master-2730-sync-attempt-1-review-2-gpt-6-astra.md` |

The intended Gemini slot was unavailable after a reported pre-execution
failure. No Gemini probe or review ran in this round. This GPT fallback does
not establish Gemini coverage or a complete three-family gauntlet.

## Snapshot and evidence basis

Rechecked HEAD, MERGE_HEAD, the complete staged tree, and the index-manifest
SHA-256 against round 1. All are unchanged, with no unresolved entries or
unstaged tracked changes. The source tree above is the fingerprint captured
with `git write-tree` before the round-1 report, not a new commit.

SHA-256 of raw `git ls-files --stage -z` output:

`f1ba864e6cd04958efc07fc672b38cc36ad48222b5e2aa023d1000819a7227fa`

Used retained source reads and the exact nine-file inventory in
`reviews\pr-2733\task-master-2730-sync-attempt-1-review-1-gpt-6-astra.md`.
The unchanged snapshot preserves that report's mode/blob comparisons and
relative-link checks; no broad rediscovery or tests were performed.

## Architecture and contract evidence

- [x] All nine incoming files remain mode/blob-identical to master and the
  sibling port. None had a pre-existing port-only edit relative to the common
  base, and every other tracked path remains baseline-identical. The import
  follows the normal-merge policy rather than inventing a separate port
  implementation. Spark 4.0.1, Scala 2.13.16, Python 3.12.11, dependencies,
  pipeline, templates, and existing Fabric/streaming/GPU policy are untouched.
- [x] `.github\skills\synapseml-external-contributor-review\SKILL.md` and
  `.github\skills\synapseml-external-contributor-review\references\contributor-safety.md`
  place authority outside contributor
  content: trusted guidance is pinned or separately installed; a newly
  proposed skill cannot authorize its own execution. Unknown membership
  retains safeguards, and ownership/edit access does not grant permission to
  run code or expose credentials. Follow-up work and CI require separate,
  scoped authorization. The workflow and comment template preserve authorship,
  existing discussion, and contributor confirmation rather than treating access as consent.
- [x] `.github\skills\synapseml-pr-loop\SKILL.md`,
  `.github\skills\synapseml-pr-loop\references\readiness-gates.md`, and
  `.github\skills\synapseml-pr-loop\references\ci-triage.md` consistently
  separate read-only evidence gathering, approved mutation, and CI approval.
  The unchanged `.github\skills\synapseml-pr-loop\scripts\Get-PrReadiness.ps1:55-82,350-359,421-441` makes
  `-RunPipeline` opt-in; its review/check-appearance wait is distinct from
  pipeline-completion monitoring. The new guidance no longer bundles a
  mutating trigger into the external-PR waiting loop.
- [x] `.github\skills\synapseml-pr-loop\scripts\watch_azure_pipeline.py`
  keeps bounded responsibilities: URL/build identity validation at 35-63,
  GitHub transport at 66-100, monitoring at 103-189, CLI validation at 192-229,
  and JSON/exit-code reporting at 232-270. It uses standard-library code and
  `gh`, not Spark, a cloud SDK, a new dependency framework, or repeated model
  invocations. Importing it does not start the CLI.
- [x] The watcher and waiting guide agree on the fixed upstream repository,
  trusted Azure project URLs, 600-second sleep cadence, and a maximum deadline
  measured from the supplied original kickoff. A restart using that verified
  kickoff consumes only the remaining budget. Replacement/head-change results
  stop rather than silently following a new run or resetting the clock.
  Kickoff provenance remains the caller's documented verification duty; the
  watcher does not query Azure queue time or authorize CI.
- [x] `tools\ci\tests\test_watch_azure_pipeline.py` exercises the imported
  helper and CLI boundary with controlled clocks and subprocess responses.
  Placement under `tools\ci\tests` reuses existing CI discovery without a
  pipeline change. These are portable unit contracts, not evidence of a
  live Azure run. Prior syntax checks covered both port Python grammars;
  nothing requires a branch-specific watcher or test variant.
- [x] All 19 incoming relative file links resolve within the repository.
  Links connect the trusted-guidance, CI, readiness, and writing documents
  without machine-local paths.
  `.github\skills\synapseml-pr-loop\references\writing-prs.md` preserves required
  template fields and keeps risks/current validation visible rather than
  hiding them in expandable implementation details. Proposed guidance remains
  review material, not authority for this reviewer's actions.

## Validation provenance and limits

The requester reports both full helper suites passed **321 tests plus 63
subtests**, and Black **22.3.0** passed the new Python files. Those results
belong to the unchanged source snapshot; this round did not rerun them.

Prior native CI is baseline evidence only. The reported watcher for prior
Spark 4.1 build **237100199** has original kickoff **2026-09-22 15:25:43 UTC**
and deadline **17:25:43 UTC**. The reported pending state is not success or validation
of this staged import. This round did not query, restart, or wait for it.
Fresh-head CI remains separate work after publication.

No concrete architecture, repository-pattern, or contract-consistency issue
was found. Only this unstaged report was written; no source edits, staging,
commits, tests, CI operations, or later rounds were performed. CLEAN is bounded
to round 2, not a full merge-readiness decision.
