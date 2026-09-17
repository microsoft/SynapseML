# Master Spark 4 branch guides, attempt 1, round 6

## Review summary

- Round: **6 only**, Polish & Hardening, sequential, `claude-opus-5` (max reasoning).
- Verdict: **CLEAN**. Concrete findings: **0**. Precision notes: 1 (not a defect).
- Scope: documentation accuracy of the refreshed branch-context references, plus the naming and
  observability implications of what they instruct a future sync to do. Not a re-audit of the ports.
- No agents or factories spawned. No source, docs, tests, staging, commits, or remotes touched.
  Only this artifact was written; rounds 1-5 remain byte-intact.

## Snapshot

| Item | Value |
| --- | --- |
| Checkout | Repository root, branch `docs/spark4-branch-context-20260916` |
| Branch | `docs/spark4-branch-context-20260916` |
| Target master / HEAD | `1305587a4afe92d27c8e28894b90e38020252e04` |
| Staged index tree (`write-tree`) | `e4be5ce43f28be90f7346d817feccc3eed00841a` (identical to round 5) |
| Unmerged paths / unstaged tracked | 0 / 0 |
| `git diff --check HEAD` | 0 issues |
| Changed docs | `branch-spark4-common.md`, `branch-spark4p0.md`, `branch-spark4p1.md` |
| Objects described | `ecec8dd58b7a07ebc24d816e321a85ff5dc19d57` (4.0), `06897e5b27e28d84ce7ffa33e93d7f756992d0f2` (4.1) |
| Snapshot timestamp | `2026-09-16T10:40:00Z` |

Source is unchanged since round 4. `AGENTS.md` and `CONTRIBUTING.md` are untouched. Every claim below
was checked against pinned Git objects with the native client, `GIT_OPTIONAL_LOCKS=0`, and `-C`.

## Evidence checklist

- [x] **Fabric gating description matches master exactly.** `branch-spark4p1.md` now says master "gates
  this job on success, `runTests`, `testFabricE2E`, and a non-fork PR". Master's `pipeline.yaml` reads
  `and(succeeded(), eq(variables.runTests, 'True'), eq('${{ parameters.testFabricE2E }}', true),
  ne(variables['System.PullRequest.IsFork'], 'True'))` — four clauses, described in order and without
  embellishment. The replaced text had asserted a third, distinct `spark4.0` form; the new
  "`spark4.0` also uses `condition: false`" is verified against target `ecec8dd58b`, whose job carries a
  bare `condition: false` plus its managed-runtime comment.
- [x] **Squash-ancestry warning is accurate and non-obvious.** The guide tells a future sync to compare
  against the recorded content baseline as well as the real merge base. Verified:
  `merge-base --is-ancestor a6fd536ad7 <4.0 target>` exits 1 (not an ancestor) while
  `merge-base` computes `a833941704`. The two genuinely disagree, so the instruction prevents a real
  failure mode rather than restating Git behavior.
- [x] **Repository-settings claim is hedged correctly.** The text reads "At this snapshot GitHub reports
  `allow_merge_commit: false` and allows squash merges" and immediately adds "Do not change repository
  merge settings as part of a sync." It is scoped as an observation with a timestamp qualifier, does not
  present an external setting as an invariant, and asks for no settings change.
- [x] **Runtime matrix spot-checked against the objects, not carried over.** The JDK table row
  `` `.github/workflows/pr-validation.yml` | 11 | 17 | 17 `` is exact: master pins `java-version: 11`
  there, while both pinned targets pin `17`. The PyArrow/MLflow claim holds — both targets pin
  `pyarrow==18.0.0` and `mlflow==2.21.3` — and the asymmetry the guide draws is real: 4.0 pins
  `numpy==1.26.4`, 4.1 leaves `numpy` deliberately unpinned with the Python 3.13 wheel rationale in place.
- [x] **The Petastorm rewording is a genuine correction, not a softening.** Replacing "`spark4.0` pins a
  different, newer pyarrow" with "Both ports now pin PyArrow 18.0.0 with MLflow 2.21.3" matches the
  measured pins above; the preserved instruction to keep both halves (`_petastorm_compat.py` and the
  `_serialize_petastorm_compatibility()` path in `_horovod.py`) still names the exact surface.
- [x] **Landed facts stay separated from candidate work.** The guide describes target state only. Neither
  the Spark 4.1 `get_python_version.sh` relaxation nor the Python stub-default guard — both of which exist
  only in the unmerged sync candidates — is presented as landed. That separation is what keeps this
  master-facing document correct if a candidate changes before merge.
- [x] **Python-only guard and unguarded R remain precisely bounded.** The `safeGetDefault` claim is
  limited to Python wrapper lookups and names `RWrappable.rParamArg` as a pre-existing upstream
  condition; the stub-generation instruction is written as guidance for newly imported paths, not as
  already-landed behavior.
- [x] **`SAR.ItemAffinity` attribution unchanged by the round 3/4 corrections.** Still attributed to 4.0
  target `ecec8dd58b` only, with 4.1's `Seq[Row]` baseline left as-is and the qualified
  `col("sarUserFactors.flatList")` noted for both.
- [x] **Naming and structure follow the repository's own rule.** Version-specific material stays in
  `branch-spark4p0.md` / `branch-spark4p1.md` / `branch-spark4-common.md` under the branch skill, and the
  cross-branch-identical files carry none of it. Pull-request references are numeric links with stated
  merge status rather than bare assertions.
- [x] **Whitespace and marker hygiene.** `git diff --check HEAD` reports zero across all three references.

## Findings

**CLEAN — no documentation inaccuracy, stale claim, or naming problem found in this round's scope.**

One precision note, not actionable:

1. **The historical-failure references are dated by construction.** Statements anchored to external,
   mutable state — the GitHub merge-setting snapshot, the pipeline-definition filter, and the
   `reason=pullRequest` / `requestedFor=GitHub` trigger fields — are correct today and are written with
   snapshot framing, but nothing in the repository re-verifies them. That is inherent to documenting
   service configuration from a Markdown file, and the existing hedging is the right mitigation; no edit
   is requested. A future sync should re-measure rather than trust these rows, which is what the guide
   already instructs for the runtime matrix.

No prior-round artifact in this worktree was found to overclaim. Rounds 3 and 4 correctly narrowed the
earlier SAR and guard wording, and round 5's CLEAN verdict is consistent with the objects re-checked here.

## Limitations

Documentation accuracy verified against pinned Git objects and the two targets' `pipeline.yaml`,
`environment.yml`, and `.github/workflows/pr-validation.yml`. Claims that depend on live service state —
GitHub repository merge settings, Azure Pipelines definition filters, Fabric capacity — were not queried
in this round and are accepted as the snapshot the guide labels them. This is a documentation verdict,
not PR readiness for any candidate. No candidate CI, JVM-backed smoke, Docker, Databricks, or cloud
result is claimed here. Master baseline `236185691` failed Fabric provisioning before running tests and
neither supports nor refutes these documentation changes. Rounds 4-6 artifacts are untracked in this
worktree — the parent owns staging.

## Resolution

Nothing to resolve. No documentation change is requested by this round.
Sequential review of this candidate ends at round 6 with **CLEAN**.
