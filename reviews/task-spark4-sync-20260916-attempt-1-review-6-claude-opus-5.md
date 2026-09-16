# Spark 4.1 sync, attempt 1, round 6

## Review summary

- Round: **6 only**, Polish & Hardening, sequential, `claude-opus-5` (max reasoning).
- Verdict: **CLEAN**. Concrete findings: **0**. Precision notes: 2 (neither is a defect).
- Scope: merge adaptations, documentation accuracy, performance/observability implications,
  and naming. Not a re-audit of unchanged upstream algorithms.
- No agents or factories spawned. No source, docs, tests, staging, commits, or remotes touched.
  Only this artifact was written; rounds 1-5 remain byte-intact.

## Snapshot

| Item | Value |
| --- | --- |
| Worktree | `C:\Users\singhrana\Documents\SynapseML\.worktrees\sync-spark41-20260916` |
| Branch | `sync/spark4.1-master-20260916` |
| Target / HEAD | `06897e5b27e28d84ce7ffa33e93d7f756992d0f2` |
| Master / MERGE_HEAD (real, retained) | `1305587a4afe92d27c8e28894b90e38020252e04` |
| Computed merge base | `a833941704b5e8334ddb40a9d601d7e0c7c0ce9f` |
| Incorporated master content | `a6fd536ad76eb1b60ac82f31a362ae624886c6ff` |
| Staged index tree (`write-tree`) | `1d13aec1838335846c676c256ea63f922b76192e` (identical to round 5) |
| Unmerged paths / unstaged tracked | 0 / 0 |
| `git diff --check HEAD` | 0 issues |
| Snapshot timestamp | `2026-09-16T10:40:00Z` |

Source is unchanged since round 4. All Git calls used the native client with
`GIT_OPTIONAL_LOCKS=0` and `-C` on this exact worktree.

## Evidence checklist

- [x] **Adaptation set recounted independently.** Intersecting `diff --cached --name-only MERGE_HEAD`
  with `diff --cached --name-only HEAD` yields 45 paths differing from *both* parents; 3 are staged
  review artifacts, leaving **42 true merge adaptations**. Byte-exact incoming master paths were not
  re-examined.
- [x] **Docker helper change is minimal and correctly scoped.** `tools/ci/get_python_version.sh` differs
  from master by exactly one regex and one comment: `^[0-9]+\.[0-9]+\.[0-9]+$` becomes
  `^[0-9]+\.[0-9]+(\.[0-9]+)?$`. Both anchors, the `set -euo pipefail` header, the
  exactly-one-match count, the `sed` extraction, the stderr message, and the verbatim output are
  unchanged. The short-circuit order still evaluates `version_count -ne 1` before the format test,
  so multiple pins fail on count rather than reaching the grammar check.
- [x] **The relaxation matches the preserved branch pin, and only it.** `environment.yml` selects
  `python=3.13`; the new grammar accepts `3.13` and still rejects `3`, `3.13.*`, `3.13.0.1`, `3.13rc1`,
  and `>=3.13`. No environment pin, dependency, workflow, or pipeline trigger changed.
- [x] **Consumer contract intact.** Both `tools/docker/{demo,minimal}/Dockerfile` receive the emitted
  value as a quoted `python=${PYTHON_VERSION}` Conda selector; nothing downstream parses a patch
  component, so a minor-series value flows through unchanged.
- [x] **Documentation matches the code exactly.** The new `tools/ci/README.md` section states the helper
  "accepts one numeric `major.minor` or `major.minor.patch` dependency and emits it unchanged, preserving
  a port branch's minor-series selection", and that missing, multiple, or malformed specifications fail
  the build. Each clause maps to a specific line of the script. The section is additive; no existing
  README text was altered.
- [x] **Cross-port divergence is justified, not drift.** Spark 4.0 keeps master's byte-identical helper
  and README (`5b0c77dcdc`, `2c7d22d8fe`) because its `python=3.12.11` pin already satisfies the stricter
  grammar. The relaxation exists only where the branch invariant requires it.
- [x] **Test naming tracks the new behavior.** `test_rejects_minor_only_python_version` was renamed to
  `test_extracts_minor_series_python_version` rather than left describing the inverted expectation, and
  `test_rejects_unpinned_python_version` became the parametrized
  `test_rejects_unpinned_or_malformed_python_version`. `test_extracts_repository_python_version_for_docker`
  names the production no-argument path it covers. `test_script_avoids_bash_four_only_mapfile` and the
  `_bash_path` helper are unchanged.
- [x] **No hot-loop, algorithm, or public-signature change from any resolution.** The combined LightGBM
  deltas are confined to `LightGBMBooster.scala:461,482` (`session.createDataset` in the
  `saveNativeModel`/`dumpModel` save paths) and `LightGBMBase.scala:239` (`.distinct.toArray`, once per
  fit). The Spark 4.1 streaming difference remains exactly two files: `HTTPSource.scala:15` and
  `DistributedHTTPSource.scala:15` importing
  `org.apache.spark.sql.execution.streaming.runtime.LongOffset`.
- [x] **Shared combined files resolve two-sidedly, verified against each parent separately.**
  `UntypedArrayParam.scala` takes master's new null/`Long`/`Short`/`Byte`/`Float` cases and ordered
  `JsObject` while preserving the port's widened `case v: scala.collection.Seq[_]`, which is what keeps a
  Spark-supplied mutable sequence from reaching `throwFailure`; `AnalyzeText.scala` keeps the same
  widening; `CleanMissingData.scala` and `Repartition.scala` take master's documentation fixes without
  disturbing port logic. These files are byte-identical to the Spark 4.0 candidate's copies.
- [x] **CI policy preserved, not merely present.** `pipeline.yaml` keeps `condition: false` on `FabricE2E`
  with its branch comment rather than adopting master's gate.
  `.github/workflows/pr-validation.yml` takes master's newer `actions/setup-java` pin (v6.0.1) and the
  `JAVA_TOOL_OPTIONS` addition while retaining `java-version: 17`, the `Set up JDK 17` step name, its
  "Spark 4.1 requires Java 17; do not take master's JDK 11 when syncing" guard comment, and
  `python-version: "3.13"` against master's `"3.12"`.
- [x] **Duplicate constant cleanup is complete.** `tools/ci/tests/test_pipeline_yaml.py` retains a single
  `BUILD_SBT` after `REPO_ROOT`; no assertion or Key Vault check was removed with the duplicate.
- [x] **Whitespace and marker hygiene.** `git diff --check HEAD` reports zero.

## Findings

**CLEAN — no concrete polish or hardening defect found in this round's scope.**

Two precision notes, neither actionable in this PR:

1. **`test_extracts_repository_python_version_for_docker` hard-codes `3.13`.** Asserting against the real
   `environment.yml` is what makes it prove the production Docker path rather than a synthetic fixture,
   and it doubles as a guard against an accidental pin change during a future sync. The trade-off is that
   any deliberate Python bump on this branch must update this assertion in the same commit. Intentional
   coupling, recorded so it is not later mistaken for a flaky test.
2. **The helper's stderr message is inherited verbatim from master.** "Expected exactly one pinned
   `python=<version>` dependency" now also fires for a single malformed value, where "exactly one" reads
   oddly. The wording predates this sync and the port did not touch that line; the README documents the
   accepted forms. Rewording it would be an unrelated upstream edit.

No prior-round artifact in this worktree was found to overclaim. Round 4's closing note that generated
syntax checks used Python 3.14.6 only was superseded by the owner's later Python 3.13.13 / PySpark 4.1.1
run; that correction is already recorded in `validation-report.json`, and round 5 cites the newer
evidence, so the historical statement is dated rather than wrong.

## Limitations

Static, evidence-based inspection of the staged snapshot plus Git object comparison against both pinned
parents. No suites were rerun in this round: the compile, style, 235-test/26-suite, all-module codegen,
142-Linux-CI, 213-release-test, 10-parser-test, 77-pipeline-test, Black 22.3.0/214-file, and the
Python 3.13.13 / PySpark 4.1.1 two-export, 325 `.py` / 222 `.pyi` parse and 3 schema-import results are
cited from the owner's record, not reproduced. No JVM-backed Python or R runtime, Docker image build,
Databricks, GPU, or cloud execution is claimed. The release-snapshot QuantileRegression failure on
unanchored version references reproduces on exact master and is correctly not counted as a candidate
pass. `FabricE2E` stays disabled by design; master baseline `236185691` failed Fabric provisioning before
any test ran. Remote Azure Pipelines CI is planned after the PRs exist; its absence is a stated gap, not a
code defect. Rounds 4-6 artifacts are untracked here — the parent owns staging.

## Resolution

Nothing to resolve. No source, documentation, or test change is requested by this round.
Sequential review of this candidate ends at round 6 with **CLEAN**.
