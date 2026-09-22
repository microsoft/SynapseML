# Round 1 review

## Review summary

| Field | Result |
| --- | --- |
| Target | microsoft/SynapseML#2733, base `spark4.0` |
| Worktree | Isolated port checkout; machine-local path omitted |
| Reviewed HEAD | `96e5ac204b45b1d0c83c8407ca97904c56f7bd94` |
| Merge source / MERGE_HEAD | `0a7fdafaa33ff4785dadc8d7eebee68efde110fb` |
| Reviewed staged tree | `c95db6cba93345b1d3fd3912dbb5e8cfc17acaac` |
| Common ancestor | `714d365e71f6d2db5b7072094a4a3ad22485eb57` |
| Scope | The 14 incremental staged paths against HEAD, necessary context, and master-content equivalence |
| Round / attempt / mode / model | 1 / 1 / sequential / `gpt-6-astra` |
| Runtime retained | Spark 4.0.1, Scala 2.13.16, Python 3.12.11 |
| Issues found / verdict | **0 / CLEAN** |
| Artifact | `reviews\pr-2733\task-latest-master-20260922-attempt-1-review-1-gpt-6-astra.md` |

Catalog-resolved slots are `gpt-6-astra`, `gemini-3.8-flash`, and
`claude-opus-5`. Only the GPT round-1 broad, security-conscious review ran.

## Snapshot fingerprints

SHA-256 of raw `git ls-files --stage -z` output, covering 3,894 index entries:

`e6809f9644426e5a9239bb3c1e48ecf94013c7774c062eda47b9a9030f4612bf`

SHA-256 of raw `git --no-pager diff --cached --binary --full-index --no-ext-diff --no-textconv HEAD --` output:

`e0b3ceef6755d294ae06f61d9dde198b62932d82a0ac6f27f64d093183dc6878`

These identify the staged source before this untracked review artifact.
There were no unmerged index entries or unstaged tracked changes.

## Evidence checklist

- [x] Compared HEAD, the common ancestor, master, and the index. Eleven of the
  14 staged paths match master exactly, including the three deletions. The
  exceptions are `pipeline.yaml`, inherited port guards in
  `tools\ci\tests\test_pipeline_yaml.py`, and the explicit disabled-Fabric
  adaptation in `tools\ci\tests\test_e2e_impact.py:382-392`.
- [x] Verified incoming ancestry for microsoft/SynapseML#2735 at
  `9708fd900aa2cb8f95f920cc713fcfe1c9acd84d` and microsoft/SynapseML#2736 at
  `e6f83069b117793e79264306e177a2c612cc5541`. GitHub reports the merge source
  itself as microsoft/SynapseML#2732's merged commit. Of 59 paths changed on
  master since the common ancestor, 55 match indexed content. The fourth
  exception beyond the three above is the existing disabled-Fabric explanation
  in `docs\Reference\Developer Setup.md:76-81`, unchanged against HEAD.
- [x] Parsed and compared pipeline structures. `pipeline.yaml:112-157` carries
  master's independent CIHelpers job, two-parent checkout, and detector step.
  At `pipeline.yaml:231-285`, only the CPU/GPU selection conditions change;
  matrices, templates, and other job fields match HEAD. The entire Fabric job
  matches HEAD, with `condition: false` at line 292. Other existing jobs,
  schedules, and variables are unchanged. Streaming scheduling and GPU
  runtime/capacity policy are not altered.
- [x] Reviewed `tools\ci\e2e_impact.py:39-173`: positive allowlist, union of
  mixed changes, exact queued merge/source-parent checks, rename expansion,
  rejection of non-regular modes, bounded Git calls without shell interpolation,
  and escaped diagnostics. Missing metadata, unknown inputs, and handled
  failures retain all notebook families. The actual staged path set selects
  all three families; the independent Fabric disable still wins.
- [x] The Boolean-False regression assertion is explicit, while Databricks
  fail-open conditions and every E2E dependency assertion remain.
  `tools\ci\tests\test_e2e_impact.py:424-454` still checks all 54 coverage
  uploads against both `codecov.yaml` thresholds. Coverage-producing job
  definitions remain structurally identical to HEAD after YAML parsing.
- [x] `GeospatialCoreSuite.scala:168-202` preserves scalar and column-bound
  parameters through save/load and checks the retirement exception after load.
  `CheckPointInPolygon.scala:30-38` throws before HTTP execution, and
  `TestBase.scala:159-163,194-199` owns temporary-file cleanup. The ValueIndexer
  edits at lines 42-45 and 76-90 remove only unused duplicate loop wrappers.
  The deleted runner's former Scala 2.13 path adjustment does not justify
  retaining it: `CodegenPlugin.scala:428-448` already invokes pytest directly,
  and inspected build/CI callers do not reference the retired runner.
- [x] `build.sbt` and `environment.yml` are unchanged against HEAD. No public
  production source or dependency pin changes in this increment. All three
  incoming Scala files and historical review records match master exactly;
  historical review claims were not used as current validation evidence.

## Files inspected

Paths are relative to the worktree above. Deleted files were read from the
HEAD diff; the obsolete runner was also compared with the common ancestor.
Shared content was read directly and checked by staged blob identity in the
sibling worktree. Port-specific pipeline, test-guard, and runtime content was
inspected separately.

```text
cognitive\src\test\scala\com\microsoft\azure\synapse\ml\services\geospatial\AzureMapsSuite.scala
cognitive\src\test\scala\com\microsoft\azure\synapse\ml\services\geospatial\GeospatialCoreSuite.scala
core\src\test\scala\com\microsoft\azure\synapse\ml\featurize\VerifyValueIndexer.scala
pipeline.yaml
reviews\pr-2735\task-test-retirement-attempt-1-review-gpt-6-astra.md
reviews\pr-2736\task-ci-test-selection-attempt-1-review-1-gpt-6-astra.md
reviews\pr-2736\task-ci-test-selection-attempt-1-review-robustness-claude-opus-5.md
tools\ci\README.md
tools\ci\databricks_impact.py
tools\ci\e2e_impact.py
tools\ci\tests\test_databricks_impact.py
tools\ci\tests\test_e2e_impact.py
tools\ci\tests\test_pipeline_yaml.py
tools\pytest\run_all_tests.py
```

Nearby source and policy context, including targeted search excerpts:

```text
AGENTS.md
.github\skills\synapseml-branches\references\branch-spark4-common.md
.github\skills\synapseml-branches\references\branch-spark4p0.md
build.sbt
environment.yml
codecov.yaml
docs\Reference\Developer Setup.md
project\CodegenPlugin.scala
website\doctest.py
cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\geospatial\CheckPointInPolygon.scala
cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\geospatial\AzureMapsTraits.scala
core\src\main\scala\com\microsoft\azure\synapse\ml\codegen\CodegenConfig.scala
core\src\main\scala\com\microsoft\azure\synapse\ml\codegen\RCodegen.scala
core\src\test\scala\com\microsoft\azure\synapse\ml\codegen\TestGen.scala
core\src\test\scala\com\microsoft\azure\synapse\ml\codegen\PyTestGen.scala
core\src\test\scala\com\microsoft\azure\synapse\ml\codegen\RTestGen.scala
core\src\test\scala\com\microsoft\azure\synapse\ml\codegen\RCodegenSuite.scala
core\src\test\scala\com\microsoft\azure\synapse\ml\core\test\base\TestBase.scala
core\src\test\scala\com\microsoft\azure\synapse\ml\nbtest\DatabricksUtilities.scala
core\src\test\scala\com\microsoft\azure\synapse\ml\nbtest\SharedNotebookE2ETestUtilities.scala
```

`CONTRIBUTING.md` was checked for unchanged index content and sibling blob
equivalence, not reread. Reference searches additionally covered `templates`,
`.github`, `.pipelines`, `tools`, `project`, and `core\src`.

## Independent checks and limits

Read-only pytest selection passed **46 tests, zero skipped**, in 1.67 seconds.
Used `python -B -m pytest -p no:cacheprovider -o addopts='' -q` with bytecode
writes and plugin autoload disabled, selecting these existing tests:

```text
tools\ci\tests\test_e2e_impact.py::test_pipeline_gates_only_audited_families_and_keeps_full_schedule
tools\ci\tests\test_e2e_impact.py::test_selection_preserves_all_expected_coverage_uploads
tools\ci\tests\test_e2e_impact.py::test_empty_and_mixed_changes_cannot_hide_impact
tools\ci\tests\test_e2e_impact.py::test_unknown_shared_and_runtime_inputs_keep_every_family
tools\ci\tests\test_e2e_impact.py::test_ambiguous_paths_keep_every_family
tools\ci\tests\test_pipeline_yaml.py::test_prewarm_job_present
```

The structural comparisons and `git diff --cached --check` also passed.
No live service, resource operation, Azure scheduling verification, full CI,
standalone vulnerability audit, or other review round was performed here.

## Closure update, 2026-09-22

The requester now reports completed local validation on this staged candidate:
aggregate compile and Test/compile, main/test Scala style, 62 core tests,
6 GeospatialCoreSuite tests, all 291 CI-helper tests, and Black passed.
Only the pre-existing ignored ValueIndexer null-case remains. These broader
runs were supplied by the requester, not rerun by this review.

Read session evidence `files\latest-master-content-proof.json` and independently
matched its staged tree and all 18 non-review path records to Git. Fourteen
path states match master exactly, including deletions; the four differences
are the documented pipeline, Developer Setup, existing pipeline guards, and
new disabled-Fabric assertion. Production/runtime, dependency, workflow, and
template files remain unchanged from HEAD. HEAD, MERGE_HEAD, and both recorded
SHA-256 fingerprints were rechecked unchanged.

Round 1 is closed **CLEAN**, with no actionable finding. This is a bounded
incremental review conclusion, not a remote-CI or later-round verdict.

No actionable finding. This artifact is the only intended write in this
worktree; no source edits, staging, commits, or pushes were made. The dirty root
checkout was not modified.
