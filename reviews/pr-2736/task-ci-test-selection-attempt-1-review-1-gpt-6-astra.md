# Round 1 review

## Review summary

| Field | Result |
| --- | --- |
| Round / attempt | 1 / 1 |
| Theme | Broad correctness and specification review |
| Mode / model | Sequential / `gpt-6-astra` |
| Scope | Current uncommitted diff, including both new selector files, against `714d365e71` on the branch targeting `master` |
| Issues found | 0 |
| Verdict | CLEAN |

No concrete correctness bug, unsafe skip relationship, failure masking, or
pipeline-condition defect was found in the reviewed changes.

## Evidence checklist

- [x] Read this worktree's `AGENTS.md`, branch guidance, tracked diff, complete
  `tools\ci\test_impact.py`, and complete
  `tools\ci\tests\test_test_impact.py`. Reviewed the replacement of the old
  Databricks classifier and its tests, pipeline wiring, and documentation.
- [x] `tools\ci\test_impact.py:43-74` uses a positive allowlist and unions mixed
  changes. Runtime, Scala tests, resources, notebooks, build/dependency files,
  tools, unknown paths, and ambiguous names retain all seven families.
- [x] Checked the isolation claims against actual consumers.
  `core\src\main\scala\com\microsoft\azure\synapse\ml\codegen\CodegenConfig.scala:36-49`
  separates test overrides from runtime sources.
  `core\src\test\scala\com\microsoft\azure\synapse\ml\codegen\TestGen.scala:34-35`
  copies overrides into test trees; `project\CodegenPlugin.scala:107-121,317-337`
  runs the respective R/Python tests. The pipeline keeps their entire matrices.
- [x] `website\doctest.py:126-143` executes Quick Examples, so their Markdown
  retains website tests.
  `core\src\test\scala\com\microsoft\azure\synapse\ml\nbtest\DatabricksUtilities.scala:250-252`
  and `SharedNotebookE2ETestUtilities.scala:89-106` select notebook inputs by
  `.ipynb`, not the allowlisted Markdown.
- [x] `tools\ci\test_impact.py:87-129` verifies the queued merge ref/SHA, two
  parents, and source SHA; compares against the first parent; disables rename
  detection; and rejects symlink/gitlink modes and malformed diff records.
  `tools\ci\test_impact.py:132-165` retains all families for non-PR builds,
  forced/unknown overrides, and handled detection errors, and emits all seven
  named outputs.
- [x] `pipeline.yaml:119-131` runs helper regressions before detection.
  The conditions at `pipeline.yaml:227,260,283,616,703,775,823` use matching
  output names with `ne(..., 'false')`; missing outputs do not authorize skips.
  Existing dependency-success checks, explicit family switches, and the Fabric
  fork restriction remain. Detector crashes fail the prerequisite visibly.
  The daily schedule at `pipeline.yaml:45-51` remains unchanged.
- [x] Independently ran focused checks using Python 3.14.6, with bytecode and
  pytest cache writes disabled: five union/YAML/prewarm/wiring cases passed;
  thirteen real-Git add/modify/delete/rename/empty-diff, moving-target,
  symlink/gitlink, and CLI-output cases passed. No full suites were rerun.

## Limits and handoff

This is a local round-1 result, not evidence that Azure has exercised selective
job scheduling. The reported 120 WSL selector passes were supplied by the
requester; the running full-helper and pipeline suites were not treated as
completed. Azure template expansion and a representative selective PR remain
unverified here, as the updated CI README already states.

No implementation changes were made. Preserve this artifact and commit it with
the reviewed code after the required gauntlet; this round does not authorize an
early code commit.

## Later scope correction

The subsequent Opus review found the Codecov upload threshold coupling missed
by this pass. The final patch retains all 54 coverage-producing matrix legs and
limits optional jobs to Databricks CPU/GPU and Fabric E2E. The helper tests now
run independently of prewarm, and the detector is named `e2e_impact.py`.
This original review is retained as history; it is not final-head evidence.
