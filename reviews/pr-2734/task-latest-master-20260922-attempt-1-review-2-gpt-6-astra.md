# Round 2 architecture and patterns review

## Review summary

| Field | Result |
| --- | --- |
| Target | microsoft/SynapseML#2734, base `spark4.1` |
| Worktree | `C:\Users\singhrana\Documents\SynapseML\.worktrees\sync-spark41-20260921` |
| Reviewed HEAD | `d6691ae29c20a87139e37ec6d5966f23b3b760dd` |
| Merge source / MERGE_HEAD | `0a7fdafaa33ff4785dadc8d7eebee68efde110fb` |
| Reviewed staged tree | `7ba0eb1de595bc97ceddccf300362ec574822d42` |
| Round / attempt | 2 / 1 |
| Mode / actual model | Sequential, explicitly authorized GPT fallback / `gpt-6-astra` |
| Scope | Architecture, dependency boundaries, repository patterns, and port/master parity in the same 14-path incremental staged merge |
| Issues found / verdict | **0 / CLEAN** |
| Artifact | `reviews\pr-2734\task-latest-master-20260922-attempt-1-review-2-gpt-6-astra.md` |

The requester reports that the intended `gemini-3.8-flash/high` invocation
failed before execution with backend `400 invalid request body`. Gemini did
not perform this review. This GPT fallback does not establish Gemini coverage
or full three-family success.

## Snapshot and evidence basis

Rechecked HEAD, MERGE_HEAD, and both fingerprints against the accepted round-1
snapshot. All are unchanged; there are no unmerged entries or unstaged tracked
changes.

SHA-256 of raw `git ls-files --stage -z` output:

`cb2725a8586b1ca20d243f91e50f777125d2b03c1aaf858068001ff9e9854447`

SHA-256 of raw `git --no-pager diff --cached --binary --full-index --no-ext-diff --no-textconv HEAD --` output:

`77f96c8c379fd1c189fabe4ff42b5e05f1454a746d3ba6e9f44ae07d6ee299d9`

Used retained source context and the exact file inventory in this directory's
`task-latest-master-20260922-attempt-1-review-1-gpt-6-astra.md`. Its verified
master-content comparisons remain applicable to this unchanged snapshot.
No broad discovery or test rerun was performed for round 2.

## Architecture evidence checklist

- [x] Master parity is preserved without another port-specific implementation.
  Eleven of 14 staged paths match master, including deletions. The retained
  18-path non-review proof has 14 identical path states and four explained
  exceptions: `pipeline.yaml`, `docs\Reference\Developer Setup.md`,
  `tools\ci\tests\test_pipeline_yaml.py`, and
  `tools\ci\tests\test_e2e_impact.py`. Existing runtime/documentation guards
  remain; the new assertion difference expresses the required Fabric disable.
- [x] Runtime boundaries remain explicit. `build.sbt:33-36` retains Spark
  4.1.1 and Scala 2.13.17; `environment.yml` retains Python 3.13 and its existing
  dependency policy. The normal merge does not introduce dependency pins or
  copy Spark 4.0/Python 3.12 NumPy or wheel choices into this port. Existing
  Spark runtime dependency exclusions and JDK 17 CI guards remain intact.
- [x] `tools\ci\e2e_impact.py:39-173` separates pure path classification,
  change collection through Git, environment-based selection, and Azure output
  emission. The runtime helper uses only the standard library. Replacing the
  old shell detector and `databricks_impact.py` leaves one notebook-E2E path
  classifier, with orchestration in YAML rather than duplicated path rules.
- [x] `pipeline.yaml:112-157` keeps CI-helper tests independent of the SBT
  prewarm dependency. `pipeline.yaml:231-292` changes only the Databricks
  selector gates and keeps the existing job structure. Fabric remains
  `condition: false`; streaming scheduling, GPU runtime/capacity policy,
  coverage-producing jobs, and compatibility jobs retain their prior
  configuration. No new dependency cycle or helper-test gate blocks product
  jobs through prewarm.
- [x] `tools\ci\tests\test_e2e_impact.py:382-392` adapts the shared test at
  the specific branch-policy boundary: it asserts Boolean False for Fabric,
  while preserving Databricks conditions and shared dependency assertions.
  Coverage checks at lines 424-454 remain common. This is narrower than
  skipping the test, coercing every condition, or maintaining a second test
  implementation for the port.
- [x] Test retirement follows the canonical runner and fixture patterns.
  `project\CodegenPlugin.scala:428-448` already runs generated Python tests
  through pytest, so deleting `tools\pytest\run_all_tests.py` removes an
  obsolete hardcoded-output-path runner rather than a distinct test layer.
  `VerifyValueIndexer.scala:42-45,76-90` simplifies existing fuzzing suites
  without adding an alternate metadata path.
- [x] `GeospatialCoreSuite.scala:168-202` keeps retired-stage persistence
  coverage in the existing offline suite and uses the `TestBase` managed
  temporary directory. It exercises the existing public stage and reader,
  rather than moving behavior into Python or creating service-backed fixtures.
  Public production classes, serialization contracts, generated files, and
  DataFrame-based implementation conventions are unchanged.
- [x] The increment respects the worktree's `AGENTS.md` and branch guidance:
  portable changes arrive through a normal master merge; branch-specific
  runtime guards remain; shared contributor guidance, production dependencies,
  workflows, and templates are not rewritten. Review records use the numbered
  PR directory and the model that actually executed the round.

## Conclusion and limits

No actionable architecture or repository-pattern finding in this increment.
The accepted round-1 artifact retains the WSL Git-fixture failure/rerun caveat
and unchanged compatibility-job evidence; neither is treated as a new
architecture change. Accepted local validation was not rerun for this round.
The only intended write is this unstaged review artifact. No source edit,
staging, commit, push, root-checkout modification, remote-CI operation, or later
review theme was performed.
