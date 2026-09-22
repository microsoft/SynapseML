# Robustness, architecture and hardening review

## Review summary

| Field | Result |
| --- | --- |
| Round / attempt | Independent robustness pass / attempt 1 |
| Theme | Edge cases, robustness, architecture, detailed correctness, coverage, final hardening |
| Mode / model | Single model, `claude-opus-5` |
| Scope | Staged diff on `ci/conservative-pr-tests-20260921` over `714d365e71`, plus the real consumers of every allowlisted path |
| Issues found | 1 high, 3 medium, 4 low |
| Verdict | CHANGES REQUESTED (one blocking issue outside the changed files) |

The selector itself is the most conservative part of this change and I could not
break it. The blocking problem is in a file the change does not touch:
`codecov.yaml` hard-codes the number of coverage uploads it expects, and that
number is only reachable when no test family is skipped.

> This is a single-model review. The multi-model gauntlet did not run: all four
> advertised Gemini models (3.8 / 3.7 / 3.6 / 3.5) returned HTTP 400. Nothing
> here should be recorded as "the gauntlet passed".

## Findings

### H1 (High) — Selection makes Codecov's `after_n_builds: 54` unreachable

`codecov.yaml:6-8` and `codecov.yaml:17` withhold notifications and the PR
comment until 54 coverage uploads arrive, and the file spells out the
arithmetic:

```yaml
# 54 = UnitTests 40 + PythonTests 7 + RTests 6 + WebsiteSamplesTests 1
# (every leg of those jobs runs templates/codecov.yml on succeededOrFailed).
after_n_builds: 54
```

I confirmed the inputs independently. `templates/codecov.yml` is referenced from
exactly four places — `pipeline.yaml:692` (PythonTests), `:764` (RTests),
`:807` (WebsiteSamplesTests), `:1013` (UnitTests) — and parsing the matrices
gives 7 + 6 + 1 + 40 = 54. Databricks and Fabric upload nothing.

Those four jobs are precisely the ones this change puts behind
`ne(dependencies.BuildAndCacheSbt.outputs['detectTestImpact.*'], 'false')`.
Every interesting selection outcome therefore falls short of 54:

| PR content | `select_suites` | Uploads | Reaches 54? |
| --- | --- | --- | --- |
| `README.md`, `reviews/*.md`, `.github/skills/*.md` only | `frozenset()` | 0 | no |
| `*/src/test/python/**` only | `{python}` | 7 | no |
| `*/src/test/R/**`, `tools/tests/run_r_tests.R` only | `{r}` | 6 | no |
| `website/**`, `docs/Quick Examples/*.md` only | `{website}` | 1 | no |
| anything unrecognized | all seven | 54 | yes |

Consequence: on exactly the PRs this feature is built for, Codecov never reaches
its notification threshold, so the PR comment and the `codecov/project/scala`
and `codecov/project/python` statuses (`codecov.yaml:24-33`) are withheld rather
than reported. If either status is required by branch policy, such a PR cannot
complete; if it is not, the coverage signal silently disappears. Either way this
is a CI regression introduced by the change, and it is not mentioned in the
rewritten `tools/ci/README.md` or in the round-1 artifact.

Note that carryforward cannot rescue this. `codecov.yaml:38-47` declares
`scala`/`python` flags with `carryforward: true`, but `templates/codecov.yml`
runs `upload-process --dir .` with no `--flags`, so uploads are unflagged. (That
mismatch is pre-existing and out of scope, but it removes the obvious mitigation.)

Options, in my order of preference:

1. Gate only the three families that upload no coverage — `databricks_cpu`,
   `databricks_gpu`, `fabric` — and leave `unit`, `python`, `r` and `website`
   always-on. This keeps the largest real savings (Databricks CPU is five legs
   plus a 300-minute timeout) and removes the whole class of problem.
2. Keep the current scope but make `after_n_builds` consistent with selection.
   It is static YAML, so in practice that means removing it and accepting the
   premature-comment behaviour it was added to prevent.
3. Keep the current scope, add `--flags` to `templates/codecov.yml` so the
   declared carryforward flags actually engage, and re-derive the threshold.

Whichever is chosen, `tools/ci/README.md` should state what happens to coverage
reporting on a selected PR, because the current text does not.

### M2 (Medium) — The new always-on pip install and 287-test pytest run gate the entire pipeline

`pipeline.yaml:120-124` adds to `BuildAndCacheSbt`:

```bash
set -euo pipefail
python3 -m pip install --disable-pip-version-check pytest pyyaml
python3 -m pytest tools/ci/tests/ -q
```

Every job in the pipeline declares `dependsOn: BuildAndCacheSbt` and
`succeeded()`. A transient PyPI failure, or one flaky helper test, now fails the
prewarm job and skips all seven test families plus `Style`, `BuildDocker`,
`PublishArtifacts`, `ReleaseBranchCompat` and `InternalCompat`. This runs on
scheduled, master and tag builds too, so it is also a release-path dependency.

The code being replaced had no install step and ran under `set -uo pipefail`
with explicit fail-open branches, so this is a genuinely wider blast radius. It
is also inconsistent with the repo's own conventions elsewhere in the same file:
`templates/sbt_cache.yml` is invoked with `maxAttempts: 7`, and
`templates/codecov.yml` pins a version, verifies a SHA-256 and uses
`curl --retry 3`. Here `pytest` and `pyyaml` are unpinned with no retry.

I did verify the dependency set is sufficient — every module under
`tools/ci/tests/` imports only the standard library, `pytest` and `yaml` — so
this is an availability and blast-radius concern, not a missing-dependency bug.

Suggested hardening: pin both packages, add a retry, and move the helper suite
into its own job (next to `Style`) so a helper regression fails that job on its
own instead of suppressing every test family through `succeeded()`.

### M3 (Medium) — Fail-open diagnostics discard git's stderr, so "selection never engages" is invisible

`test_impact.py:77-84` runs git with `check=True` and `stderr=subprocess.PIPE`.
The handler at `:146-152` logs `type(error).__name__` and `json.dumps(str(error))`.
`CalledProcessError.__str__` renders only `Command '...' returned non-zero exit
status N`, so the captured stderr is dropped.

Because every failure degrades silently to "run everything", a persistent
misconfiguration — for example `SYSTEM_PULLREQUEST_SOURCECOMMITID` not matching
the merge's second parent in some PR configuration — would make selection never
activate, and the warning would not say why. The change would look harmless and
deliver nothing. Include `error.stderr` (decoded and `json.dumps`-escaped, to
preserve the injection safety noted in L7) and consider a distinct marker so a
"selection never engaged" pattern is greppable across builds.

### M4 (Medium) — `/azp run` cannot reach the `fullTests` escape hatch

`pipeline.yaml:54-57` declares `fullTests` as a queue-time parameter, and
`select_suites` (`test_impact.py:139`) only runs everything when
`SYNAPSEML_FULL_TESTS` is anything other than `false`. A `/azp run` comment
keeps `Build.Reason=PullRequest` and the parameter default `false`, so
re-running validation from the PR produces the same selected subset again. The
repository's `AGENTS.md` tells contributors and agents to "Trigger Azure
validation with `/azp run`", so the documented workflow cannot reach the
documented override; it requires a manual queue from the ADO UI.

The README says "Set the queue parameter `fullTests=true`" but does not say that
`/azp run` cannot set it. Document the exact escape hatch, and consider a second
trigger that works from the PR (a label, or a pipeline variable honoured when the
parameter is default).

### L5 (Low) — `tools/ci/test_impact.py` matches pytest's default test-discovery glob

The production helper is named `test_*.py`. I confirmed that
`python -m pytest tools/ci -q --collect-only` imports it during collection (287
tests collected, no error). It is benign today because the pipeline scopes to
`tools/ci/tests/`, and the file's entry point is guarded by
`if __name__ == "__main__"`. But it means the module is importable under two
names simultaneously — `test_impact` via pytest's basedir insertion and
`tools.ci.test_impact` via `test_test_impact.py:12` — and any future broadening
of the pytest path collects a non-test file. The replaced `databricks_impact.py`
did not have this property. A name such as `select_tests.py` or
`impact_selection.py` removes the footgun.

### L6 (Low) — Helper tests depend on `python -m pytest`, undocumented

There is no root `conftest.py` and no `tools/__init__.py`, so
`from tools.ci.test_impact import ...` resolves only because `-m` puts the
working directory on `sys.path`. `pytest tools/ci/tests/` (without `-m`) fails
to import. Both `pipeline.yaml:123` and the README use the correct form, so this
is right as written, and the pattern is pre-existing
(`test_patch_internal_typing_support.py:6`) rather than a regression — but it is
load-bearing and worth one line in `tools/ci/README.md`.

### L7 (Low) — Per-path logging is O(changed files) and re-derives classification

`test_impact.py:153-156` loops over every changed path and recomputes
`suites_for_path`, which `required_suites` already computed, emitting one stderr
line per file. Log volume only.

Recorded as a positive: because the line is built as
`f"Changed path {json.dumps(path)} requires: ..."`, a filename such as
`##vso[task.setvariable variable=runUnitTests;isOutput=true]false` can never
start a line, and `json.dumps` escapes embedded newlines. ADO only parses
logging commands from line starts, so filename-based logging-command injection
is prevented. The same protection is correctly applied to the warning at `:148-149`.

### L8 (Low) — `MODULES` is a hand-maintained second copy of the module list

`test_impact.py:25` duplicates the module list owned by `build.sbt`. Drift is
fail-safe in both directions (a new module is unrecognized and enables
everything; a stale entry only allowlists a directory that does not exist), and
`test_test_impact.py` covers `new-module/src/test/python/...` returning all
families. Noted only so a future reader does not assume the list is validated.
Related: `opencv/src/test/python/**` is allowlisted to `{python}` although that
directory does not exist and only `core` has a `src/test/R`; harmless, since the
selector enables the whole family rather than a leg.

## What I verified as correct

These are the areas the request called out. I checked each against real
consumers rather than against the README's claims, and found no defect.

- **Immutable merge-parent comparison.** `test_impact.py:87-129` requires
  `refs/pull/<n>/merge`, pins `HEAD` to `BUILD_SOURCEVERSION`, requires exactly
  two parents, requires `parents[2] == SYSTEM_PULLREQUEST_SOURCECOMMITID`, and
  diffs `parents[1]..HEAD`. It never fetches or consults a branch tip, so a
  target that advances after queueing cannot erase a queued runtime change —
  demonstrated by `test_target_advancement_cannot_erase_the_queued_runtime_change`,
  which fast-forwards `master` onto the source and still gets `[RUNTIME_PATH]`.
- **Rename / symlink / type / malformed metadata.** `--no-renames`
  (`:111`) expands moves into delete+add pairs, so a move out of a runtime
  directory cannot present as docs-only. The raw parser enforces a 5-field
  header, a leading `:`, both modes in `{000000, 100644, 100755}` and a status
  in `{A, D, M}`, plus NUL framing (`fields.pop() != b"" or len(fields) % 2`).
  Symlinks (`120000`), gitlinks (`160000`) and typechanges (`T`) all fail open.
  Exception coverage is complete by inheritance: `UnicodeDecodeError` is a
  `ValueError`, and `TimeoutExpired` and `CalledProcessError` are both
  `SubprocessError`, so decode failures on non-UTF-8 paths and the 60-second
  git timeout are caught.
- **Missing pipeline outputs.** All seven conditions use `ne(..., 'false')`, so
  an absent output variable means run, not skip — the inverse of the `eq(...,
  'true')` form it replaces. Verified at `pipeline.yaml:227, 260, 283, 616, 703,
  775, 823`, and asserted by `test_test_impact.py`.
- **Boolean parameter casing.** Azure renders `${{ parameters.fullTests }}` as
  `True`/`False`; `:139` lowercases before comparing, so both spellings work and
  anything that is not `false` runs everything.
- **Codegen consumer dependencies.** `CodegenConfig.pyTestOverrideDir` and
  `rTestOverrideDir` (`CodegenConfig.scala:40,49`) are read only by
  `TestGen.scala:34-35`, `PyTestGen.scala:61-62`, `RTestGen.scala:143-144` and
  `RCodegen.scala:101-102`. All are reached through `CodegenPlugin`'s
  `testgen`/`pyTestgen`/`rTestGen` tasks feeding `testPython`/`testR`; they are
  `object`s invoked via `Test/runMain`, not ScalaTest suites, so `sbt test` does
  not run them. The Scala codegen suites that do run under `sbt test`
  (`VerifyCodegenConfig`, `VerifyRCodegen`, `PyCodegenSuite`) use synthetic
  `topDir` values, not the real module trees. The `src/test/python → {python}`
  and `src/test/R → {r}` claims therefore hold.
- **Notebook E2E cannot be hidden by the Markdown allowlist.**
  `DatabricksUtilities.scala:250-252` and
  `SharedNotebookE2ETestUtilities.scala:94-96` both select `.ipynb` under
  `docs/`, so allowlisting `docs/Quick Examples/*.md` cannot skip a notebook. I
  confirmed by probe that `docs/Quick Examples/example.ipynb` and
  `docs/Quick Examples/data.json` fall through to all seven families.
- **The ONNX documentation coupling the README cites is real and correctly handled.**
  `ONNXRuntimeDependencySuite.scala:31` reads
  `docs/Explore Algorithms/Deep Learning/ONNX.md`, which is not allowlisted; the
  probe returns all seven families. `website/docs/**` is generated by
  `convertNotebooks` and untracked, so it cannot appear in a diff.
- **`website/doctest.py` and Quick Examples have an always-on guard.**
  `tools/ci/tests/test_website_doctest.py` loads `website/doctest.py` and reads
  `docs/Quick Examples/transformers/cognitive/*.md`, and runs on every build via
  the new prewarm step, so the `{website}` mapping is backed by a test that
  selection cannot skip.
- **No Scala CI-helper regression from the new multi-line conditions.**
  `PipelineTestCoverageSuite` parses the `UnitTests` job out of `pipeline.yaml`
  and is the one guard of this kind that the pytest suite does not cover. I
  reproduced its `matrixSpecs` regexes in Python against `HEAD:pipeline.yaml`
  and the staged file: 67 specs, identical sets, non-empty, even though the new
  `condition: >-` block now falls inside `matrixBlock`. The leg splitter
  `^      \w+:` does not match `      succeeded(),`, `      eq(...)` or
  `      ne(...)`, and none of the condition text contains
  `com.microsoft.azure.synapse.ml` or `PACKAGE:`.
- **The change validates itself under full coverage.**
  `required_suites(<staged paths>)` returns all seven families, because
  `pipeline.yaml` and `tools/ci/**` are not allowlisted.
- **Ungated jobs stay ungated.** `Style`, `BuildDocker`, `PublishArtifacts`,
  `ReleaseBranchCompat` and `InternalCompat` keep their existing gates, and the
  daily schedule is unchanged. `ReleaseBranchCompat` retains its own independent
  path classifier at `pipeline.yaml:1071`; note it is a second, broader
  allowlist (it exempts all of `docs/*` and `tools/ci/*`), so the repository now
  has two path-classification mechanisms that can drift.

## Independent checks I ran

- `python -m pytest tools/ci/tests/test_test_impact.py -q` — **120 passed** in
  44.87s, reproduced first-hand rather than taken from the request.
- `python -m pytest tools/ci -q --collect-only` — 287 tests collected, no import
  or collection errors; also establishes L5.
- Probed `suites_for_path` with 32 adversarial inputs beyond the committed
  tests, including case variants (`Website/x.md`, `_X.MD`, `core/src/test/r/`),
  prefix near-misses (`websiteX/`, `website`), dotfile names (`website/.md`),
  nested governance paths, and `website/docs/.../ONNX.md`. Every case resolved
  conservatively; no hole found.
- Reproduced `PipelineTestCoverageSuite.matrixSpecs` in Python and diffed the
  67 parsed specs between `HEAD` and the staged `pipeline.yaml`.
- Parsed `pipeline.yaml` to count matrix legs per gated job (UnitTests 40,
  PythonTests 7, RTests 6, WebsiteSamplesTests 1, DatabricksCPUE2E 5,
  DatabricksGPUE2E 1, FabricE2E 1) and cross-referenced the four
  `templates/codecov.yml` call sites — this is the evidence for H1.
- Enumerated imports across `tools/ci/tests/**` to confirm `pytest` and `pyyaml`
  are the only third-party requirements of the new prewarm step.

## Limitations

- **Single model.** The multi-model gauntlet is blocked — Gemini 3.8, 3.7, 3.6
  and 3.5 all fail with HTTP 400. This artifact is one model's opinion and must
  not be recorded as gauntlet coverage.
- **Nothing was run on Azure.** Template expansion of
  `${{ parameters.fullTests }}`, `isOutput` wiring, `fetchDepth: 2` against a
  real `refs/pull/<n>/merge`, and actual job skipping are all unverified on the
  service. H1 in particular is derived from `codecov.yaml` semantics and the
  upload arithmetic, not from an observed Codecov run; the exact behaviour when
  `after_n_builds` is never reached should be confirmed against Codecov's
  current documentation before choosing a fix.
- **No sbt was run.** No Scala compilation or test executed.
  `PipelineTestCoverageSuite` was verified by faithful reproduction of its
  regexes, not by execution.
- **I did not re-run the full 287-test helper suite**, only collected it and ran
  the 120 selector tests.
- **Not verifiable from the repository:** whether the Codecov statuses are
  required by ADO branch policy, fork-PR behaviour of the merge-ref checks, and
  whether `SYSTEM_PULLREQUEST_SOURCECOMMITID` always equals the merge's second
  parent in this organisation's configuration.
- No implementation, commit or push was made, and no sub-agents were used.

## Resolution notes

The implementation was narrowed after this review. The original findings above
are preserved as review history, not a description of the final patch.

- H1: Removed selection conditions from UnitTests, PythonTests, RTests, and
  WebsiteSamplesTests. Only Databricks CPU/GPU and Fabric E2E are optional.
  `test_selection_preserves_all_expected_coverage_uploads` discovers the actual
  Codecov producers, counts their matrix legs, compares both configured upload
  thresholds, and asserts none is gated by the detector. No coverage thresholds,
  flags, or required checks were weakened.
- M2: Moved helper tests out of the prewarm gate into an independent CIHelpers
  job. Installation has bounded pip retries and two task retries; assertions
  are not retried. It uses the same pytest/PyYAML dependency policy as
  environment.yml rather than adding unrelated pins. Helper failures still fail
  CI, but no longer suppress product jobs or add a serial prewarm delay.
- M3: Git errors now include their captured stderr, JSON-escaped in the warning.
  The real non-repository regression asserts that the underlying Git diagnostic
  is visible as well as the fail-open warning.
- M4: The README now states that `/azp run` cannot set queue parameters and
  documents an unfiltered manual run against the PR merge ref.
- L5: Renamed the production helper to `e2e_impact.py` and its regression file to
  `test_e2e_impact.py`, eliminating production-module test discovery.
- L6: Documented `python -m pytest` from the repository root.
- L7: Retained per-path, escaped decisions intentionally so reviewers can audit
  every reason for an E2E skip. Linear processing is necessary to classify the
  diff, and the classifier does no network or filesystem work per path.
- L8: Kept an explicit audited module allowlist. Dynamically admitting a new
  module merely because it exists would weaken the default-to-full policy.
  New-module paths remain covered by the unknown-path regression.

The original full helper run passed 287 tests. Validation of these resolutions
is recorded in the final verification artifact and PR description. The
unavailable Gemini family remains a disclosed review limitation.

## Final verification (narrow follow-up pass)

Scope: verify only the resolutions above against the current worktree. Not a new
audit. No implementation, commit, push, or sub-agent. Same model
(`claude-opus-5`); the Gemini family still returns HTTP 400, so this is **not**
gauntlet coverage.

| Finding | Verdict | Evidence |
| --- | --- | --- |
| H1 | **Resolved** | Independent parse of `HEAD:pipeline.yaml` vs. the staged file: the four `templates/codecov.yml` producers are unchanged — UnitTests 40, PythonTests 7, RTests 6, WebsiteSamplesTests 1 = **54 in both**, matching `codecov.yaml` `notify.after_n_builds` and `comment.after_n_builds` (54/54). All four conditions are byte-identical to `HEAD` and contain no `detectTestImpact`. Exactly three jobs are gated — `DatabricksCPUE2E`, `DatabricksGPUE2E`, `FabricE2E` — none of which uploads coverage. `OUTPUTS` (`e2e_impact.py:15-19`) holds only those three families. |
| M2 | **Resolved** | `CIHelpers` (`pipeline.yaml:112-128`) is the only added job: no `dependsOn`, no `condition`, so it neither gates nor is gated by the seven families. Install uses `--retries 5 --timeout 30` plus `retryCountOnTaskFailure: 2`; the pytest step has neither, so assertions are never retried. `environment.yml` is untouched and still lists `pytest`/`pyyaml` unpinned — no dependency pin was added, changed, or removed anywhere in the diff. |
| M3 | **Resolved by repro, not by reading** | Ran `e2e_impact.py` in a non-repository directory with a well-formed PR env. stderr: `##vso[task.logissue type=warning]Cannot prove PR test isolation; running all tests. CalledProcessError: "Command '[...rev-parse, HEAD]' returned non-zero exit status 128.; stderr: fatal: not a git repository (or any of the parent directories): .git\n"`. Git's own diagnostic is now present; `json.dumps` escaped the embedded newline and quotes, so L7's line-start injection protection still holds. Exit code 0 with all three outputs `true` — fail-open intact. |
| M4 | **Resolved (documentation)** | `tools/ci/README.md` states that `/azp run` uses default parameters and cannot set the override, and directs to **Run pipeline** against `refs/pull/<number>/merge`. Azure-side behaviour remains unverified by construction. |
| L5 | **Resolved** | Production helper is `tools/ci/e2e_impact.py`; regression file is `tests/test_e2e_impact.py`. `python -m pytest tools/ci -q --collect-only` collects **288 tests, no errors**, and no `e2e_impact.py::` item — the production module is no longer discovered. No live reference to `databricks_impact`/`test_impact.py` survives outside `reviews/` (correctly retained as review history). |
| L6 | **Resolved** | Import is `from tools.ci.e2e_impact import ...`; the README documents `python -m pytest` from the repository root. |
| L7 / L8 | **Accepted as reasoned** | Per-path escaped logging and the audited `MODULES` allowlist are deliberate; both keep the default-to-full policy. No change required. |

Targeted runs on this worktree (Windows, repository root):

- `python -m pytest tools/ci/tests/test_e2e_impact.py -q` — **121 passed, 0
  skipped**, 50.90s.
- `python -m pytest tools/ci/tests/test_e2e_impact.py tools/ci/tests/test_pipeline_yaml.py -q`
  — **171 passed, 36 skipped**, 51.07s. Every skip is a pre-existing Bash-only
  pipeline/replay/Fabric test on Windows; none is in the selector suite.
- `python -m pytest tools/ci -q --collect-only` — 288 collected, no errors.

The full 288-test helper suite was not executed here; the collection check and
the two targeted suites are what this pass claims.

**Verdict: the blocking H1 and the three medium findings are resolved. No
remaining bug found.** One non-blocking observation, recorded so it is not
rediscovered as a defect: `test_selection_preserves_all_expected_coverage_uploads`
calls `item.get(...)` on list elements without an `isinstance` guard, so a future
step whose list value holds scalars would raise `AttributeError` instead of
failing cleanly. I probed every step in the current `pipeline.yaml` and found no
such shape (0 occurrences), so the test is correct today.

Limitations unchanged: single model with the Gemini family unavailable (HTTP
400), nothing executed on Azure — `${{ parameters.fullTests }}` expansion,
`isOutput` wiring, `fetchDepth: 2` against a real `refs/pull/<n>/merge`, actual
job skipping, and Codecov's live threshold behaviour remain unobserved — and no
sbt or Scala test was run.
