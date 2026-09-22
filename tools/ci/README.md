# CI bootstrap helpers

## `sbt_retry.sh` — resilient sbt bootstrap

SynapseML's Azure Pipelines (`pipeline.yaml`) fans out ~30 hosted-agent matrix
jobs. Each one cold-bootstraps the sbt launcher (`org.scala-sbt:sbt:<version>`,
pinned in `project/build.properties`) and resolves Ivy dependencies from public
Maven Central. When many fresh agents — and several overlapping PR builds — do
this simultaneously, Maven Central returns **HTTP 429 (rate limit)** and the
`Setup repo` step fails before any test runs (e.g. ADO build 229124511).

The durable fix has four layers:

1. **`templates/sbt_cache.yml`** (primary) — Azure `Cache@2` for the sbt launcher
   boot directory (`~/.sbt/boot`), Ivy cache (`~/.ivy2/cache`), and Coursier
   cache (`~/.cache/coursier`). In steady state, jobs restore these from Azure's
   cache service and never touch Maven Central. Keys are derived from the
   bootstrap inputs (`project/build.properties`, `project/plugins.sbt`,
   `build.sbt`, and `project` Scala sources). `continueOnError` keeps a cache
   service outage non-fatal.
2. **`BuildAndCacheSbt` prewarm job** — warms those caches once per pipeline run
   (mirrors the existing `BuildAndCacheCondaEnv` job). Every sbt-running job
   depends on this gate, so a new cache key is populated before the fan-out
   starts instead of racing it. A failed prewarm remains visible and prevents a
   cold-cache stampede.
3. **Canonical Maven Central fallback** — `build.sbt` keeps
   `https://repo.maven.apache.org/maven2` after sbt's default
   `https://repo1.maven.org/maven2` resolver. Ivy therefore continues to the
   same official repository through its canonical hostname when `repo1` returns
   HTTP 429. A synthetic Ivy test with a resolver that returned only HTTP 429
   confirmed that the fallback downloaded every dependency successfully.
4. **`sbt_retry.sh`** (supplement) — covers cold-cache starts and resolution
   failures caused by unusable restored entries. It adds a bounded random start
   stagger so concurrent cold jobs don't hit Maven at the same instant, then
   bounded jittered exponential-backoff retries and the targeted recovery below.
   On exhaustion it fails visibly (non-zero exit); it never masks a failure with
   a success fallback. Exact hits on all three caches disable the start stagger
   automatically.

### Unusable restored caches

A restored cache can be **unusable on a single agent**: the module directory
under `~/.ivy2/cache` exists, but Ivy still reports the dependency as unresolved.
Retrying the identical command preserves that local state and can fail
identically.

Observed in ADO build 231667649: `UnitTests language` failed ten consecutive
times in 12–17s each on `com.globalmentor#hadoop-bare-naked-local-fs`, while the
other **39 of 40** shards in that same build resolved that module offline from
the byte-identical cache key. This isolates the exposure to one agent's restored
state, but does not by itself distinguish a later HTTP 404/429, TLS, or DNS
failure; the diagnostic probe exists to make that distinction.

`sbt_retry.sh` therefore parses `unresolved dependency: <org>#<name>;<rev>` out
of each failed attempt, including when Ivy wraps the coordinate onto the next
log line, and deletes exactly those modules from `~/.ivy2/cache`, `~/.ivy2/local`,
and the Coursier entries for both Maven Central hostnames before backing off, so
the next attempt re-fetches them cleanly. Unrelated failures evict nothing.
Override
`SBT_SETUP_IVY_HOME` / `SBT_SETUP_COURSIER_CACHE` with absolute, non-root paths
to relocate the scan. Eviction is disabled if `HOME` is unavailable and no safe
override is provided. Cache entries with a symlinked ancestor are skipped rather
than recursively followed outside the configured root. Only one representative
unresolved coordinate is probed per wrapper invocation; both Maven Central
endpoints share a 30-second total timeout, so diagnostics cannot grow with the
number of missing modules or retry attempts. Terminal failures are still probed,
but cache entries are removed only when another wrapper retry will follow.

### Tests

```bash
python -m pytest tools/ci/tests/ -v
```

`test_sbt_retry.py` drives the wrapper with a fake `sbt` (deterministic, no real
sleeps) to verify retry/backoff/stagger, visible-failure behaviour, and that a
resolution failure evicts exactly the named modules before retrying — including
a control asserting that the same state exhausts every attempt when eviction
cannot reach it.
`test_pipeline_yaml.py` verifies `pipeline.yaml` parses and that every
sbt-running job is wired to the shared cache template + prewarm job.

## Release compatibility replay

Replay excludes Markdown review records under `reviews/`, like other CI
documentation. Executable files in that directory and mixed code/documentation
changes still require replay. Conflicting patches remain errors.

The prerequisite list is temporary dependency metadata, not a change history.
Remove integrated backports after verifying the release targets contain them;
reapplying an old patch onto a newer port can create a false conflict.

## External setup and coverage publication

Fabric credential reads and Azure coverage publication each allow two task
retries for transient service failures. Exhausted attempts still fail the job.
Certificate validation, test assertions, and required coverage reports are not
bypassed.

## Conservative PR notebook E2E selection

`e2e_impact.py` can skip the five Databricks CPU jobs and one Databricks GPU job
for the isolated inputs below. Mixed changes take the union; any unrecognized
path keeps all enabled notebook E2E jobs selected.

Fabric E2E remains disabled on this Spark port regardless of the selector output.

| Paths allowed to skip notebook E2E | Why notebook execution is independent |
| --- | --- |
| Module `src/test/python/` | `CodegenConfig.pyTestOverrideDir` and `TestGen` copy these into the generated Python test tree, not the runtime package. |
| Module `src/test/R/`, `tools/tests/run_r_tests.R` | `rTestOverrideDir` and `CodegenPlugin.testRImpl` consume these only as R tests. |
| `website/`, Markdown under `docs/Quick Examples/` | `website/doctest.py` executes the Quick Examples Markdown. These are not runtime sources or `.ipynb` notebook inputs. |
| Explicit root governance files, Markdown under `.github/skills/`, `.agents/`, `reviews/` | These are contributor/agent instructions and review records, not test inputs. The exact list is in `GOVERNANCE_FILES`. |

Unit, Python, R, and website-sample tests remain unfiltered. This preserves all
54 uploads expected by `codecov.yaml`, rather than silently losing coverage
statuses/comments on selectively tested PRs. Generated tests and cross-module
helpers also prevent a simple module-to-matrix mapping. Style, compilation/cache
preparation, Docker builds, publishing, and compatibility keep their existing
gates. More aggressive matrix filtering needs a separate coverage design and
verified dependency model first.

All production changes, Scala test changes, shared fixtures, resources,
notebooks, build/dependency files, pipeline/templates, and CI helpers run all
enabled notebook E2E jobs. There is no blanket Markdown exemption:
`ONNXRuntimeDependencySuite` reads the ONNX documentation, and website samples
execute Markdown. The former Databricks detector's broad test/tooling exemptions
and CPU/GPU module assumptions are removed.

Only `Build.Reason=PullRequest` with a verified two-parent PR merge commit can
skip anything. The checkout includes both parents. Detection compares the exact
queued merge with its first parent, not a freshly fetched target tip that may
have advanced. Renames are expanded into deletion/addition pairs; symlinks,
submodules, missing history, empty/malformed diffs, unknown paths, and Git errors
enable everything. Missing output variables also mean run, not skip. An
unexpected detector crash fails the prerequisite job visibly.

Scheduled, manual, push, and tag builds always retain all enabled test jobs.
Set the queue parameter `fullTests=true` to bypass PR selection as well. A
`/azp run` comment uses the default parameters and cannot set this override.
For an unfiltered rerun, use Azure Pipelines **Run pipeline** against the PR's
`refs/pull/<number>/merge` ref, not its source branch. Manual runs are always full.
Explicit family-disable parameters still apply; "full" does not enable
previously disabled suites.

The independent `CIHelpers` job runs `python3 -m pytest tools/ci/tests/ -q` on
every build. It has no cloud credentials or Spark dependency; dependency
installation retries, but test failures do not. A failure marks CI red without
preventing product tests from running. Use `python -m pytest` from the repository
root so the helper modules are importable.

The selector tests exercise real Git repositories, shallow
checkouts, moving targets, renames, type changes, mixed inputs, manual/scheduled
runs, and output-to-job wiring. This PR changes CI itself, so its own validation
must run every family. A separate representative PR is needed to observe
Azure's selective job scheduling before treating the skip path as proven in CI.

## `get_python_version.sh`

Read exactly one numeric `python=major.minor` or `python=major.minor.patch`
dependency from the supplied environment file, defaulting to `environment.yml`.
Preserve the declared value without inventing a patch release. Missing,
duplicate, range, wildcard, and malformed pins fail explicitly.

The interpreter pin is not a Maven-version parser. Candidate artifact versions
can legitimately contain `-pythonX.Y`; use the version published by the build,
not a blanket suffix-removal rule.

## `patch_internal_typing_support.py`

Internal compatibility calls this adapter after checking out the paired
Internal branch. For a present `utils/typing_build_support.py`, it patches the
known legacy `TYPING_PACKAGE_DATA` constant idempotently and rejects unknown
values or malformed declarations.

Older Internal builds invoke OSS `CodeGen` directly and have no typing helper.
The adapter accepts that layout only when the root build and codegen plugin
exist, the plugin contains active direct-codegen/package task definitions, and
no SBT build or project Scala source references the absent helper.
Comments and multiline documentation strings cannot establish the task layout.
A missing referenced helper, unknown layout, or misspelled path remains an error. This
path creates no helper and does not disable packaging or compatibility tests.
Recognizing the layout alone is not proof that the candidate wheel works.

`test_patch_internal_typing_support.py` covers both layouts. Verify actual
Internal packaging and tests against the exact OSS candidate after adaptation.
