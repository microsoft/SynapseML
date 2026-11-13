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

## Spark port validation

Master PRs no longer replay their patches onto Spark 4.1. The advisory
`ReleaseBranchCompat` job and its prerequisite list have been removed.
Pipeline triggers for pushes and PRs targeting `spark4.1` remain enabled.
Validate port changes and resolved syncs on the actual port branch; a green
master build does not prove port compatibility. The separate
SynapseML-Internal compatibility check retains its existing gates.

## External setup and coverage publication

Fabric credential reads and Azure coverage publication each allow two task
retries for transient service failures. Exhausted attempts still fail the job.
Certificate validation, test assertions, and required coverage reports are not
bypassed.

## Conservative PR validation selection

`e2e_impact.py` can skip the five Databricks CPU jobs, one Databricks GPU job,
and Fabric E2E for the isolated inputs below. Mixed changes take the union;
any unrecognized path enables all seven jobs.

| Paths allowed to skip notebook E2E | Why notebook execution is independent |
| --- | --- |
| Module `src/test/python/` | `CodegenConfig.pyTestOverrideDir` and `TestGen` copy these into the generated Python test tree, not the runtime package. |
| Module `src/test/R/`, `tools/tests/run_r_tests.R` | `rTestOverrideDir` and `CodegenPlugin.testRImpl` consume these only as R tests. |
| `website/`, Markdown under `docs/Quick Examples/` | `website/doctest.py` executes the Quick Examples Markdown. These are not runtime sources or `.ipynb` notebook inputs. |
| Explicit root governance files, Markdown under `.github/skills/`, `.agents/`, `reviews/` | These are contributor/agent instructions and review records, not test inputs. The exact list is in `GOVERNANCE_FILES`. |

The same exact-merge detector selects unit, Python, R, website, Docker and
Internal-compatibility jobs. Selection follows the compile/test dependencies in
`build.sbt`, not only filename extensions:

- Shared core Scala sources, fixtures and codegen, resources,
  build/dependency/configuration files, CI helpers and unknown paths retain every job.
- Other modules' Scala sources select their Scala, Python and R tests.
  OpenCV also selects deep-learning, which depends on it. Scala test fixtures
  retain the generated Python/R tests too.
- Any selected Scala tests also retain the `core` and cross-module `misc` legs,
  including codegen, suite-discovery and serialization checks.
- Hand-written Python selects the module's Python tests and dependent modules.
  Python/R test-only changes select the corresponding language tests; core
  language fixtures conservatively select that language in every module.
- Production changes retain website samples, Docker packaging and Internal
  compatibility. Their existing credential/fork restrictions still apply.
- Isolated governance Markdown does not require runtime jobs. Website and
  Quick Examples Markdown retain website sample execution.

The required Azure aggregate, independent CI-helper tests, Style, cache
preparation and **Publish** remain. Publish still builds/uploads the candidate
packages and uploads `Build.md` to the Extensions tab. The summary adds direct
links to the built JARs and module wheels; either missing artifact family fails
the summary step rather than advertising unavailable downloads.

Coverage uploads use explicit test-family/package flags with carryforward
enabled for omitted, unaffected jobs. Shards sharing a flag are always selected
together (for example, both cognitive speech shards). The full configuration PR
and subsequent full master build seed all flags before selective PRs use them.
`CoverageComplete` waits for every selected coverage producer and explicitly
sends Codecov notifications; there is no fixed 55-upload wait or invented empty
upload for a skipped job. Failed tests/publication remain failures. Inspect
actual carryforward flags when comparing selective reports.

GitHub retains fast Scala compile/style checks, security/dependency checks and
the path-filtered website build. Python formatting is owned by Azure's Style
job instead of duplicated in Actions. The deployed-site link scan runs on a
schedule or manual dispatch, not on unrelated PRs.

All production changes, Scala test changes, shared fixtures, resources,
notebooks, build/dependency files, pipeline/templates, and CI helpers run all
notebook E2E jobs. There is no blanket Markdown exemption:
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
Existing
explicit family-disable parameters and the Fabric fork credential restriction
are unchanged; "full" does not enable previously disabled suites.

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
