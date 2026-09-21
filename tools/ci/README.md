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

## `databricks_impact.py` — conservative PR E2E gating

The `BuildAndCacheSbt` job compares a pull request with its target branch and
uses `databricks_impact.py` to decide independently whether the five CPU matrix
jobs and the GPU matrix job can be skipped. Scheduled, master, tag, and manual
builds always run both suites.

The detector mirrors the enabled test suites:

- CPU runs for runtime changes in any module and non-GPU notebooks.
- GPU runs for shared core/deep-learning runtime changes and the complete
  `GPUNotebooks` set selected by `DatabricksGPUTests`, including
  `Quickstart - End-to-end Local RAG with Phi Model`.
- Databricks utility changes are assigned to CPU, GPU, or both according to
  which suite imports them.

The detector is fail-open. Unknown paths, build definitions, templates,
environment files, shared test infrastructure, missing diffs, and detection
errors run both suites. It skips both suites only for paths known not to affect
runtime artifacts or notebook execution:

- GitHub metadata and workflows
- unrelated pipelines and ACR/Docker/Helm tooling
- CI helper code under `tools/ci/`
- website files
- Markdown/reStructuredText documentation
- module test source outside the Databricks notebook and shared test infrastructure

Unknown non-notebook assets under `docs/` remain fail-open because notebooks may
load adjacent data or configuration files.

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
