# CI bootstrap helpers

## `sbt_retry.sh` — resilient sbt bootstrap

SynapseML's Azure Pipelines (`pipeline.yaml`) fans out ~30 hosted-agent matrix
jobs. Each one cold-bootstraps the sbt launcher (`org.scala-sbt:sbt:<version>`,
pinned in `project/build.properties`) and resolves Ivy dependencies from public
Maven Central. When many fresh agents — and several overlapping PR builds — do
this simultaneously, Maven Central returns **HTTP 429 (rate limit)** and the
`Setup repo` step fails before any test runs (e.g. ADO build 229124511).

The durable fix has three layers:

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
3. **`sbt_retry.sh`** (supplement) — smooths the *cold-cache* path only. It adds a
   bounded random start stagger so concurrent cold jobs don't hit Maven at the
   same instant, then bounded jittered exponential-backoff retries. On exhaustion
   it fails visibly (non-zero exit); it never masks a failure with a success
   fallback. Exact hits on all three caches disable the start stagger
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
of each failed attempt and deletes exactly those modules from `~/.ivy2/cache`,
`~/.ivy2/local`, and the Coursier cache before backing off, so the next attempt
re-fetches them cleanly. Unrelated failures evict nothing. Override
`SBT_SETUP_IVY_HOME` / `SBT_SETUP_COURSIER_CACHE` with absolute, non-root paths
to relocate the scan. Eviction is disabled if `HOME` is unavailable and no safe
override is provided. Maven Central is probed at most once per unresolved
coordinate per wrapper invocation, so diagnostics do not amplify an outage.

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

## `databricks_impact.py` — conservative PR E2E gating

The `BuildAndCacheSbt` job compares a pull request with its target branch and
uses `databricks_impact.py` to decide independently whether the five CPU matrix
jobs and the GPU matrix job can be skipped. Scheduled, master, tag, and manual
builds always run both suites.

The detector mirrors the enabled test suites:

- CPU runs for runtime changes in any module and non-GPU notebooks.
- GPU runs for shared core/deep-learning runtime changes and the three
  `Fine-tune`/`Phi Model` notebooks selected by `DatabricksGPUTests`.
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
