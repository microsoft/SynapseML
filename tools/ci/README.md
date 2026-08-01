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

### Tests

```bash
python -m pytest tools/ci/tests/ -v
```

`test_sbt_retry.py` drives the wrapper with a fake `sbt` (deterministic, no real
sleeps) to verify retry/backoff/stagger and visible-failure behaviour.
`test_pipeline_yaml.py` verifies `pipeline.yaml` parses and that every
sbt-running job is wired to the shared cache template + prewarm job.

## `databricks_impact.py` — conservative PR E2E gating

The `BuildAndCacheSbt` job compares a pull request with its target branch and
uses `databricks_impact.py` to decide whether the six Databricks matrix jobs can
be skipped. Scheduled, master, tag, and manual builds always run Databricks.

The detector is intentionally fail-open. It skips Databricks only when every
changed path is clearly unrelated to runtime artifacts and notebook execution:

- GitHub metadata and workflows
- CI helper code under `tools/ci/`
- website files
- Markdown/reStructuredText documentation
- non-notebook files under `docs/`
- module test source outside the Databricks notebook and shared test infrastructure

Runtime source, `.ipynb` notebooks, build definitions, pipeline/templates,
Databricks test utilities, unknown paths, an empty diff, or a failed target
branch fetch all keep Databricks E2E enabled.
