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
   boot directory (`~/.sbt/boot`) and the Ivy cache (`~/.ivy2/cache`). In steady
   state, jobs restore these from Azure's cache service and never touch Maven
   Central. Keys are derived from the bootstrap inputs (`project/build.properties`,
   `project/plugins.sbt`, `build.sbt`) so they invalidate exactly when those
   inputs change. `continueOnError` keeps a cache miss/corruption non-fatal.
2. **`BuildAndCacheSbt` prewarm job** — warms those caches once per pipeline run
   (mirrors the existing `BuildAndCacheCondaEnv` job).
3. **`sbt_retry.sh`** (supplement) — smooths the *cold-cache* path only. It adds a
   bounded random start stagger so concurrent cold jobs don't hit Maven at the
   same instant, then bounded jittered exponential-backoff retries. On exhaustion
   it fails visibly (non-zero exit); it never masks a failure with a success
   fallback.

### Tests

```bash
python -m pytest tools/ci/tests/ -v
```

`test_sbt_retry.py` drives the wrapper with a fake `sbt` (deterministic, no real
sleeps) to verify retry/backoff/stagger and visible-failure behaviour.
`test_pipeline_yaml.py` verifies `pipeline.yaml` parses and that every
sbt-running job is wired to the shared cache template + prewarm job.
