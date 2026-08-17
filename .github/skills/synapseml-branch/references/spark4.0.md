# `spark4.0`

- Shared Spark 4.0 port branch with Scala 2.13-era adaptations; verify exact
  current versions in this branch's `build.sbt` and `environment.yml`.
- Merge `master` into this branch; preserve only genuine runtime differences.
- This branch has historically lagged `master` substantially. Do not add it to
  compatibility replay until a real patch applies and compiles without turning
  the check into permanent noise.
- PR automation previously omitted this target entirely. Inspect the workflows
  and `pipeline.yaml` on `spark4.0`, then cite actual queued builds; a fix merged
  only to `master` does not prove this branch inherited it.
- Treat any `.agents/skills` copy as legacy; use `.github/skills`.
