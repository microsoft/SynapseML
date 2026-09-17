# Shared Spark 4 branch context

Read the matching [Spark 4.0](branch-spark4p0.md) or
[Spark 4.1](branch-spark4p1.md) reference and the
[live configuration sources](../SKILL.md#sources-of-truth).
Keep validation results and integrated commit IDs in PR descriptions, not here.

## Sync decisions

- Land portable fixes on master, then merge them into the ports. Check the
  sibling port before debugging a compatibility failure from scratch.
- Compare the merge base, master, and port content per conflict. A squash merge
  can hide already integrated changes from ancestry; consult the prior sync's
  recorded content baseline. Preserve real merge parents and repository policy.
- Preserve version-driven differences, not every difference between branches.
  A passing sibling build is a control, not proof that its runtime or native
  dependencies can replace the target's.

## Toolchains and data

- Spark 4 uses Scala 2.13 and Java 17-era tooling. Check every JDK entry point,
  including Docker, environment files, workflow jobs, and Java setup templates.
  Do not restore removed CMS JVM flags. A replay job's `JAVA_VERSION` describes
  its replay target, not necessarily the pipeline's owning branch.
- Preserve dependency constraints required by the branch's Python and native
  ABI. Check wheel availability and package metadata; PyArrow and MLflow bounds
  are coupled. Keep explanatory comments, but verify them against actual pins.
- Spark-produced collections can be mutable even when an API expects immutable
  `Seq`. Preserve `CognitiveServiceBase` collection normalization and test other
  row access paths with real Spark rows. Prefer `toIndexedSeq` when indexed
  access matters; compilation alone does not catch these runtime casts.
- Petastorm compatibility is driven by PyArrow APIs, not just Python versions.
  Preserve both `_petastorm_compat.py` and the serialized worker setup in
  `_horovod.py`. Unit tests using stubbed Horovod estimators do not exercise
  this path.

## Portable sync lessons

- Keep foreign-owned parameter guards in constructor arguments, runtime
  defaults, and generated stubs. A Python guard does not prove R defaults safe.
- Distinguish production JARs from test JARs, including snapshot aliases and
  exploded class directories. Verify the loaded candidate, nonempty generated
  APIs, and exclusion of test fixtures with packaged artifacts together.
  SBT `bgRunMain` uses `Runtime / fullClasspathAsJars`; ordinary classpath
  overrides or `java.class.path` alone can miss the actual codegen dependencies.
- Inspect the public Python class and its generated base before changing
  wrappers or stubs. An internal wrapper can have a different class name;
  generated overrides must not hardcode the public name in `super()`.
- `PythonInitMerger` makes hand-written initializers live code after generated
  imports. Remove redundant re-exports and stale `__all__` lists, while retaining
  exports codegen cannot provide. Run the HTTP and recommendation package-export
  guards against the installed package.
- Candidate Maven versions can legitimately contain `-pythonX.Y`. Preserve the
  exact published coordinate rather than confusing it with a wheel version.
  Validate the declared Python environment, including optional HTTP transports.
- Downstream build layouts differ. Recognize supported layouts, but fail for
  referenced missing helpers or unknown layouts. See the
  [CI helper documentation](../../../../tools/ci/README.md).

## R and Spark SQL

- Preserve both `spark.sql.ansi.enabled` and
  `spark.sql.ansi.doubleQuotedIdentifiers` for generated R queries.
- Keep the compatible sparklyr/R/dependency pairing from the environment file
  and the `SPARK_HOME` connection path. Interleaved NULL-connection errors can
  originate in sparklyr/dbplyr selection proxies; inspect the backtrace rather
  than assuming the Spark session died.
- Load nested stages through the shared `PipelineStageWrappable.rLoadLine`
  implementation and JVM stages. Validate sparklyr-internal API compatibility
  when changing its version. Run `RCodegenSuite` before broader R validation.
- Preserve qualified SAR self-join columns. Use valid feature vectors in tests
  of ordinary training; test rejected NaN input separately rather than weakening
  assertions when Spark validation becomes stricter.

## Runtime and CI

- Select CPU/GPU runtimes for the target Spark version. Instance pools do not
  select a runtime. When branches share a GPU pool, serialize capacity-heavy
  builds and keep the consolidated suite, CPU driver placement, and dynamic
  `GPUNotebooks` selection rather than hardcoded notebook indices.
- Match Horovod wheels to the actual Python/PyTorch runtime. Avoid unnecessary
  overrides of runtime-provided torch/torchvision, which can trigger large
  downgrades. Library-install timeouts are not proof of capacity exhaustion;
  inspect package statuses and notebook setup logs.
- Check whether `DatabricksCPUStreamingTests` is actually scheduled. An omitted
  suite produces no skipped-test result. Preserve streaming cluster isolation:
  notebook shutdown can cancel other SparkContext jobs.
- Verify Fabric support and workspace Spark selection on the actual branch.
  A working Databricks run is not Fabric evidence. Inspect loaded JVM/native
  artifacts separately from Python package versions, including LightGBM.
- Read the first provisioning error before retrying. Quota failures can leave
  partial resources and later name conflicts; another naming change is not a
  quota fix. Clean up only resources whose ownership is established.
- Confirm `/azp run` produced a build of the PR merge ref. UI-defined Azure
  trigger filters can override YAML. Check the definition when no build appears;
  distinguish a trigger-driven `pullRequest` run from a manually queued run.
- Replay compilation, GitHub checks, and stubbed local tests do not replace
  branch-specific service, R, native, and real-runtime validation.

Use the shared [Python isolation and async cleanup checklist](../../code-review/SKILL.md#python-isolation-and-async-cleanup),
[CI triage](../../synapseml-pr-loop/references/ci-triage.md), and
[readiness gates](../../synapseml-pr-loop/references/readiness-gates.md)
instead of duplicating their procedures in branch references.
