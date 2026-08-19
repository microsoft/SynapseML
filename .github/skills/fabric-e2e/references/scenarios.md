# Fabric E2E scenarios

## `runtime-smoke`

Runs a four-partition DataFrame action and records the Fabric application ID,
Spark version, and default parallelism. It proves managed Fabric Spark access,
not SynapseML module behavior.

## `jar-provenance`

Requires one `--extra-jar`. By default it loads
`com.microsoft.azure.synapse.ml.build.BuildInfo$`, asserts that the class source
contains the supplied jar's basename, and runs a distributed DataFrame action.

Override the class or expected token by putting scenario arguments last:

```bash
python tools/fabric_e2e/run.py \
  --scenario jar-provenance \
  --workspace <workspace> \
  --extra-jar <jar> \
  --scenario-args \
  --class-name <fully-qualified-class> \
  --expected-jar-token <jar-basename>
```

## `lightgbm-streaming`

Requires core, the exact `lightgbmlib` dependency, and the SynapseML LightGBM
jar, in that order. It asserts all three class sources, creates deterministic
synthetic training and validation data, and performs repeated public
`LightGBMClassifier.fit()` and prediction operations with streaming transfer
and single-dataset mode. Omitting `lightgbmlib` can mix the PR classes with an
incompatible runtime-bundled JNI library.

The manifest forces a non-runtime `java.library.path` for the driver and
executors. This makes `NativeLoader` extract `lib_lightgbm.so` and
`lib_lightgbm_swig.so` from the supplied JNI jar. The scenario records the
driver's effective path and `/proc/<pid>/maps` entries before fitting. Do not
override those two Spark settings unless the replacement preserves this
isolation.

Useful overrides:

- `--rows <n>`: generated row count; default 4000.
- `--partitions <n>`: Spark and LightGBM task count; default 4.
- `--native-threads <n>`: LightGBM native threads per task; default 2.
- `--repetitions <n>`: repeated fits; default 2.

For issue #2333 diagnostics, begin with the default starter pool. Use an
explicit ephemeral topology only when the test requires it:

```bash
python tools/fabric_e2e/run.py \
  --scenario lightgbm-streaming \
  --workspace <workspace> \
  --node-size Medium \
  --node-count 1 \
  --extra-jar <core-jar> \
  --extra-jar <lightgbmlib-jar> \
  --extra-jar <lightgbm-jar> \
  --scenario-args --repetitions 20
```

The scenario reports executor addresses and configured executor cores. Do not
claim multi-core coverage if that evidence does not show the intended topology.

## `openai-prompt-ai-functions`

Requires exact core and cognitive jars, in that order. It imports the public
PySpark `OpenAIPrompt` wrapper and proves that the JVM `FabricClient`,
`OpenAIPrompt`, and `OpenAIResponses` classes came from those supplied jars.

The scenario uses Fabric's implicit OpenAI workload endpoint and MWC token. It
must not receive a subscription key, AAD token, endpoint, or other credential
through scenario arguments or Spark configuration. The retained evidence
records only that the implicit endpoint was selected; it does not record the
endpoint URL or response text.

This profile uses `fabric-spark-cli notebook run`, not `batch submit`.
Platform-notebook execution supplies the notebook artifact context required by
the implicit LLM endpoint. A direct batch attached only to a Lakehouse reaches
the endpoint but is rejected because its workload-operation context is invalid.
The runner generates a unique notebook and deletes that exact notebook and
scratch lakehouse after the run. Structured markers are written to the scratch
lakehouse and downloaded before cleanup, avoiding the delegated-only executed
notebook snapshot API.
Platform notebooks use the workspace's configured runtime and pool, so the
runner rejects `--runtime`, `--node-size`, and `--node-count` for this profile.

One small structured prompt covers the origin behaviors used by PySpark AI
Functions: prompt interpolation and generation, sentiment classification,
summarization, translation, structured extraction, null propagation, usage,
and service-error handling. Assertions target schema, row count, obvious
sentiment labels, non-empty generated fields, null behavior, and empty error
columns rather than brittle exact prose.

The default model is `gpt-5-mini`. Override it only when the target Fabric
tenant enables a different supported model:

```bash
python tools/fabric_e2e/run.py \
  --scenario openai-prompt-ai-functions \
  --workspace <workspace> \
  --extra-jar <core-jar> \
  --extra-jar <cognitive-jar> \
  --scenario-args --model <model>
```

## Outputs

Each run writes under `target/fabric-e2e/<run-id>/` unless `--output-dir` is
provided:

- `evidence.json`: source commit, CLI version, jar hashes, Spark configuration,
  runtime evidence or pre-failure diagnostics, submission result, and cleanup
  result.
- `junit.xml`: one test result suitable for CI publication.
- `runner.log`: complete runner and CLI output.
- `fabric-logs/`: downloaded driver and executor logs for batch profiles.
- `scenario.ipynb` and `notebook-markers.jsonl`: generated input and retained
  structured output for notebook profiles.

The default cleanup deletes the unique lakehouse and, for notebook profiles,
the unique notebook created for that run.
