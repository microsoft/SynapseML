---
name: fabric-e2e
description: Run SynapseML pull-request code on a real Microsoft Fabric Spark runtime with fabric-spark-cli. Use for Fabric E2E validation, runtime-only bugs, jar provenance, LightGBM native or concurrency issues, and PR evidence that local Spark tests cannot provide.
compatibility: Windows, Linux, or WSL with Python 3.10+, Azure CLI authentication, fabric-spark-cli, and access to a Fabric test workspace.
---

# SynapseML Fabric E2E

Use the checked-in runner for real Fabric validation. Do not recreate the
legacy Fabric REST/Spark Job Definition client or manually upload jars.

## Important

- Resolve the PR base branch and load `synapseml-branches` first.
- Read the live `fabric-spark-cli` batch and notebook help before composing a
  run because their flags evolve.
- Always pass `--workspace`; unattended runs must never use an interactive
  picker.
- Build jars from the exact checkout under test. A green run against a
  runtime-bundled or previously published jar is not PR evidence.
- Do not pass credentials through scenario arguments or Spark configuration.
- Evidence redacts secret-like Spark configuration values as defense in depth;
  redaction is not permission to pass credentials through the runner.
- For OpenAI scenarios, use Fabric's implicit workload endpoint and MWC token.
  Never add an Azure OpenAI key to CLI arguments, Spark configuration, or
  retained logs.
- The runner creates a unique scratch lakehouse and deletes that exact item in
  `finally`. Notebook profiles also create and delete one unique notebook. Use
  `--keep-lakehouse` only for active debugging, then delete it.
- Do not expose Fabric credentials to untrusted fork builds.

## Coverage model

The runner exposes four agent-invokable scenarios: managed runtime smoke, exact
jar provenance, LightGBM streaming with validation data, and OpenAIPrompt with
AI Functions-inspired cases. Select the scenario that exercises the affected
module; a generic smoke run is not module coverage.

`FabricOpenAIPromptE2E` promotes the OpenAIPrompt scenario into an always-on
trusted pipeline job. The other scenarios remain available to agents for
targeted PR validation without making known runtime regressions block every
SynapseML build. A failed module scenario is valid blocking evidence when its
diagnostics prove that Fabric loaded the exact jars under review.

## Workflow

### 1. Confirm the live CLI and scenarios

Run `fabric-spark-cli --help`, `fabric-spark-cli batch submit --help`,
`fabric-spark-cli notebook run --help`, and:

```bash
python tools/fabric_e2e/run.py --list-scenarios
```

### 2. Prove managed runtime access

Run `runtime-smoke` with an explicit test workspace:

```bash
python tools/fabric_e2e/run.py \
  --scenario runtime-smoke \
  --workspace <workspace>
```

Capture the `SYNAPSEML_FABRIC_E2E_EVIDENCE` path. The evidence must contain a
Fabric application ID and runtime Spark version.

### 3. Build the exact SynapseML jars

Use the JDK selected by `synapseml-local-setup`. Build only the affected module
and its dependencies. Do not use a jar from another checkout or an earlier CI
run.

### 4. Run the relevant PR scenario

Pass jars in classpath-precedence order with one `--extra-jar` per jar.
For LightGBM, include the exact `lightgbmlib` dependency between core and the
SynapseML LightGBM jar so Fabric cannot mix PR Scala code with a bundled JNI
library. The scenario also overrides `java.library.path` on the driver and
executors so `NativeLoader` extracts both `.so` files from that exact jar
instead of accepting Fabric's preinstalled native libraries:

```bash
python tools/fabric_e2e/run.py \
  --scenario lightgbm-streaming \
  --workspace <workspace> \
  --extra-jar <core-jar> \
  --extra-jar <lightgbmlib-jar> \
  --extra-jar <lightgbm-jar>
```

For the PySpark AI Functions-inspired check, pass exact core and cognitive jars.
The scenario imports the public `OpenAIPrompt` wrapper and requires Fabric's
implicit OpenAI endpoint; it must not be adapted to inject a model key. The
runner executes this scenario as a Fabric platform notebook because direct
batch/Lakehouse execution does not supply the LLM workload-operation context:

```bash
python tools/fabric_e2e/run.py \
  --scenario openai-prompt-ai-functions \
  --workspace <workspace> \
  --extra-jar <core-jar> \
  --extra-jar <cognitive-jar>
```

The Azure Pipeline pins `FABRIC_E2E_ENV` and `FABRIC_E2E_WORKSPACE` to
the proven environment and dedicated build service workspace. Do not derive
these values from the legacy integration user; the build service principal
cannot see per-user workspaces.
Both Fabric jobs authenticate through the active Azure CLI service connection.
The legacy Spark Job Definition suite resolves the pinned workspace explicitly
instead of using the retired certificate identity. `FabricOpenAIPromptE2E`
remains a separate job so failures in either coverage surface cannot skip or
mask the other gate.

When the jars came from another Git worktree, pass that checkout through
`--source-repo` so evidence records the producing commit rather than the
runner's checkout.

Put any `--scenario-args` last. See
[references/scenarios.md](references/scenarios.md) for profiles and arguments.

### 5. Review evidence

Require all of the following before citing the run:

- commit SHA is the checkout under review;
- every jar has a SHA-256 digest;
- secret-like Spark configuration values are recorded as `<redacted>`;
- class-source paths name the supplied jars;
- `junit.xml` and `runner.log` exist, plus downloaded Fabric logs for batch
  profiles or `notebook-markers.jsonl` for notebook profiles;
- scratch-lakehouse cleanup succeeded.

For a passing claim, also require `status` to be `passed` and runtime evidence
to show the intended Spark version and topology. A failed run is valid blocking
evidence when `runtimeDiagnostics` proves the expected class and native-library
sources; cite the failure and do not relabel it as a successful Fabric check.

Do not describe a generic runtime smoke as coverage for a module-specific
behavior. Cite the exact scenario and evidence path in the PR.
