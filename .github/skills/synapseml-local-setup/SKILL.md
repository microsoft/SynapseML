---
name: synapseml-local-setup
description: Set up and validate the Spark 4.0 SynapseML branch locally in WSL or Linux. Use for sbt compile/test, Java toolchain, Scala 2.13, Spark, or local validation failures.
compatibility: Linux/WSL with bash, git, rg, sbt, and JDK 17 installed. Designed for the SynapseML spark4.0 branch.
---

# SynapseML Local Setup

Use this skill before any local SynapseML build, compile, or test validation.

## Important

- Always use an explicit SynapseML repo path.
- Use the branch's tested JDK 17 toolchain:
  `/usr/lib/jvm/java-17-openjdk-amd64`
- The wrapper also opens `java.util.prefs`, which sbt requires on Java 17.
- Compile commands are safe. Some cognitive service tests create, write, list, or delete real Azure resources. Inspect before running those tests and ask for approval if live resources are involved.

## Workflow

### 1. Diagnose the repo and toolchain

Run [scripts/synapseml-doctor.sh](scripts/synapseml-doctor.sh):

```bash
scripts/synapseml-doctor.sh --repo <synapseml-repo>
```

Capture:

- Git branch and dirty state.
- Default Java version.
- JDK 17 availability.
- sbt version and SynapseML Scala/Spark versions.

### 2. Compile with JDK 17

Run [scripts/synapseml-sbt.sh](scripts/synapseml-sbt.sh):

```bash
scripts/synapseml-sbt.sh --repo <synapseml-repo> -- cognitive/Test/compile
```

Expected result:

- sbt welcome line says Java 17.
- `core` and `cognitive` main/test classes compile.
- Command exits with `[success]`.

### 3. Run a safe local smoke test

Run [scripts/synapseml-smoke-test.sh](scripts/synapseml-smoke-test.sh):

```bash
scripts/synapseml-smoke-test.sh --repo <synapseml-repo>
```

Expected result:

- One local Spark test runs.
- Output includes `All tests passed.`

### 4. Inspect PR-specific tests before running them

Before running service tests, run [scripts/check-live-service-tests.sh](scripts/check-live-service-tests.sh):

```bash
scripts/check-live-service-tests.sh --path <test-file-or-directory>
```

If it reports live-service hooks, ask the user before running that suite. Do not create or delete Azure Search indexes just to test a PR.

### 5. Run targeted tests only after safety review

Use the JDK 17 wrapper for any targeted SBT command:

```bash
scripts/synapseml-sbt.sh --repo <synapseml-repo> -- '<module>/testOnly <SuiteName> -- -z "<test filter>"'
```

If tests fail before compiling project code, load [references/troubleshooting.md](references/troubleshooting.md).

## Expected toolchain

- JDK: `/usr/lib/jvm/java-17-openjdk-amd64`
- SynapseML: Scala `2.13.16`, Spark `4.0.1`
- sbt: version pinned in `project/build.properties`
