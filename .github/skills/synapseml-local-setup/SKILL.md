---
name: synapseml-local-setup
description: Set up and validate SynapseML locally in WSL or Linux. Use when an agent needs SynapseML working locally, runs sbt compile/test, sees Java 21, Scala 2.12 compiler-bridge, bad constant pool index, Spark, or local validation failures.
compatibility: Linux/WSL with bash, git, rg, sbt, and JDK 11 installed. Designed for the SynapseML repo.
---

# SynapseML Local Setup

Use this skill before any local SynapseML build, compile, or test validation.

## Important

- Always use an explicit SynapseML repo path.
- Do not run SynapseML SBT with the machine default Java 21. Use JDK 11:
  `/usr/lib/jvm/java-11-openjdk-amd64`
- Java 21 can fail before project code compiles with `bad constant pool index: 0` while building Scala 2.12 `compiler-bridge_2.12`.
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
- JDK 11 availability.
- sbt version and SynapseML Scala/Spark versions.

### 2. Compile with JDK 11

Run [scripts/synapseml-sbt.sh](scripts/synapseml-sbt.sh):

```bash
scripts/synapseml-sbt.sh --repo <synapseml-repo> -- cognitive/Test/compile
```

Expected result:

- sbt welcome line says Java 11.
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

Use the JDK 11 wrapper for any targeted SBT command:

```bash
scripts/synapseml-sbt.sh --repo <synapseml-repo> -- '<module>/testOnly <SuiteName> -- -z "<test filter>"'
```

If tests fail before compiling project code, load [references/troubleshooting.md](references/troubleshooting.md).

## Known-good baseline

On 2026-05-05, this setup was validated with:

- JDK: `/usr/lib/jvm/java-11-openjdk-amd64`
- Java: `openjdk version "11.0.30"`
- SynapseML: Scala `2.12.17`, Spark `3.5.0`, sbt `1.10.11`
- Compile: `sbt 'cognitive/Test/compile'` succeeded.
- Smoke test: `UDFTransformerSuite` filtered test succeeded.
