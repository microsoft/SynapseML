---
title: Developer Setup
hide_title: true
sidebar_label: Developer Setup
description: Developer Setup
---

# SynapseML Development Setup

1. [Install JDK 11](https://www.oracle.com/java/technologies/javase/jdk11-archive-downloads.html)
    - You may need an Oracle login to download.
1. [Install SBT](https://www.scala-sbt.org/1.x/docs/Setup.html)
1. Fork the repository on GitHub
    - See how to here: [Fork a repo - GitHub Docs](https://docs.github.com/en/get-started/quickstart/fork-a-repo)
1. Clone your fork
    - `git clone https://github.com/<your GitHub handle>/SynapseML.git`
    - This command will automatically add your fork as the default remote, called `origin`
1. Add another Git Remote to track the original SynapseML repo. It's recommended to call it `upstream`:
    - `git remote add upstream https://github.com/microsoft/SynapseML.git`
    - See more about Git remotes here: [Git - Working with remotes](https://git-scm.com/book/en/v2/Git-Basics-Working-with-Remotes)
1. Go to the directory where you cloned the repo (for instance, `SynapseML`) with `cd SynapseML`
1. Run sbt to compile and grab datasets
    - `sbt setup`
1. [Install IntelliJ](https://www.jetbrains.com/idea/download)
1. Configure IntelliJ
    - Install [Scala plugin](https://plugins.jetbrains.com/plugin/1347-scala) during initialization
    - **OPEN** the SynapseML directory from IntelliJ
    - If the project doesn't automatically import, click on `build.sbt` and import the project
1. Prepare your Python Environment
    - Install [Miniconda](https://docs.conda.io/en/latest/miniconda.html)
    - Note: if you want to run conda commands from IntelliJ, you may need to select the option to add conda to PATH during installation.
    - Activate the `synapseml` conda environment by running `conda env create -f environment.yml` from the `synapseml` directory.
    :::note
    If you're using a Windows machine, remove
    `horovod` requirement in the environment.yml file, because horovod installation only
    supports Linux or macOS. Horovod is used only for namespace `synapse.ml.dl`.
    :::
1. Update the ScalaTest Configuration Template
    - In IntelliJ, select the sandwich menu in the top left.
    - Select Run, then select Edit Configurations. At the bottom of the pop-up, select Edit Configuration Templates.
    - Select ScalaTest from the list on the right
    - Under VM options, add `--add-exports java.base/sun.nio.ch=ALL-UNNAMED  `. Apply the changes.


> NOTE
>
> If you will be regularly contributing to the SynapseML repo, you'll want to keep your fork synced with the
> upstream repository. Please read [this GitHub doc](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/working-with-forks/syncing-a-fork)
> to know more and learn techniques about how to do it.

# Publishing and Using Build Secrets

To use secrets in the build, you must be part of the synapsemlkeyvault
 and Azure subscription. If you're MSFT internal and would like to be
 added, reach out to `synapseml-support@microsoft.com`

# SBT Command Guide

## Scala build commands

### `compile`, `test:compile` and `it:compile`

Compiles the main, test, and integration test classes respectively

### `test`

Runs all synapsemltests

### Fabric test workspace cleanup

`core/testOnly com.microsoft.azure.synapse.ml.nbtest.FabricTestCleanup` deletes
repository-owned test items only when both their creation and last-update times
are strictly older than 24 hours, measured in UTC. It uses the existing Fabric
integration account and workspace environment variables.

CI runs a named `Fabric cleanup preflight` task after authentication and build
setup, then runs E2E only if that task succeeds. Cleanup results and phase
metadata are retained even if E2E is skipped or fails.

Each smoke and notebook suite performs its own cached preflight before creating
its first Fabric resource, both in CI and when run directly. CI therefore runs
cleanup once in the gate and once more per E2E suite. Suite construction does not
connect to Fabric. A failed preflight is reported by the selected tests without retrying
cleanup or starting notebook work; successful notebook runs retain their bounded
parallel execution and per-job artifact cleanup. Interrupted cleanup preserves
the interrupt signal. Store creation and executor setup failures are also
cached, so later tests do not repeat initialization or start another notebook batch.
Per-job cleanup never suppresses an interrupt or fatal error behind a notebook
failure. The cleanup throwable escapes with the earlier notebook failure attached
where that throwable permits suppression. An unsuccessful per-job deletion stays
tracked for final cleanup.

Set `SYNAPSEML_FABRIC_CLEANUP_DRY_RUN=true` to preview eligible deletions without
changing the workspace. Omit it, or set it to `false`, to perform cleanup.
Review the preview before a manual cleanup. A preview can omit lakehouses whose
job definitions have not yet been deleted.

Cleanup recognizes the OSS ownership description on new items and the exact
test names and descriptions on older items. A legacy lakehouse without a unique
suffix also needs a relationship to an identified OSS test job. Unknown items,
missing metadata, active or recently completed jobs, enabled or unknown
schedules, and shared dependencies are not deletion candidates.
Stores are also retained while any OSS test job remains, or any notebook/job
has no usable reference edges, rather than assuming that missing edges prove
there are no consumers.
Relation entries must contain only GUID references in nonempty objects or arrays.
Malformed references or unknown metadata fail the inventory read rather than
authorizing cleanup with an incomplete graph. A valid reference elsewhere in
the entry cannot hide them.

Job definitions are deleted before their stores. After the deletion API returns,
cleanup checks inventory immediately, then makes up to ten more checks with
30-second waits per item. Each item gets a fresh five-minute waiting budget plus
request time, not a wall-clock deadline. Confirmation polling never resends DELETE.
An unconfirmed or failed deletion prevents store deletion. After failed DELETE
requests, independent job deletions are still attempted, and collected errors
fail the cleanup afterward. If a deletion cannot be confirmed, or any inventory,
job-history, or schedule read fails, cleanup stops immediately. Nonfatal failures
from those checks are rethrown with earlier deletion errors attached as
suppressed exceptions. Reused exception
instances are never added as their own suppressed error; interrupts and fatal
errors keep their existing propagation.
SQL endpoints are left to Fabric's lakehouse deletion rather than deleted
independently. Authentication, inventory, and deletion errors fail the cleanup.

Smoke and notebook job-wait handlers restore interrupt status and propagate
interrupts and fatal errors unchanged. Ordinary failures retain the notebook
name and the original cause.

### `scalastyle`

Runs scalastyle check on main

### `test:scalastyle`

Runs scalastyle check on test

### `unidoc`

Generates documentation for scala sources

## Python Commands

### `createCondaEnv`

Creates a conda environment `synapseml` from `environment.yml` if it doesn't already exist.
This env is used for python testing.
**Activate this env before using python build commands.**

### `cleanCondaEnv`

Removes `synapseml` conda env

### `packagePython`

Compiles scala, runs python generation scripts, and creates a wheel

### `generatePythonDoc`

Generates documentation for generated python code

### `installPipPackage`

Installs generated python wheel into existing env

### `testPython`

Generates and runs python tests

## Environment + Publishing Commands

### `getDatasets`

Downloads all datasets used in tests to target folder

### `setup`

Combination of `compile`, `test:compile`, `it:compile`, `getDatasets`

### `package`

Packages the library into a jar

### `publishBlob`

Publishes Jar to SynapseML's Azure blob-based Maven repo. (Requires Keys)

### `publishLocal`

Publishes library to the local Maven repo

### `publishDocs`

Publishes scala and python doc to SynapseML's Azure storage account. (Requires Keys)

### `publishSigned`

Publishes the library to Sonatype staging repo

### `sonatypeRelease`

Promotes the published Sonatype artifact
