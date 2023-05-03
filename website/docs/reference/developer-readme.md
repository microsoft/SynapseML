---
title: Development Setup and Building From Source
hide_title: true
sidebar_label: Development Setup
description: SynapseML Development Setup
---

# SynapseML Development Setup

1. [Install JDK 11](https://www.oracle.com/java/technologies/javase/jdk11-archive-downloads.html)
    - You may need an Oracle login to download.
1. [Install SBT](https://www.scala-sbt.org/1.x/docs/Setup.html)
1. Install Apache Spark
   - [Download and install Apache Spark](https://spark.apache.org/downloads.html) with version >= 3.2.0. (SynapseML v0.11.1 only supports spark version >= 3.2.0)
   - Extract downloaded zipped files (with 7-Zip app on Windows or `tar` on linux) and remember the location of
extracted files, we take `C:\bin\spark-3.2.0-bin-hadoop3.2` or `~/bin/spark-3.2.0-bin-hadoop3.2/` as an example here.

   - On Windows, run the following commands to set the environment variables used to locate Apache Spark. Make sure to run the command prompt in administrator mode.
    <Tabs groupId="operating-systems">
        <TabItem value="win" label="Windows" default>

            setx /M HADOOP_HOME C:\bin\spark-3.2.0-bin-hadoop3.2\
            setx /M SPARK_HOME C:\bin\spark-3.2.0-bin-hadoop3.2\
            setx /M PATH "%PATH%;%HADOOP_HOME%;%SPARK_HOME%bin" # Warning: Don't run this if your path is already long as it will truncate your path to 1024 characters and potentially remove entries!

        </TabItem>
    </Tabs>

    - On Linux, add the following to your .bashrc:
    <Tabs groupId="operating-systems">
        <TabItem value="linux" label="Mac/Linux">

            export SPARK_HOME=~/bin/spark-3.2.0-bin-hadoop3.2/
            export PATH="$SPARK_HOME/bin:$PATH"

        </TabItem>
    </Tabs>

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
1. On Windows, install WinUtils
    - Download [WinUtils.exe](https://github.com/steveloughran/winutils/blob/master/hadoop-3.0.0/bin/winutils.exe) and copy it into the `bin` directory of your Spark installation, e.g. C:\Users\user\AppData\Local\Spark\spark-3.3.2-bin-hadoop3\bin
1. Install pre-commit
    - This repository uses the [pre-commit](https://pre-commit.com/index.html) tool to manage git hooks and enforce linting/coding styles.
    - The hooks are configured in [.pre-commit-config.yaml](https://github.com/microsoft/SynapseML/blob/master/environment.yml).
    - To use the hooks, run the following commands:
    ```bash
    pip install pre-commit
    pre-commit install
    ```
    - Now `pre-commit` should automatically run on every `git commit` operation to find AND fix linting issues.

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
