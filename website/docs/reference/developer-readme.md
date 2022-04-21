---
title: Build System Commands
hide_title: true
sidebar_label: Build System Commands
description: SynapseML Development Setup
---

# SynapseML Development Setup

1) [Install JDK 11](https://www.oracle.com/java/technologies/javase/jdk11-archive-downloads.html)
    - You may need an Oracle login to download.
2) [Install SBT](https://www.scala-sbt.org/1.x/docs/Setup.html)
3) Fork the repository on github
    - See how to here: [Fork a repo - GitHub Docs](https://docs.github.com/en/get-started/quickstart/fork-a-repo)
4) Clone your fork
    - `git clone https://github.com/<your GitHub handle>/SynapseML.git`
    - This will automatically add your fork as the default remote, called `origin`
5) Add another Git Remote to track the original SynapseML repo. It's recommended to call it `upstream`:
    - `git remote add upstream https://github.com/microsoft/SynapseML.git`
    - See more about Git remotes here: [Git - Working with remotes](https://git-scm.com/book/en/v2/Git-Basics-Working-with-Remotes)
6) Go to the directory where you cloed the repo (e.g., `SynapseML`) with `cd SynapseML`
6) Run sbt to compile and grab datasets
    - `sbt setup`
7) [Install IntelliJ](https://www.jetbrains.com/idea/download)    
8) Configure IntelliJ
    - Install [Scala plugin](https://plugins.jetbrains.com/plugin/1347-scala) during initialization
    - **OPEN** the synapseml directory from IntelliJ
    - If the project does not automatically import,click on `build.sbt` and import project
9) Prepare your Python Environment
    - Install [Miniconda](https://docs.conda.io/en/latest/miniconda.html)
    - Note: if you want to run conda commands from IntelliJ, you may need to select the option to add conda to PATH during installation.
    - Activate the `synapseml` conda environment by running `conda env create -f environment.yml` from the `synapseml` directory.

> NOTE
> 
> If you will be regularly contributing to the SynapseML repo, you'll want to keep your fork synced with the
> upstream repository. Please read [this GitHub doc](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/working-with-forks/syncing-a-fork)
> to know more and learn techniques about how to do it.

# Publishing and Using Build Secrets

To use secrets in the build you must be part of the synapsemlkeyvault
 and azure subscription. If you are MSFT internal would like to be 
 added please reach out `synapseml-support@microsoft.com`

# SBT Command Guide

## Scala build commands

### `compile`, `test:compile` and `it:compile`

Compiles the main, test, and integration test classes respectively

### `test`

Runs all synapsemltests

### `scalastyle`

Runs scalastyle check

### `unidoc`

Generates documentation for scala sources

## Python Commands

### `createCondaEnv`

Creates a conda environment `synapseml` from `environment.yml` if it does not already exist. 
This env is used for python testing. **Activate this env before using python build commands.**

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

Publishes Jar to synapseml's azure blob based maven repo. (Requires Keys)

### `publishLocal`

Publishes library to local maven repo

### `publishDocs`

Publishes scala and python doc to synapseml's build azure storage account. (Requires Keys)

### `publishSigned`

Publishes the library to sonatype staging repo

### `sonatypeRelease`

Promotes the published sonatype artifact
