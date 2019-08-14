# MMLSpark Development Setup

1) [Install SBT](https://www.scala-sbt.org/1.x/docs/Setup.html)
    \- Make sure to download JDK 1.8 if you don't have it
2) Git Clone Repository
    \- `git clone https://github.com/Azure/mmlspark.git`
3) Run sbt to compile and grab datasets
    \- `cd mmlspark`
    \- `sbt setup`
4) [Install IntelliJ](https://www.jetbrains.com/idea/download)
    \- Install Scala plugins during install
5) Configure IntelliJ
    \- **OPEN** the mmlspark directory
    \- If the project does not automatically import,click on `build.sbt` and import project

# Publishing and Using Build Secrets

To use secrets in the build you must be part of the mmlspark keyvault
 and azure subscription. If you are MSFT internal would like to be 
 added please reach out `mmlspark-support@microsoft.com`

# SBT Command Guide

## Scala build commands

### `compile`, `test:compile` and `it:compile`

Compiles the main, test, and integration test classes respectively

### `test`

Runs all mmlspark tests

### `scalastyle`

Runs scalastyle check

### `unidoc`

Generates documentation for scala sources

## Python Commands

### `createCondaEnv`

Creates a conda environment `mmlspark` from `environment.yaml` if it does not already exist. 
This env is used for python testing. **Activate this env before using python build commands.**

### `cleanCondaEnv`

Removes `mmlspark` conda env

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

Publishes Jar to mmlspark's azure blob based maven repo. (Requires Keys)

### `publishLocal`

Publishes library to local maven repo

### `publishDocs`

Publishes scala and python doc to mmlspark's build azure storage account. (Requires Keys)

### `publishSigned`

Publishes the library to sonatype staging repo

### `sonatypeRelease`

Promotes the published sonatype artifact