// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

name := "mmlspark"

Extras.rootSettings

enablePlugins(ScalaUnidocPlugin)

// Use `in ThisBuild` to provide defaults for all sub-projects
version in ThisBuild := Extras.mmlVer

val fullDependencies = "compile->compile;test->test"

lazy val core = project
  .settings(Extras.defaultSettings: _*)

lazy val codegen = project
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    core % fullDependencies)

lazy val lib = project
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    core % fullDependencies)

lazy val io = project
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    core % fullDependencies,
    lib % fullDependencies)

lazy val `image-transformer` = project
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    core % fullDependencies,
    io % fullDependencies)

lazy val cntk = project
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    core % fullDependencies,
    io % fullDependencies,
    lib % fullDependencies,
    `image-transformer` % fullDependencies)

lazy val `image-featurizer` = project
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    core % fullDependencies,
    io % fullDependencies,
    cntk % fullDependencies,
    `image-transformer` % fullDependencies,
    lib % fullDependencies)

lazy val lightgbm = project
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    core % fullDependencies,
    lib % fullDependencies)

val aggregationDeps = "compile->compile;optional"

lazy val mmlspark = (project in file("."))
  .settings(Extras.defaultSettings: _*)
  .aggregate(cntk, codegen, core, `image-featurizer`, `image-transformer`, io, lib, lightgbm)
  .dependsOn(
    `cntk` % aggregationDeps,
    `codegen` % aggregationDeps,
    `core` % aggregationDeps,
    `image-featurizer` % aggregationDeps,
    `image-transformer` % aggregationDeps,
    `io` % aggregationDeps,
    `lib` % aggregationDeps,
    `lightgbm` % aggregationDeps)
