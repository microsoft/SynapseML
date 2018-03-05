// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

name := "mmlspark"

Extras.rootSettings

enablePlugins(ScalaUnidocPlugin)

// Use `in ThisBuild` to provide defaults for all sub-projects
version in ThisBuild := Extras.mmlVer

lazy val core = project
  .settings(Extras.defaultSettings: _*)

lazy val codegen = project
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    core % "compile->compile;test->test")

lazy val lib = project
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    core % "compile->compile;test->test")

lazy val io = project
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    core % "compile->compile;test->test",
    lib % "compile->compile;test->test")

lazy val `image-transformer` = project
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    core % "compile->compile;test->test",
    io % "compile->compile;test->test")

lazy val cntk = project
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    core % "compile->compile;test->test",
    io % "compile->compile;test->test",
    lib % "compile->compile;test->test",
    `image-transformer` % "compile->compile;test->test")

lazy val `image-featurizer` = project
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    core % "compile->compile;test->test",
    io % "compile->compile;test->test",
    cntk % "compile->compile;test->test",
    `image-transformer` % "compile->compile;test->test",
    lib % "compile->compile;test->test")

lazy val lightgbm = project
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    core % "compile->compile;test->test",
    lib % "compile->compile;test->test")

lazy val mmlspark = (project in file("."))
  .settings(Extras.defaultSettings: _*)
  .aggregate(cntk, codegen, core, `image-featurizer`, `image-transformer`, io, lib, lightgbm)
  .dependsOn(
    `cntk` % "compile->compile;optional",
    `codegen` % "compile->compile;optional",
    `core` % "compile->compile;optional",
    `image-featurizer` % "compile->compile;optional",
    `image-transformer` % "compile->compile;optional",
    `io` % "compile->compile;optional",
    `lib` % "compile->compile;optional",
    `lightgbm` % "compile->compile;optional")
