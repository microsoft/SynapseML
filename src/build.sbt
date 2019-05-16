// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

name := "mmlspark"

Extras.rootSettings

enablePlugins(ScalaUnidocPlugin)

// Use `in ThisBuild` to provide defaults for all sub-projects
version in ThisBuild := Extras.mmlVer

val topDir = file(".")

lazy val IntegrationTest2 = config("it").extend(Test)

val `core` = (project in topDir / "core")
  .configs(IntegrationTest2)
  .settings(Extras.defaultSettings: _*)

val `MMLSpark` = (project in topDir)
  .configs(IntegrationTest2)
  .settings(Extras.defaultSettings: _*)
  .aggregate(
    `core`)
  .dependsOn(
    `core` % "compile->compile;optional")
