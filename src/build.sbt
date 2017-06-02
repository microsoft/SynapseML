// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

name := "mmlspark"

Extras.rootSettings

enablePlugins(ScalaUnidocPlugin)

// Use `in ThisBuild` to provide defaults for all sub-projects
version in ThisBuild := Extras.mmlVer
