// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

// Same options as in build.scala
scalacOptions ++= Seq(
  "-encoding", "UTF-8",
  // Explain warnings, optimize
  "-deprecation", "-unchecked", "-feature", "-optimise",
  "-Xfatal-warnings", "-Xlint", // all warnings
  // -Y* are Scala options
  "-Yno-adapted-args", // "-Ywarn-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard"
  // this leads to problems sometimes: "-Yinline-warnings"
)
