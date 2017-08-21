// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

// Similar options as in build.scala (since sbt runs with 2.12)
scalacOptions ++= Seq(
  "-encoding", "UTF-8",
  // Explain warnings
  "-deprecation", "-unchecked", "-feature",
  "-Xfatal-warnings",
  // "-Xlint", // all warnings
  //   => disable since it spits out many unused-imports warnings in a plain .sbt
  // -Y* are Scala options
  "-Yno-adapted-args", // "-Ywarn-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard"
  // this leads to problems sometimes: "-Yinline-warnings"
)
