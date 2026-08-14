// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

import sbt._
import sbt.Keys._

/**
  * Rewrites the existing dependency as the final deep-learning project setting. Keeping the version
  * separate lets the same change replay across Spark branches with different adjacent protobuf coordinates.
  */
object OnnxRuntimeDependency {
  val Version = "1.16.3"

  val settings: Seq[Setting[_]] = Seq(
    libraryDependencies ~= {
      _.map {
        case dependency
            if dependency.organization == "com.microsoft.onnxruntime" &&
              dependency.name == "onnxruntime_gpu" =>
          dependency.withRevision(Version)
        case dependency => dependency
      }
    }
  )
}
