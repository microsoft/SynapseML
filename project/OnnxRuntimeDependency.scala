// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

import sbt._
import sbt.Keys._

/**
  * Rewrites the known ONNX Runtime artifacts (`onnxruntime`, the default CPU-only artifact, and
  * `onnxruntime_gpu`, the opt-in CUDA artifact) to a single shared version, as the final deep-learning
  * project setting. Keeping the version separate lets the same change replay across Spark branches with
  * different adjacent protobuf coordinates, and covers a branch that still declares `onnxruntime_gpu` on
  * its own test classpath.
  *
  * Matching is restricted to `ManagedArtifactIds` (not just the `com.microsoft.onnxruntime` organization):
  * a plain org-wide match would silently coerce the version of any future, unrelated artifact published
  * under the same organization (for example one with its own independent release cadence) without anyone
  * noticing. Any dependency declaration this setting actually changes -- or any unmanaged artifact under
  * this organization it deliberately leaves untouched -- is logged so the override is never silent.
  *
  * GH2417: 1.8.1 fails to load its natives for local Spark 3.5 inference, and the GPU-only artifact never
  * ships macOS natives at all (CUDA has no macOS support), so upgrading `onnxruntime_gpu` alone cannot fix
  * macOS. 1.17.3 is confirmed to fix local CPU inference and additionally publishes a CPU-only `onnxruntime`
  * artifact with macOS x64/aarch64 natives, so it is used as the default cross-platform dependency in
  * build.sbt. See docs/Explore Algorithms/Deep Learning/ONNX.md for the CUDA opt-in instructions.
  */
object OnnxRuntimeDependency {
  val Version = "1.17.3"
  private val Organization = "com.microsoft.onnxruntime"
  private val ManagedArtifactIds: Set[String] = Set("onnxruntime", "onnxruntime_gpu")

  val settings: Seq[Setting[_]] = Seq(
    libraryDependencies ~= {
      _.map {
        case dependency if dependency.organization == Organization && ManagedArtifactIds(dependency.name) =>
          if (dependency.revision != Version) {
            println(s"[info] OnnxRuntimeDependency: overriding $Organization:${dependency.name} " +
              s"${dependency.revision} -> $Version (see project/OnnxRuntimeDependency.scala).")
          }
          dependency.withRevision(Version)
        case dependency if dependency.organization == Organization =>
          println(s"[warn] OnnxRuntimeDependency: leaving unmanaged $Organization:${dependency.name}:" +
            s"${dependency.revision} at its declared version -- add it to ManagedArtifactIds in " +
            s"project/OnnxRuntimeDependency.scala if it should track the shared $Version instead.")
          dependency
        case dependency => dependency
      }
    }
  )
}
