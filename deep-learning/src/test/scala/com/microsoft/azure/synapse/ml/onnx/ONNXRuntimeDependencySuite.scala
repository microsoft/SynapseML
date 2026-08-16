// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.onnx

import com.microsoft.azure.synapse.ml.build.BuildInfo
import com.microsoft.azure.synapse.ml.core.env.FileUtilities
import org.scalatest.funsuite.AnyFunSuite

/**
  * GH2417: verifies the published dependency strategy for ONNX Runtime, independent of whatever version
  * happens to be resolved on the test classpath (that runtime behavior is covered by ONNXModelSuite and
  * ONNXRuntime's own tests). This reads the build definitions directly so a future edit that re-adds
  * onnxruntime_gpu as a default/compile dependency -- reintroducing the ~300+MB, macOS-incompatible
  * artifact for every user -- fails a fast, local test instead of only being caught by a live macOS run.
  *
  * A live "does this resolve from Maven Central" integration test isn't practical here: it would need
  * network access and a version of SynapseML already published under the coordinate being tested, which
  * doesn't exist for an unmerged change (the same reason VerifyPackageUtils.scala only checks coordinate
  * *format*, not live resolution). Instead, the tests below regression-guard the documented opt-in
  * coordinate/exclusion syntax in ONNX.md itself, and a separate scratch sbt project (not committed) was
  * used to empirically confirm resolution behavior before writing that guidance -- see the PR description.
  */
class ONNXRuntimeDependencySuite extends AnyFunSuite {

  private val repoRoot: String = BuildInfo.baseDirectory.getParent
  private val buildSbt = FileUtilities.readFile(FileUtilities.join(repoRoot, "build.sbt"))
  private val onnxRuntimeDependencyScala = FileUtilities.readFile(
    FileUtilities.join(repoRoot, "project", "OnnxRuntimeDependency.scala"))
  private val onnxDoc = FileUtilities.readFile(
    FileUtilities.join(repoRoot, "docs", "Explore Algorithms", "Deep Learning", "ONNX.md"))

  private val deepLearningBlock: String = {
    val start = buildSbt.indexOf("lazy val deepLearning")
    require(start >= 0, "Could not find the deepLearning project definition in build.sbt")
    val nextProject = buildSbt.indexOf("\nlazy val ", start + 1)
    buildSbt.substring(start, if (nextProject >= 0) nextProject else buildSbt.length)
  }

  test("deep-learning declares the CPU-only, cross-platform onnxruntime artifact as its default dependency") {
    assert(deepLearningBlock.contains(""""com.microsoft.onnxruntime" % "onnxruntime""""),
      "build.sbt must declare com.microsoft.onnxruntime:onnxruntime (CPU-only, cross-platform, " +
        "including macOS) as the default deep-learning dependency so ONNXModel works out of the box " +
        "on macOS/Linux/Windows.")
  }

  test("deep-learning does not force the GPU-only onnxruntime_gpu artifact on every user by default") {
    assert(!deepLearningBlock.contains(""""onnxruntime_gpu""""),
      "onnxruntime_gpu (Linux/Windows-only CUDA build, ~300+MB) must not be a default/compile " +
        "dependency of synapseml-deep-learning; it should remain an explicit, documented opt-in " +
        "(see docs/Explore Algorithms/Deep Learning/ONNX.md) so it is never forced on macOS or " +
        "CPU-only users.")
  }

  test("the shared ONNX Runtime version is pinned at or above the confirmed GH2417 fix version") {
    val versionPattern = """val Version = "([\d.]+)"""".r
    val version = versionPattern.findFirstMatchIn(onnxRuntimeDependencyScala).map(_.group(1))
      .getOrElse(fail("Could not find `val Version = \"...\"` in project/OnnxRuntimeDependency.scala"))

    def parts(v: String): Seq[Int] = v.split("\\.").map(_.toInt)
    assert(Ordering.Iterable[Int].gteq(parts(version), parts("1.16.3")),
      s"Expected the ONNX Runtime version pinned in project/OnnxRuntimeDependency.scala ($version) " +
        s"to stay at or above 1.16.3, the smallest version confirmed to fix GH2417.")
  }

  test("ONNX.md documents the Spark --packages/--exclude-packages exclusion syntax for the GPU opt-in") {
    assert(onnxDoc.contains("--exclude-packages") && onnxDoc.contains("spark.jars.excludes"),
      "ONNX.md must document both the spark-submit CLI form (--packages/--exclude-packages) and the " +
        "equivalent Spark conf keys (spark.jars.packages/spark.jars.excludes) for excluding the " +
        "transitive CPU-only onnxruntime artifact when opting in to onnxruntime_gpu.")
    assert(onnxDoc.contains("com.microsoft.onnxruntime:onnxruntime_gpu"),
      "ONNX.md must name the exact onnxruntime_gpu Maven coordinate for the --packages/spark.jars." +
        "packages example.")
  }

  test("ONNX.md documents the Databricks Maven library Exclusions field on the correct entry") {
    assert(onnxDoc.contains("\"exclusions\""),
      "ONNX.md must show the Databricks Maven library JSON \"exclusions\" field for the GPU opt-in.")
    assert(onnxDoc.contains("Exclusions"),
      "ONNX.md must name the Databricks UI \"Exclusions\" field for the GPU opt-in.")
    // The exclusion must be documented on the SynapseML entry (the one that actually depends
    // transitively on the CPU-only onnxruntime), not on the onnxruntime_gpu entry -- an exclusion on
    // onnxruntime_gpu would be a no-op since it never depends on the plain onnxruntime artifact.
    assert(onnxDoc.contains("not on the onnxruntime_gpu entry") ||
      onnxDoc.contains("not on `onnxruntime_gpu`"),
      "ONNX.md must call out that the Databricks Exclusions field belongs on the SynapseML library " +
        "entry, not the onnxruntime_gpu entry, since Databricks resolves each Maven library's " +
        "dependency tree independently.")
  }

  test("ONNX.md states the exactly-one-ai.onnxruntime-jar invariant for the GPU opt-in") {
    assert(onnxDoc.contains("exactly one `ai.onnxruntime` jar"),
      "ONNX.md must state that exactly one ai.onnxruntime jar must be present after opting in to " +
        "onnxruntime_gpu, so users have a concrete way to verify their exclusion actually worked.")
  }
}
