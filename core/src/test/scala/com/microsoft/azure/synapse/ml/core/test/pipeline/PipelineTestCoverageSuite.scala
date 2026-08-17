// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.test.pipeline

import com.microsoft.azure.synapse.ml.build.BuildInfo
import com.microsoft.azure.synapse.ml.core.env.StreamUtilities.usingSource
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import scala.annotation.tailrec
import scala.io.Source

/**
  * Guards against test suites that silently never run in CI.
  *
  * The UnitTests matrix runs `testOnly com.microsoft.azure.synapse.ml.$PACKAGE.**` unless a leg
  * overrides it with an explicit TEST_CLASSES list. A suite in a package no leg names is not
  * reported as skipped -- it simply never executes, and the build stays green.
  */
class PipelineTestCoverageSuite extends AnyFunSuite {

  private val rootPackage = "com.microsoft.azure.synapse.ml"

  /** Suite name prefixes launched by their own pipeline stages rather than the UnitTests matrix. */
  private val dedicatedStageSuites = Seq(
    "nbtest.DatabricksCPUTests",
    "nbtest.DatabricksGPUTests",
    "nbtest.DatabricksRapidsTests",
    "nbtest.SynapseTests",
    "nbtest.FabricNotebookTests",
    "nbtest.FabricSmokeTests",
    "nbtest.FabricTestCleanup",
    "nbtest.SynapseTestCleanup"
  ).map(suffix => s"$rootPackage.$suffix")

  /**
    * Suites that deliberately have no pipeline leg yet, listed here so the guard stays green
    * without pretending they are covered.
    *
    * DatabricksCPUStreamingTests drives the "Deploying a Classifier" notebook, which
    * DatabricksUtilities keeps out of the parallel CPU partitions because its server.stop()
    * cancels every SparkContext job on Spark 4 and would kill notebooks sharing the cluster.
    * Giving it a matrix leg of its own was tried and does not work yet: the extra concurrent
    * cluster exhausts the instance pool so its libraries never finish installing, and the
    * notebook itself still errors. Scheduling it needs pool capacity plus a notebook fix,
    * neither of which belongs in a branch sync.
    */
  private val unscheduledSuites = Seq(
    "nbtest.DatabricksCPUStreamingTests"
  ).map(suffix => s"$rootPackage.$suffix")

  /**
    * ScalaTest entry points. Anything extending these, directly or transitively, is a suite.
    * The `*Like` traits are separate entry points, not subtypes of the classes, so both spellings
    * are needed -- `AnalyzeTextLROSuite` extends `AnyFunSuiteLike` directly.
    */
  private val scalaTestBaseTypes: Set[String] = {
    val styles = Set("Suite", "FunSuite", "FlatSpec", "WordSpec", "FreeSpec", "PropSpec", "FeatureSpec", "FunSpec")
    val spellings = styles ++ styles.map("Any" + _)
    spellings ++ spellings.map(_ + "Like") + "TestSuite" + "TestSuiteLike"
  }

  private val repoRoot: File = BuildInfo.baseDirectory.getParentFile

  private def readFile(file: File): String =
    usingSource(Source.fromFile(file, "UTF-8"))(_.mkString).get

  private def scalaFilesUnder(dir: File): Seq[File] = {
    if (!dir.isDirectory) Seq.empty
    else Option(dir.listFiles()).toSeq.flatten.flatMap { child =>
      if (child.isDirectory) scalaFilesUnder(child)
      else if (child.getName.endsWith(".scala")) Seq(child)
      else Seq.empty
    }
  }

  private def testSourceFiles: Seq[File] =
    Option(repoRoot.listFiles()).toSeq.flatten
      .filter(_.isDirectory)
      .flatMap(module => scalaFilesUnder(new File(module, "src/test/scala")))

  private case class Declaration(name: String, parents: Seq[String], isConcreteClass: Boolean)

  /**
    * Declarations routinely wrap, e.g. `class Foo(bar: String)\n  extends TestBase`, so match
    * against a whitespace-flattened copy of the source. The segment between the name and
    * `extends` may not cross another declaration keyword, which keeps a class without an
    * `extends` clause from borrowing the next declaration's parents.
    */
  private val declarationPattern =
    raw"(?<![\w.])(abstract class|case class|class|trait) (\w+)\b" +
      raw"((?:(?!\b(?:class|trait|object)\b).)*?) extends ([^\{]*?)" +
      raw"(?=\s*\{|\s+(?:abstract class|case class|class|trait|object)\b|$$)"

  private def parseDeclarations(source: String): Seq[Declaration] = {
    val flattened = source.replaceAll("\\s+", " ")
    declarationPattern.r.findAllMatchIn(flattened).map { m =>
      val parents = m.group(4)
        .split("\\bwith\\b")
        .map(_.trim.takeWhile(c => c.isLetterOrDigit || c == '.' || c == '_'))
        .map(name => name.split('.').lastOption.getOrElse(name))
        .filter(_.nonEmpty)
        .toSeq
      Declaration(m.group(2), parents, m.group(1) == "class" || m.group(1) == "case class")
    }.toSeq
  }

  /** Walks the local extends graph so suites are found by ancestry, not by naming convention. */
  private def suiteTypeNames(declarations: Seq[Declaration]): Set[String] = {
    val parentsByName = declarations.groupBy(_.name).map {
      case (name, decls) => name -> decls.flatMap(_.parents).toSet
    }
    @tailrec
    def expand(known: Set[String]): Set[String] = {
      val grown = known ++ parentsByName.collect { case (name, parents) if parents.exists(known) => name }
      if (grown.size == known.size) known else expand(grown)
    }
    expand(scalaTestBaseTypes)
  }

  private def matrixSpecs(pipeline: String): Seq[String] = {
    val unitTestsJob = pipeline.split(raw"(?m)^- job: ")
      .find(_.startsWith("UnitTests"))
      .getOrElse(fail("Could not locate the UnitTests job in pipeline.yaml"))
    val matrixBlock = unitTestsJob.split(raw"(?m)^  steps:").head

    // Anchor each package segment so the trailing ".**" is not swallowed by the segment matcher.
    val fromTestClasses = raw"$rootPackage(?:\.\w+)*(?:\.\*\*)?".r.findAllIn(matrixBlock).toSeq
    // A leg with TEST_CLASSES ignores its PACKAGE, so a package glob only counts when that leg
    // does not also pin an explicit class list.
    val fromPackages = matrixBlock.split(raw"(?m)^      \w+:")
      .filterNot(_.contains("TEST_CLASSES:"))
      .flatMap { leg =>
        raw"""PACKAGE:\s*"([\w.]+)"""".r.findAllMatchIn(leg).map(m => s"$rootPackage.${m.group(1)}.**")
      }.toSeq

    (fromTestClasses ++ fromPackages).distinct
  }

  private def isCovered(fqcn: String, specs: Seq[String]): Boolean =
    specs.exists { spec =>
      if (spec.endsWith(".**")) fqcn.startsWith(spec.dropRight(2))
      else fqcn == spec
    }

  test("Every test suite is claimed by a leg of the UnitTests matrix") {
    val pipelineFile = new File(repoRoot, "pipeline.yaml")
    assert(pipelineFile.exists(), s"pipeline.yaml not found at ${pipelineFile.getAbsolutePath}")
    val specs = matrixSpecs(readFile(pipelineFile))
    assert(specs.nonEmpty, "Parsed no test specs out of the UnitTests matrix")

    val sources = testSourceFiles.map(file => file -> readFile(file))
    assert(sources.size > 100, s"Expected to scan the whole repo, only found ${sources.size} test files")

    val parsed = sources.map { case (file, source) =>
      val pkg = raw"(?m)^package\s+([\w.]+)".r.findFirstMatchIn(source).map(_.group(1))
      (file, pkg, parseDeclarations(source))
    }
    val suiteTypes = suiteTypeNames(parsed.flatMap { case (_, _, decls) => decls })

    val discovered = parsed.flatMap { case (file, pkg, decls) =>
      pkg.toSeq.flatMap { p =>
        decls
          .filter(decl => decl.isConcreteClass && decl.parents.exists(suiteTypes))
          .map(decl => s"$p.${decl.name}" -> file.getName)
      }
    }.distinct

    // Without this the guard would pass vacuously if source parsing ever silently broke.
    assert(discovered.size > 200, s"Only discovered ${discovered.size} suites; source parsing looks broken")
    Seq(
      "com.microsoft.azure.synapse.ml.services.language.AnalyzeTextLROSuite", // wraps, extends AnyFunSuiteLike
      "com.microsoft.azure.synapse.ml.stages.EnsembleByKeySuite",
      "com.microsoft.azure.synapse.ml.services.search.AzureSearchAuthSuite"
    ).foreach { known =>
      assert(discovered.exists(_._1 == known), s"Suite discovery missed $known")
    }

    val orphans = discovered
      .filterNot { case (fqcn, _) => dedicatedStageSuites.exists(fqcn.startsWith) }
      .filterNot { case (fqcn, _) => unscheduledSuites.exists(fqcn.startsWith) }
      .filterNot { case (fqcn, _) => isCovered(fqcn, specs) }
      .map { case (fqcn, fileName) => s"$fqcn ($fileName)" }
      .sorted

    assert(orphans.isEmpty,
      s"${orphans.size} test suite(s) are never run by CI. Add them to the UnitTests matrix in " +
        s"pipeline.yaml:\n  ${orphans.mkString("\n  ")}")
  }
}
