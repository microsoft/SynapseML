// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.test.pipeline

import com.microsoft.azure.synapse.ml.build.BuildInfo
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import scala.annotation.tailrec
import scala.io.Source
import scala.util.Using

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

  /** ScalaTest entry points. Anything extending these, directly or transitively, is a suite. */
  private val scalaTestBaseTypes = Set(
    "Suite", "FunSuite", "FlatSpec", "WordSpec", "FreeSpec", "PropSpec", "FeatureSpec", "FunSpec",
    "AnyFunSuite", "AnyFlatSpec", "AnyWordSpec", "AnyFreeSpec", "AnyPropSpec", "AnyFeatureSpec", "AnyFunSpec"
  )

  private val repoRoot: File = BuildInfo.baseDirectory.getParentFile

  private def readFile(file: File): String =
    Using.resource(Source.fromFile(file, "UTF-8"))(_.mkString)

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

  private val declarationPattern =
    raw"(?m)^\s*(abstract class|case class|class|trait)\s+(\w+)[^\n]*?\bextends\s+([^\{]+)".r

  private def parseDeclarations(source: String): Seq[Declaration] =
    declarationPattern.findAllMatchIn(source).map { m =>
      val parents = m.group(3)
        .split("\\bwith\\b")
        .map(_.trim.takeWhile(c => c.isLetterOrDigit || c == '.' || c == '_'))
        .map(name => name.split('.').lastOption.getOrElse(name))
        .filter(_.nonEmpty)
        .toSeq
      Declaration(m.group(2), parents, m.group(1) == "class" || m.group(1) == "case class")
    }.toSeq

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

    val suiteTypes = suiteTypeNames(sources.flatMap { case (_, source) => parseDeclarations(source) })

    val orphans = sources.flatMap { case (file, source) =>
      raw"(?m)^package\s+([\w.]+)".r.findFirstMatchIn(source).map(_.group(1)).toSeq.flatMap { pkg =>
        parseDeclarations(source)
          .filter(decl => decl.isConcreteClass && decl.parents.exists(suiteTypes))
          .map(decl => s"$pkg.${decl.name}")
          .filterNot(fqcn => dedicatedStageSuites.exists(fqcn.startsWith))
          .filterNot(isCovered(_, specs))
          .map(fqcn => s"$fqcn (${file.getName})")
      }
    }.distinct.sorted

    assert(orphans.isEmpty,
      s"${orphans.size} test suite(s) are never run by CI. Add them to the UnitTests matrix in " +
        s"pipeline.yaml:\n  ${orphans.mkString("\n  ")}")
  }
}
