// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.codegen

import java.io.File

import com.microsoft.ml.spark.codegen.Config._
import com.microsoft.ml.spark.core.env.FileUtilities._
import com.microsoft.ml.spark.core.test.base.TestBase
import com.microsoft.ml.spark.core.test.fuzzing.PyTestFuzzing
import com.microsoft.ml.spark.core.utils.JarLoadingUtils.loadTestClass
import org.apache.commons.io.FileUtils
import org.apache.commons.io.FilenameUtils._

object CodeGen {

  def clean(dir: File): Unit = if (dir.exists()) FileUtils.forceDelete(dir)

  def toDir(f: File): File = new File(f, File.separator)

  def generateArtifacts(): Unit = {
    println(
      s"""|Running code generation with config:
          |  topDir:     $TopDir
          |  packageDir: $PackageDir
          |  pySrcDir:   $PySrcDir
          |  pyTestDir:  $PyTestDir
          |  rsrcDir:    $RSrcDir""".stripMargin)

    println("Creating temp folders")
    clean(PackageDir)
    clean(PySrcDir)
    clean(RSrcDir)

    println("Generating python APIs")
    PySparkWrapperGenerator()
    println("Generating R APIs")
    SparklyRWrapperGenerator(Version)

    FileUtils.copyDirectoryToDirectory(toDir(PySrcOverrideDir), toDir(PySrcDir))

    // add init files
    makeInitFiles()

    // package python+r zip files
    RPackageDir.mkdirs()
    zipFolder(RSrcDir, new File(RPackageDir, s"mmlspark-$Version.zip"))

  }


  private def makeInitFiles(packageFolder: String = ""): Unit = {
    val dir = new File(new File(PySrcDir, "mmlspark"), packageFolder)
    val packageString = if (packageFolder != "") packageFolder.replace("/", ".") else ""
    val importStrings =
      dir.listFiles.filter(_.isFile).sorted
        .map(_.getName)
        .filter(name => name.endsWith(".py") && !name.startsWith("_") && !name.startsWith("test"))
        .map(name => s"from mmlspark$packageString.${getBaseName(name)} import *\n").mkString("")
    writeFile(new File(dir, "__init__.py"), packageHelp(importStrings))
    dir.listFiles().filter(_.isDirectory).foreach(f =>
      makeInitFiles(packageFolder + "/" + f.getName)
    )
  }

  def main(args: Array[String]): Unit = {
    generateArtifacts()
  }

}

object TestGen {
  import CodeGen.{toDir, clean}

  def generatePythonTests(): Unit = {
    loadTestClass[PyTestFuzzing[_]](false).foreach { ltc =>
      try {
        ltc.makePyTestFile()
      } catch {
        case _: NotImplementedError =>
          println(s"ERROR: Could not generate test for ${ltc.testClassName} because of Complex Parameters")
      }
    }
  }

  private def makeInitFiles(packageFolder: String = ""): Unit = {
    val dir = new File(new File(PyTestDir, "mmlsparktest"), packageFolder)
    writeFile(new File(dir, "__init__.py"), "")
    dir.listFiles().filter(_.isDirectory).foreach(f =>
      makeInitFiles(packageFolder + "/" + f.getName)
    )
  }

  def main(args: Array[String]): Unit = {
    clean(TestDataDir)
    clean(PyTestDir)
    generatePythonTests()
    TestBase.stopSparkSession()
    FileUtils.copyDirectoryToDirectory(toDir(PyTestOverrideDir), toDir(PyTestDir))
    makeInitFiles()
  }
}
