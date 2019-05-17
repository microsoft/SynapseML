// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.codegen

import Config._
import DocGen._
import WrapperClassDoc._

import scala.util.matching.Regex
import java.util.regex.Pattern
import com.microsoft.ml.spark.core.env.FileUtilities._
import org.apache.commons.io.FilenameUtils._
import org.apache.commons.io.FileUtils

object CodeGen {

  def generateArtifacts(): Unit = {
    println(
      s"""|Running code generation with config:
          |  topDir:    $topDir
          |  outputDir: $outputDir
          |  pyDir:     $pyDir
          |  pyTestDir: $pyTestDir
          |  pyDocDir:  $pyDocDir
          |  rDir:      $rDir
          |  rsrcDir:   $rSrcDir
          |  tmpDocDir: $tmpDocDir""".stripMargin)
    val roots = List("src")
    println("Creating temp folders")
    FileUtils.forceDelete(artifactsDir)
    FileUtils.forceDelete(testResultsDir)
    pySdkDir.mkdirs
    pyDir.mkdirs
    pyTestDir.mkdirs
    pyDocDir.mkdirs
    tmpDocDir.mkdirs
    rSrcDir.mkdirs
    rSdkDir.mkdirs

    println("Generating python APIs")
    PySparkWrapperGenerator()
    println("Generating R APIs")
    SparklyRWrapperGenerator()
    println("Generating .rst files for the Python APIs documentation")
    genRstFiles()

    FileUtils.copyDirectoryToDirectory(pySourcePath, pyDir.getParentFile)

    // build init file
    writeFile(new File(pyDir, "__init__.py"), packageHelp)

    // package python+r zip files
    zipFolder(pyDir, pyZipFile)
    zipFolder(rDir, rZipFile)

    FileUtils.forceDelete(rDir)
    // leave the python source files, so they will be included in the super-jar
    // FileUtils.forceDelete(pyDir)
    // delete the text files with the Python Class descriptions - truly temporary
    FileUtils.forceDelete(tmpDocDir)
  }

  def main(args: Array[String]): Unit = {
    org.apache.log4j.BasicConfigurator.configure(new org.apache.log4j.varia.NullAppender())
    generateArtifacts()
  }

}
