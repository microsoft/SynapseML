// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.codegen

import java.io.File

import com.microsoft.ml.spark.codegen.Config._
import com.microsoft.ml.spark.codegen.DocGen._
import com.microsoft.ml.spark.codegen.WrapperClassDoc._
import com.microsoft.ml.spark.core.env.FileUtilities._
import org.apache.commons.io.FileUtils

object CodeGen {

  def generateArtifacts(): Unit = {
    println(
      s"""|Running code generation with config:
          |  topDir:    $topDir
          |  packageDir: $packageDir
          |  pySrcDir:  $pySrcDir
          |  pyTestDir: $pyTestDir
          |  pyDocDir:  $pyDocDir
          |  rsrcDir:   $rSrcDir
          |  tmpDocDir: $tmpDocDir """.stripMargin)

    val roots = List("src")
    println("Creating temp folders")
    if (generatedDir.exists()) FileUtils.forceDelete(generatedDir)

    println("Generating python APIs")
    PySparkWrapperGenerator()
    println("Generating R APIs")
    SparklyRWrapperGenerator()
    println("Generating .rst files for the Python APIs documentation")
    genRstFiles()

    def toDir(f: File): File = new File(f, File.separator)

    writeFile(new File(pySrcDir, "__init__.py"), packageHelp)
    FileUtils.copyDirectoryToDirectory(toDir(pySrcOverrideDir), toDir(pySrcDir))
    FileUtils.copyDirectoryToDirectory(toDir(pyTestOverrideDir), toDir(pyTestDir))

    // build init file
    // package python+r zip files
    // zipFolder(pyDir, pyZipFile)
    rPackageDir.mkdirs()
    zipFolder(rSrcDir, rPackageFile)

    //FileUtils.forceDelete(rDir)
    // leave the python source files, so they will be included in the super-jar
    // FileUtils.forceDelete(pyDir)
    // delete the text files with the Python Class descriptions - truly temporary
    // FileUtils.forceDelete(tmpDocDir)
  }

  def main(args: Array[String]): Unit = {
    org.apache.log4j.BasicConfigurator.configure(new org.apache.log4j.varia.NullAppender())
    generateArtifacts()
  }

}
