// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.codegen

import java.io.File

import com.microsoft.ml.spark.codegen.Config._
import com.microsoft.ml.spark.core.env.FileUtilities._
import org.apache.commons.io.FileUtils
import org.apache.commons.io.FilenameUtils._

object CodeGen {

  def generateArtifacts(): Unit = {
    println(
      s"""|Running code generation with config:
          |  topDir:     $TopDir
          |  packageDir: $PackageDir
          |  pySrcDir:   $PySrcDir
          |  pyTestDir:  $PyTestDir
          |  rsrcDir:    $RSrcDir""".stripMargin)

    println("Creating temp folders")
    if (GeneratedDir.exists()) FileUtils.forceDelete(GeneratedDir)

    println("Generating python APIs")
    PySparkWrapperGenerator()
    println("Generating R APIs")
    SparklyRWrapperGenerator(Version)

    def toDir(f: File): File = new File(f, File.separator)

    //writeFile(new File(pySrcDir, "__init__.py"), packageHelp(""))
    FileUtils.copyDirectoryToDirectory(toDir(PySrcOverrideDir), toDir(PySrcDir))
    FileUtils.copyDirectoryToDirectory(toDir(PyTestOverrideDir), toDir(PyTestDir))
    makeInitFiles()

    // build init file
    // package python+r zip files
    // zipFolder(pyDir, pyZipFile)
    RPackageDir.mkdirs()
    zipFolder(RSrcDir, new File(RPackageDir, s"mmlspark-$Version.zip"))

    //FileUtils.forceDelete(rDir)
    // leave the python source files, so they will be included in the super-jar
    // FileUtils.forceDelete(pyDir)
  }

  private def makeInitFiles(packageFolder: String = ""): Unit = {
    val dir = new File(new File(PySrcDir,"mmlspark"), packageFolder)
    val packageString = if (packageFolder != "") packageFolder.replace("/",".") else ""
    val importStrings =
      dir.listFiles.filter(_.isFile).sorted
        .map(_.getName)
        .filter(name => name.endsWith(".py") && !name.startsWith("_") && !name.startsWith("test"))
        .map(name => s"from mmlspark$packageString.${getBaseName(name)} import *\n").mkString("")
    writeFile(new File(dir, "__init__.py"), packageHelp(importStrings))
    dir.listFiles().filter(_.isDirectory).foreach(f =>
      makeInitFiles(packageFolder +"/" + f.getName)
    )
  }

  def main(args: Array[String]): Unit = {
    generateArtifacts()
  }

}
