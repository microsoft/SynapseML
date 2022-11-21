// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.nbtest

import com.microsoft.azure.synapse.ml.build.BuildInfo
import com.microsoft.azure.synapse.ml.core.env.FileUtilities
import org.apache.commons.io.FileUtils

import java.io.File
import java.lang.ProcessBuilder.Redirect
import scala.sys.process._


object SharedNotebookE2ETestUtilities {
  val ResourcesDirectory = new File(getClass.getResource("/").toURI)
  val NotebooksDir = new File(ResourcesDirectory, "generated-notebooks")

  def generateNotebooks(): Unit = {
    cleanUpGeneratedNotebooksDir()

    FileUtilities.recursiveListFiles(FileUtilities
      .join(BuildInfo.baseDirectory.getParent, "notebooks/features")
      .getCanonicalFile)
      .filter(_.getName.endsWith(".ipynb"))
      .map { f =>
        FileUtilities.copyFile(f, NotebooksDir, true)
        val newFile = new File(NotebooksDir, f.getName)
        val targetName = new File(NotebooksDir, f.getName.replace(" ", "").replace("-", ""))
        newFile.renameTo(targetName)
        targetName
      }

    runCmd(activateCondaEnv ++ Seq("jupyter", "nbconvert", "--to", "python", "*.ipynb"), NotebooksDir)
  }

  def cleanUpGeneratedNotebooksDir(): Unit = {
    FileUtils.deleteDirectory(NotebooksDir)
    assert(NotebooksDir.mkdirs())
  }

  def isWindows: Boolean = {
    sys.props("os.name").toLowerCase.contains("windows")
  }

  def osPrefix: Seq[String] = {
    if (isWindows) {
      Seq("cmd", "/C")
    } else {
      Seq()
    }
  }

  def runCmd(cmd: Seq[String],
             wd: File = new File("."),
             envVars: Map[String, String] = Map()): Unit = {
    val pb = new java.lang.ProcessBuilder()
      .directory(wd)
      .command(cmd: _*)
      .redirectError(Redirect.INHERIT)
      .redirectOutput(Redirect.INHERIT)
    val env = pb.environment()
    envVars.foreach(p => env.put(p._1, p._2))
    assert(pb.start().waitFor() == 0)
  }

  def condaEnvName: String = "synapseml"

  def activateCondaEnv: Seq[String] = {
    if (sys.props("os.name").toLowerCase.contains("windows")) {
      osPrefix ++ Seq("activate", condaEnvName, "&&")
    } else {
      Seq()
      //TODO figure out why this doesent work
      //Seq("/bin/bash", "-l", "-c", "source activate " + condaEnvName, "&&")
    }
  }


  def exec(command: String): String = {
    val os = sys.props("os.name").toLowerCase
    os match {
      case x if x contains "windows" => Seq("cmd", "/C") ++ Seq(command) !!
      case _ => command.!!
    }
  }
}
