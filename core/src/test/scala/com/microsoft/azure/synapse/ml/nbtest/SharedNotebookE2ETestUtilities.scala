// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.nbtest

import com.microsoft.azure.synapse.ml.build.BuildInfo
import com.microsoft.azure.synapse.ml.core.env.{FileUtilities, StreamUtilities}
import org.apache.commons.io.FileUtils

import scala.concurrent.{Future, Await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import java.io.File
import java.lang.ProcessBuilder.Redirect
import scala.sys.process._
import scala.io.Source
import java.io.{BufferedWriter, File, FileWriter}

object SharedNotebookE2ETestUtilities {
  val ResourcesDirectory = new File(getClass.getResource("/").toURI)
  val NotebooksDir = new File(ResourcesDirectory, "generated-notebooks")
  val NotebookPreamble: String =
    """
      |# In[ ]:
      |
      |
      |# This cell ensures make magic command like '%pip install' works on synapse scheduled spark jobs
      |from synapse.ml.core.platform import running_on_synapse
      |
      |if running_on_synapse():
      |    from IPython import get_ipython
      |    from IPython.terminal.interactiveshell import TerminalInteractiveShell
      |    from synapse.ml.core.platform import materializing_display as display
      |
      |    try:
      |        shell = TerminalInteractiveShell.instance()
      |    except:
      |        pass
      |
      |# Always ensure spark session exists (needed for Fabric SJDs and local runs)
      |from pyspark.sql import SparkSession
      |spark = SparkSession.builder.getOrCreate()
      |
      |# Provide display function if not already defined (e.g. on Fabric SJDs)
      |if 'display' not in dir():
      |    def display(df):
      |        df.show()
      |
      |""".stripMargin

  val OnePlusOneNotebook =
    """
    |{
    |    "cells": [
    |        {
    |            "cell_type": "code",
    |            "execution_count": null,
    |            "id": "de5aebcf",
    |            "metadata": {},
    |            "outputs": [],
    |            "source": [
    |                "1+1"
    |            ]
    |        }
    |    ],
    |    "metadata": {
    |        "language_info": {
    |            "name": "python"
    |        }
    |    },
    |    "nbformat": 4,
    |    "nbformat_minor": 5
    |}
    |""".stripMargin

  def insertTextInFile(file: File, textToPrepend: String, locToInsert: Int): Unit = {
    val existingLines = StreamUtilities.using(Source.fromFile(file)) { s =>
      s.getLines().toList
    }.get
    val linesBefore = existingLines.take(locToInsert)
    val linesAfter = existingLines.takeRight(existingLines.length - locToInsert)
    val linesInMiddle = textToPrepend.split("\n")
    val newText = (linesBefore ++ linesInMiddle ++ linesAfter).mkString("\n")
    StreamUtilities.using(new BufferedWriter(new FileWriter(file))) { writer =>
      writer.write(newText)
    }
  }

  def generateNotebooks(): Array[File] = {
    println("Cleaning up generated notebooks directory...")
    cleanUpGeneratedNotebooksDir()

    println("Generating new notebooks...")
    val docsDir = FileUtilities.join(BuildInfo.baseDirectory.getParent, "docs").getCanonicalFile
    val newFiles = FileUtilities.recursiveListFiles(docsDir)
      .filter(_.getName.endsWith(".ipynb"))
      .map { f =>
        val relative = docsDir.toURI.relativize(f.toURI).getPath
        val newName = relative
          .replace("/", "")
          .replace(" ", "")
          .replace("-", "")
          .replace(",", "")
        FileUtilities.copyAndRenameFile(f, NotebooksDir, newName, true)
        new File(NotebooksDir, newName)
      } :+ {
        val onePlusOne = new File(NotebooksDir, "OnePlusOne.ipynb")
        FileUtilities.writeFile(onePlusOne, OnePlusOneNotebook)
        onePlusOne
      }

    // Parallelize nbconvert for each notebook file
    val conversions = newFiles.toSeq.map { f =>
      Future {
        runCmd(
          activateCondaEnv ++ Seq("jupyter", "nbconvert", "--to", "python", "--log-level=ERROR", f.getName),
          NotebooksDir)
      }
    }
    // Wait for all conversions to finish
    Await.result(Future.sequence(conversions), Duration.Inf)
    println(s"Generated ${newFiles.length} Python files from notebooks.")

    newFiles.map { f =>
      val newFile = new File(f.getPath.replace(".ipynb", ".py"))
      insertTextInFile(newFile, NotebookPreamble, 2)
      newFile
    }
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

  private[ml] def exec(command: String, maxRetries: Int = 0, attempt: Int = 0): String = {
    val osCommand = sys.props("os.name").toLowerCase match {
      case x if x contains "windows" => Seq("cmd", "/C") ++ Seq(command)
      case _ => Seq("bash", "-c", command)
    }

    try {
      osCommand.!!
    } catch {
      case e: RuntimeException if attempt < maxRetries =>
        println(s"Retrying after error: $e")
        Thread.sleep(1000)
        exec(command, maxRetries, attempt + 1)
    }
  }

}
