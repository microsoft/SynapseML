// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.io.{FileOutputStream, ObjectOutputStream}
import java.util.UUID

import scala.collection.mutable.ListBuffer
import scala.sys.process._

import com.microsoft.ml.spark.schema._
import FileUtilities._

import org.apache.hadoop.fs.Path

import org.apache.spark.ml.classification._
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.linalg.{DenseVector, SparseVector}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.{Identifiable, MLWritable, MLWriter}
import org.apache.spark.ml._

import org.apache.spark.sql._
import org.apache.spark.sql.types._

// Don't get too excited AK. This is starting to look like a set of contracts..
case class InputShape(dim: Int, form: String)
case class InputData(format: String, path: String, shapes: Map[String, InputShape])
case class BrainScriptConfig(name: String, text: Seq[String])

// It would be nice to extend from Params for this, but this
// seems more useful than just Spark, so not doing it for now
class BrainScriptBuilder {

  // We need to know a few things:
  // 1. Where is the input data?
  // 2. How do we configure the training itself?
  // 3. Where should we put the outputs?
  var modelName = "ModelOut"

  var inData: Option[InputData] = None

  var rootDir: String = ""
  var outDir: String = ""
  var weightPrecision: String = "float"

  var commands = ListBuffer[String]("trainNetwork")
  var testModel = false

  def setInputFile(path: String, format: String, shapes: Map[String, InputShape]): this.type = {
    inData = Some(InputData(format, path, shapes))
    this
  }

  def setModelName(n: String): this.type = {
    modelName = n
    this
  }

  def getModelPath(): String = {
    s"""$outDir/Models/$modelName"""
  }

  def setRootDir(p: String): this.type = {
    outDir = p
    this
  }

  def setOutputRoot(p: String): this.type = {
    outDir = p
    this
  }

  private def getInputString(): String = {
    val ips = inData.get.shapes
                .map { case(name, shape) => name + " = [ dim = " +
                       shape.dim.toString + " ; format = \"" + shape.form + "\" ]" }
                .mkString("; ")
    s"input = [ $ips ]"
  }

  def setCommands(c: String*): this.type = {
    this
  }

  def setTestModel(b: Boolean): this.type = {
    if (!testModel && b) {
      commands.append("testNetwork")
    }
    this
  }

  def toReaderConfig(): String = {
    val ipstring = getInputString()
    val loc = inData.get.path
    val form = inData.get.format match {
      case "text" => "CNTKTextFormatReader"
    }
    s"""reader = [ readerType = $form ; file = "$loc" ; $ipstring ]"""
  }

  def toOverrideConfig(): Seq[String] = {
    val rootOverrides = Seq(
      s"""command = ${ commands.mkString(":") }""",
      s"precision=$weightPrecision",
      "traceLevel=1",
      "deviceId=\"auto\"",
      s"""rootDir="$rootDir" """,
      s"""outputDir="$outDir" """,
      s"""modelPath="${getModelPath}" """)
    val commandReaders = commands.map(c => s"$c = [ ${toReaderConfig()} ]")

    rootOverrides ++ commandReaders
  }

}
