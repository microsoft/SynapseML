// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.io.{FileOutputStream, ObjectOutputStream}
import java.util.UUID
import java.net.URI

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

abstract class CNTKCommandBuilderBase {
  val command: String
  def arguments(): Seq[String]
  val configs = ListBuffer.empty[BrainScriptConfig]

  var workingDir = new File(".").toURI

  def setWorkingDir(p: String): this.type = {
    workingDir = new File(p).toURI
    this
  }

  def insertBaseConfig(t: String): this.type = {
    configs.insert(0, BrainScriptConfig("baseConfig", Seq(t)))
    this
  }

  def appendOverrideConfig(t: Seq[String]): this.type = {
    configs.append(BrainScriptConfig("overrideConfig", t))
    this
  }

  protected def configToFile(c: BrainScriptConfig): String = {
    val outFile = new File(new File(workingDir).getAbsolutePath + s"/${c.name}.cntk")
    writeFile(outFile, c.text.mkString("\n"))
    println(s"wrote string to ${outFile.getName}")
    outFile.getAbsolutePath
  }

  def buildCommand(): String
}

class CNTKCommandBuilder(fileBased: Boolean = true) extends CNTKCommandBuilderBase {
  val command = "cntk"
  val arguments = Seq[String]()

   def buildCommand(): String = {
    val cntkArgs = configs
      .map(c => if (fileBased) s"configFile=${configToFile(c)} " else c.text.mkString(" "))
      .mkString(" ")

    command + " " + cntkArgs
  }
}

trait MPIConfiguration {
  val command = "mpiexec"
  // nodename -> workers per node
  def nodeConfig: Map[String, Int]
}

class MPICommandBuilder(fileBased: Boolean = true) extends CNTKCommandBuilderBase with MPIConfiguration {

  def nodeConfig: Map[String, Int] = Map("127.0.0.1" -> EnvironmentUtils.GPUCount.get)

  val argName = "-n"
  val arguments = Seq(argName, nodeConfig.head._2.toString)

  def buildCommand(): String = {
    val cntkArgs = "cntk " + configs
      .map(c => if (fileBased) s"configFile=${configToFile(c)} " else c.text.mkString(" "))
      .mkString(" ")

    Seq(command, arguments.mkString(" "), cntkArgs, "parallelTrain=true").mkString(" ")
  }
}

class MultiNodeParallelLauncher(fileBased: Boolean = false) extends CNTKCommandBuilderBase with MPIConfiguration {

  // The difference here is the requirement of locating
  // and passing on the hosts information
  val nodeConfig = Map("localhost" -> 1, "remotehost" -> 1)
  val arguments = if (EnvironmentUtils.IsWindows) {
    Seq("--hosts", nodeConfig.size.toString) ++ nodeConfig.map { case(name, num) => s"$name $num" }
  } else {
    val hostFile = new File(".", "hostfile.txt")
    val txt = nodeConfig.map { case(name, num) => s"$name slots=$num" }.mkString("\n")
    writeFile(hostFile, txt)
    Seq("-hostfile", hostFile.getCanonicalPath)
  }

  def buildCommand(): String = {
    val cntkArgs = configs
      .map(c => if (fileBased) s"configFile=${configToFile(c)} " else c.text.mkString(" "))
      .mkString(" ")

    Seq(command, arguments.mkString(" "), cntkArgs).mkString(" ")
  }

}
