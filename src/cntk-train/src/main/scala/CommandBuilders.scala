// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.net.URI

import scala.collection.mutable.ListBuffer
import scala.sys.process._

import com.microsoft.ml.spark.schema._
import FileUtilities._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.slf4j.Logger

import scala.sys.process._

abstract class CNTKCommandBuilderBase(log: Logger) {
  val command: String
  def arguments(): Seq[String]
  val configs = ListBuffer.empty[BrainScriptConfig]

  var workingDir = new File(".").toURI
  var outputDir: String = ""
  var sparkSession: SparkSession = null

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

  def setOutputDir(p: String): this.type = {
    outputDir = p
    this
  }

  def setSparkSession(p: SparkSession): this.type = {
    sparkSession = p
    this
  }

  protected def configToFile(c: BrainScriptConfig): String = {
    val outFolder = new File(workingDir)
    val outFile = new File(s"${outFolder.getAbsolutePath}/${c.name}.cntk")
    if (!outFolder.exists()) {
      outFolder.mkdirs()
    }
    writeFile(outFile, c.text.mkString("\n"))

    log.info(s"wrote string to ${outFile.getName}")
    outFile.getAbsolutePath
  }

  def runCommand(): Unit

  protected def printOutput(command: ProcessBuilder): Unit = {
    var result = ProcessUtils.getProcessOutput(log, command)
    log.info(s"Command succeeded with output: $result")
  }
}

class CNTKCommandBuilder(log: Logger, fileBased: Boolean = true) extends CNTKCommandBuilderBase(log) {
  val command = "cntk"
  val arguments = Seq[String]()

   def runCommand(): Unit = {
     val cntkArgs = configs
       .map(c => if (fileBased) s"configFile=${configToFile(c)} " else c.text.mkString(" "))
       .mkString(" ")
     printOutput(command + " " + cntkArgs)
  }
}

trait MPIConfiguration {
  val command = "mpirun"
  // nodename -> workers per node
  def nodeConfig: Map[String, Int]
}

class MPICommandBuilder(log: Logger,
                        gpuMachines: Array[String],
                        hdfsPath: Option[(String, String, String)],
                        mountedInputPath: String,
                        fileBased: Boolean = true) extends CNTKCommandBuilderBase(log) with MPIConfiguration {
  private val defaultNumGPUs = 1

  def nodeConfig: Map[String, Int] = gpuMachines.map(value => {
    val nodeAndGpus = value.split(",")
    (nodeAndGpus(0),
      if (nodeAndGpus.length == 2)
        try {
          nodeAndGpus(1).toInt
        } catch {
          case ex: Exception => defaultNumGPUs
        }
      else defaultNumGPUs)
  }).toMap

  val argName = "-n"
  val arguments = Seq(argName, nodeConfig.head._2.toString)

  def runCommand(): Unit = {
    val pathEnvVar = "PATH=/usr/bin/cntk/cntk/bin:$PATH"
    val ldLibraryPathEnvVar =
      "LD_LIBRARY_PATH=/usr/bin/cntk/cntk/lib:/usr/bin/cntk/cntk/dependencies/lib:$LD_LIBRARY_PATH"
    val mpiArgs = s" -x $pathEnvVar -x $ldLibraryPathEnvVar --npernode 1 "
    val cntkArgs = "cntk " + configs
      .map(c => if (fileBased) s"configFile=${configToFile(c)} " else c.text.mkString(" "))
      .mkString(" ")

    val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
    val inputPath = new Path("wasb:///MML-GPU/identity")
    // Get the user's home directory
    val userHomePath = System.getProperty("user.home")
    val identityDir = new File(new Path(userHomePath, ".ssh/MML-GPU").toString)
    if (!identityDir.exists()) {
      log.info(s"Creating directory $identityDir")
      identityDir.mkdirs()
    }
    val identity = s"${identityDir.getAbsolutePath}/identity"
    val identityPath = new Path(identity)
    val outputPath = new Path(s"file:///${identityDir.getAbsolutePath}/identity")
    // Copy from wasb to local file
    using(Seq(inputPath.getFileSystem(hadoopConf))) { fs =>
      log.info(s"Copying from wasb: $inputPath to local: $identityPath")
      fs(0).copyToLocalFile(inputPath, identityPath)
      fs(0).setOwner(outputPath, System.getProperty("user.name"), null)
    }.get
    var localDir = workingDir.toString.replaceFirst("file:/+", "/")
    val modelPath = new Path(localDir, new Path(outputDir, "Models")).toString
    val localOutputPath = new Path(localDir, outputDir).toString
    val modelDir = new File(modelPath)
    // Create the directory if it does not exist
    if (!modelDir.exists()) {
      modelDir.mkdirs()
    }
    val nodeName = nodeConfig.head._1
    val gpuUser = s"sshuser@$nodeName"

    // Give less permissive file permissions to the private RSA key
    printOutput(Seq("hdfs", "dfs", "-chmod", "700", outputPath.toString))
    // Copy the working directory to the GPU machines
    printOutput(
      Seq("scp", "-i", identity, "-r", "-o", "StrictHostKeyChecking=no", localDir, s"$gpuUser:$localDir"))

    // Run commands below only if writing input data to HDFS
    var gpuHdfsMntProc: Option[Process] = None
    var mountedDir: String = ""
    if (hdfsPath.isDefined) {
      // Add the mounted directory and all parents if it does not exist so mount will work
      mountedDir = new URI(hdfsPath.get._3).toString
      if (!mountedDir.startsWith("/")) {
        mountedDir = "/" + mountedDir
      }
      printOutput(Seq("ssh", "-i", identity, gpuUser, "mkdir", "-p", mountedDir))
      // Get the node:port from the hdfs URI
      var nodeAndPort = hdfsPath.get._1.replaceFirst("hdfs://", "")
      // Formatting: remove the trailing slash if it exists, otherwise we get unknown port error
      if (nodeAndPort.endsWith("/")) {
        nodeAndPort = nodeAndPort.substring(0, nodeAndPort.length - 1)
      }
      // Setup hdfs if used to read in dataset on the GPU VM
      val gpuHdfsMnt: ProcessBuilder =
        Seq("ssh", "-i", identity, gpuUser, "hdfs-mount", nodeAndPort, mountedDir)
      val gpulogger = ProcessLogger(output => log.info(s"Output received from VM mount: $output"))
      gpuHdfsMntProc = Some(gpuHdfsMnt.run(gpulogger))
      // Concatenate the hdfs output files together into one merged one
      val mergedOutputFile = s"${hdfsPath.get._2}/merged-input.txt"
      // Create local directory if it does not exist on the driver
      val mountedDirFolder = new File(mountedDir)
      if (!mountedDirFolder.exists()) {
        mountedDirFolder.mkdirs()
      }
      // Do HDFS merge to local directory
      printOutput(Seq("hdfs", "dfs", "-getmerge", s"${hdfsPath.get._2}/*.txt", mountedInputPath))
      // Copy back to HDFS
      printOutput(Seq("hdfs", "dfs", "-moveFromLocal", mountedInputPath, s"${hdfsPath.get._2}/merged-input.txt"))
    }
    // Run the mpi command
    printOutput(Seq("ssh", "-i", identity, gpuUser, command, mpiArgs, cntkArgs, "parallelTrain=true"))
    if (gpuHdfsMntProc.isDefined) {
      // Destroy the mount on the GPU VM
      gpuHdfsMntProc.get.destroy()
    }
    // Copy the model back
    val modelOrigin = s"$gpuUser:$modelPath"
    printOutput(Seq("scp", "-i", identity, "-r", "-o", "StrictHostKeyChecking=no", modelOrigin, localOutputPath))
    // Cleanup: Remove the GPU machine's working directory
    printOutput(Seq("ssh", "-i", identity, gpuUser, "rm", "-r", localDir))
    if (hdfsPath.isDefined) {
      // Cleanup: Remove the HDFS directory
      printOutput(Seq("hdfs", "dfs", "-rm", "-r", s"${hdfsPath.get._2}"))
      // Cleanup: close the mounted drive on the GPU VM
      printOutput(Seq("ssh", "-i", identity, gpuUser, "fusermount", "-uz", mountedDir))
      // Remove the empty directory
      printOutput(Seq("ssh", "-i", identity, gpuUser, "rmdir", mountedDir))
    }
  }

}
