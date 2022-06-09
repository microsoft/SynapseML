// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.nbtest

import com.microsoft.azure.synapse.ml.build.BuildInfo
import com.microsoft.azure.synapse.ml.core.env.FileUtilities
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.io.http.RESTHelpers.sendAndParseJson
import com.microsoft.azure.synapse.ml.nbtest.SynapseUtilities._
import org.apache.commons.io.FileUtils
import org.apache.http.client.methods.HttpGet

import java.io.File
import java.lang.ProcessBuilder.Redirect
import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.language.existentials
import scala.util.Try

class SynapseTestCleanup extends TestBase {

  import SynapseJsonProtocol._

  ignore("Clean up all pools") {
    println("Deleting stray old Apache Spark Pools...")
    val getBigDataPoolsUri =
      s"""
         |$ManagementUrlRoot/resources?api-version=2021-04-01&
         |$$filter=substringof(name, \'$WorkspaceName\') and
         | resourceType eq \'Microsoft.Synapse/workspaces/bigDataPools\'
         |""".stripMargin.replaceAll(LineSeparator, "").replaceAll(" ", "%20")
    val getBigDataPoolRequest = new HttpGet(getBigDataPoolsUri)
    getBigDataPoolRequest.setHeader("Authorization", s"Bearer $ArmToken")
    val sparkPools = sendAndParseJson(getBigDataPoolRequest).convertTo[SynapseResourceResponse].value
    sparkPools.foreach(sparkPool => {
      val name = sparkPool.name.stripPrefix(s"$WorkspaceName/")
      deleteSparkPool(name)
    })
  }

}

class SynapseTests extends TestBase {

  val resourcesDirectory = new File(getClass.getResource("/").toURI)
  val notebooksDir = new File(resourcesDirectory, "generated-notebooks")
  println(s"Notebooks dir: $notebooksDir")
  FileUtils.deleteDirectory(notebooksDir)
  assert(notebooksDir.mkdirs())

  val notebooks: Array[File] = FileUtilities.recursiveListFiles(FileUtilities
    .join(BuildInfo.baseDirectory.getParent, "notebooks/features")
    .getCanonicalFile)
    .filter(_.getName.endsWith(".ipynb"))
    .map { f =>
      FileUtilities.copyFile(f, notebooksDir, true)
      val newFile = new File(notebooksDir, f.getName)
      val targetName = new File(notebooksDir, f.getName.replace(" ", "").replace("-", ""))
      newFile.renameTo(targetName)
      targetName
    }

  assert(notebooks.length > 1)

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
    val pb = new ProcessBuilder()
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

  runCmd(activateCondaEnv ++ Seq("jupyter", "nbconvert", "--to", "python", "*.ipynb"), notebooksDir)

  val selectedPythonFiles: Array[File] = FileUtilities.recursiveListFiles(notebooksDir)
    .filter(_.getAbsolutePath.endsWith(".py"))
    .filterNot(_.getAbsolutePath.contains("HyperParameterTuning"))
    .filterNot(_.getAbsolutePath.contains("CyberML"))
    .filterNot(_.getAbsolutePath.contains("VowpalWabbitOverview"))
    .filterNot(_.getAbsolutePath.contains("IsolationForest"))
    .filterNot(_.getAbsolutePath.contains("ExplanationDashboard"))
    .filterNot(_.getAbsolutePath.contains("DeepLearning"))
    .filterNot(_.getAbsolutePath.contains("InterpretabilitySnowLeopardDetection"))
    .sortBy(_.getAbsolutePath)

  selectedPythonFiles.foreach(println)
  assert(selectedPythonFiles.length > 1)

  val expectedPoolCount: Int = selectedPythonFiles.length

  println("SynapseTests E2E Test Suite starting...")
  tryDeleteOldSparkPools()

  println(s"Creating $expectedPoolCount Spark Pools...")
  val sparkPools: Seq[String] = createSparkPools(expectedPoolCount)

  val livyBatches: Array[LivyBatch] = selectedPythonFiles.zip(sparkPools).map { case (file, poolName) =>
    SynapseUtilities.uploadAndSubmitNotebook(poolName, file) }

  livyBatches.foreach { livyBatch =>
    println(s"submitted livy job: ${livyBatch.id} for ${livyBatch.runName} to sparkPool: ${livyBatch.sparkPool}")
    test(livyBatch.runName) {
      try {
        val result = Await.ready(
          livyBatch.monitor(),
          Duration(SynapseUtilities.TimeoutInMillis.toLong, TimeUnit.MILLISECONDS)).value.get
        assert(result.isSuccess)
      } catch {
        case t: Throwable =>
          livyBatch.cancelRun()
          throw new RuntimeException(s"Job failed see ${livyBatch.jobStatusPage} for details", t)
      }
    }
  }

  protected override def afterAll(): Unit = {
    println("Synapse E2E Test Suite finished. Deleting Spark Pools...")
    val failures = sparkPools.map(pool => Try(deleteSparkPool(pool)))
      .filter(_.isFailure)
    if (failures.isEmpty) {
      println("All Spark Pools deleted successfully.")
    } else {
      println("Failed to delete all spark pools cleanly:")
      failures.foreach(failure =>
        println(failure.failed.get.getMessage))
    }
    super.afterAll()
  }
}
