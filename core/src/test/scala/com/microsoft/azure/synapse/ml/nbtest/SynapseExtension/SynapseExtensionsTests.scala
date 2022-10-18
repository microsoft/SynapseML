package com.microsoft.azure.synapse.ml.nbtest.SynapseExtension

import com.microsoft.azure.synapse.ml.build.BuildInfo
import com.microsoft.azure.synapse.ml.core.env.FileUtilities
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.commons.io.FileUtils

import java.io.File
import java.lang.ProcessBuilder.Redirect
import java.util.concurrent.TimeUnit
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.language.existentials

import scala.concurrent.ExecutionContext.Implicits.global

class SynapseExtensionsTests extends TestBase {
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
    //.filterNot(A => A.getAbsolutePath.contains("SparkServing"))
    //.filter(A => A.getAbsolutePath.contains("NoStreaming"))
    .sortBy(_.getAbsolutePath)

  selectedPythonFiles.foreach(println)
  assert(selectedPythonFiles.length > 0)

  // Clean up existing SJDs
  SynapseExtensionUtilities.listArtifacts()
    .foreach(sjd =>
    {
      println(s"Artifact cleanup: deleting SJD ${sjd.displayName}")
      SynapseExtensionUtilities.deleteArtifact(sjd.objectId)
    })

  val lakehouseArtifactId = SynapseExtensionUtilities.createLakehouseArtifact()

  val chunked = selectedPythonFiles.seq.grouped(9).seq//.drop(2).take(1).seq
    .foreach(fileGroup => Future.sequence(fileGroup.seq.map(createAndExecuteSJD)))

  def createAndExecuteSJD(notebookFile: File): Future[String] =
  {
    val notebookName = SynapseExtensionUtilities.getBlobNameFromFilepath(notebookFile.getPath)
    val artifactId = SynapseExtensionUtilities.createSJDArtifact(notebookFile.getPath)
    val notebookBlobPath = SynapseExtensionUtilities.uploadNotebookToAzure(notebookFile)
    SynapseExtensionUtilities.updateSJDArtifact(notebookBlobPath, artifactId, lakehouseArtifactId)
    val jobInstanceId = SynapseExtensionUtilities.submitJob(artifactId)
    test(notebookName) {
      try {
        val result = Await.ready(
          SynapseExtensionUtilities.monitorJob(artifactId, jobInstanceId),
          Duration(SynapseExtensionUtilities.TimeoutInMillis.toLong, TimeUnit.MILLISECONDS)).value.get
        assert(result.isSuccess)
      } catch {
        case t: Throwable =>
          throw new RuntimeException(s"Job failed for $notebookName", t)
      }
    }
    SynapseExtensionUtilities.monitorJob(artifactId, jobInstanceId)
  }
}