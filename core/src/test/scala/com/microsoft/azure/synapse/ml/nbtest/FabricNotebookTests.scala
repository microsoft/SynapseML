// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.nbtest

import com.microsoft.azure.synapse.ml.core.env.FileUtilities
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.fabric.{FabricTestConstants, HasFabricOperationsConnection}

import java.io.{File, PrintWriter}
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, blocking}
import scala.util.{Failure, Try}
import scala.util.control.NonFatal

trait HasFabricNotebookTestConnection extends HasFabricOperationsConnection {
  fabricClientId = Some(FabricTestConstants.INTEGRATION_APP_ID)
  fabricRedirectUri = Some(FabricTestConstants.INTEGRATION_REDIRECT_URI)

  protected def integrationWorkspaceId: String = FabricTestConstants.INTEGRATION_WORKSPACE_ID

  protected def cleanupStaleArtifacts(): Unit = {
    val dryRun = sys.env.getOrElse("SYNAPSEML_FABRIC_CLEANUP_DRY_RUN", "false")
    require(Set("true", "false")(dryRun), "SYNAPSEML_FABRIC_CLEANUP_DRY_RUN must be true or false")
    fabricWorkspaceId = Some(integrationWorkspaceId)
    fabric.cleanupTestArtifacts(dryRun.toBoolean)
  }

  protected final def captureFabricSetup[T](setup: => T): Try[T] = {
    try {
      Try(setup)
    } catch {
      case error: InterruptedException => Failure(error)
    }
  }

  protected final def getFabricSetup[T](setup: Try[T]): T = setup match {
    case Failure(error: InterruptedException) =>
      Thread.currentThread().interrupt()
      throw error
    case _ => setup.get
  }

  private lazy val preflight = captureFabricSetup(cleanupStaleArtifacts())

  protected final def ensureFabricPreflight(): Unit = getFabricSetup(preflight)

  private lazy val storeSetup = captureFabricSetup {
    ensureFabricPreflight()
    createTrackedStore()
  }

  protected final def preparedStore: String = getFabricSetup(storeSetup)

  private val artifactTracker =
    new FabricTestArtifactTracker(artifactId => fabric.deleteArtifact(artifactId))

  protected def trackArtifact(artifactId: String): String = artifactTracker.track(artifactId)

  protected def withTrackedArtifact[T](artifactId: String)(use: String => T): T =
    artifactTracker.withArtifact(artifactId)(use)

  protected def cleanupTrackedArtifacts(): Unit = artifactTracker.cleanup()

  protected def createTrackedStore(): String = trackArtifact(fabric.createStoreArtifact())
}

class FabricTestCleanup extends TestBase with HasFabricNotebookTestConnection {
  test("Clean up owned Fabric test artifacts older than 24 hours") {
    ensureFabricPreflight()
  }
}

class FabricSmokeTests extends TestBase with HasFabricNotebookTestConnection {

  val trivialScript: String =
    """
      |from pyspark.sql import SparkSession
      |
      |spark = SparkSession.builder.getOrCreate()
      |
      |# Trivial 1+1 test
      |result = 1 + 1
      |assert result == 2, f"Expected 2, got {result}"
      |print(f"SUCCESS: 1 + 1 = {result}")
      |
      |spark.stop()
      |""".stripMargin

  lazy val notebookFile: File = {
    val dir = new File(System.getProperty("java.io.tmpdir"), "fabric-e2e-test")
    dir.mkdirs()
    val f = new File(dir, "OnePlusOne.py")
    val pw = new PrintWriter(f)
    try { pw.write(trivialScript) } finally { pw.close() }
    f
  }

  lazy val storeArtifactId: String = preparedStore

  test("OnePlusOne") {
    ensureFabricPreflight()
    runSmokeTest(storeArtifactId)
  }

  protected def runSmokeTest(storeId: String): Unit = {
    val notebookName = fabric.getBlobNameFromFilepath(notebookFile.getPath)
    withTrackedArtifact(fabric.createSJDArtifact(notebookFile.getPath)) { artifactId =>
      val notebookBlobPath = fabric.uploadNotebookToAzure(notebookFile)
      fabric.updateSJDArtifact(notebookBlobPath, artifactId, storeId, includePackages = false)
      blocking {
        Thread.sleep(3000) //scalastyle:ignore
      }
      val jobInstanceId = fabric.submitJob(artifactId)
      blocking {
        Thread.sleep(10000) //scalastyle:ignore
      }
      try {
        val result = Await.ready(
          fabric.monitorJob(artifactId, jobInstanceId),
          Duration(fabric.timeoutInMillis.toLong, TimeUnit.MILLISECONDS)).value.get
        assert(result.isSuccess)
      } catch {
        case t: Throwable =>
          throw new RuntimeException(s"Job failed for $notebookName", t)
      }
    }
  }

  override def afterAll(): Unit = {
    try {
      cleanupTrackedArtifacts()
    } finally {
      super.afterAll()
    }
  }
}

class FabricNotebookTests extends TestBase with HasFabricNotebookTestConnection {
  protected def discoverNotebooks(): Array[File] = {
    SharedNotebookE2ETestUtilities.generateNotebooks()
    FileUtilities.recursiveListFiles(SharedNotebookE2ETestUtilities.NotebooksDir)
      .filter(_.getAbsolutePath.endsWith(".py"))
      .filter(f => FabricNotebookTests.IncludedNotebooks.exists(f.getName.startsWith))
      .sortBy(_.getAbsolutePath)
  }

  val selectedPythonFiles: Array[File] = discoverNotebooks()

  selectedPythonFiles.foreach(x => println(s"Fabric notebook to be tested: $x"))
  assert(selectedPythonFiles.nonEmpty, "No notebooks found to test")

  lazy val storeArtifactId: String = preparedStore

  @volatile private var executorStarted = false
  protected def createNotebookExecutor(): ExecutorService =
    Executors.newFixedThreadPool(FabricNotebookTests.MaxConcurrency)

  lazy val executorService: ExecutorService = {
    val executor = createNotebookExecutor()
    executorStarted = true
    executor
  }
  implicit lazy val executionContext: ExecutionContext = ExecutionContext.fromExecutor(executorService)

  protected def notebookTimeout: Duration =
    Duration(fabric.timeoutInMillis.toLong, TimeUnit.MILLISECONDS)

  protected def runNotebook(notebookFile: File, storeId: String): String =
    withTrackedArtifact(fabric.createSJDArtifact(notebookFile.getPath)) { artifactId =>
      val notebookBlobPath = fabric.uploadNotebookToAzure(notebookFile)
      fabric.updateSJDArtifact(notebookBlobPath, artifactId, storeId)
      blocking { Thread.sleep(3000) } //scalastyle:ignore
      val jobInstanceId = fabric.submitJob(artifactId)
      blocking { Thread.sleep(10000) } //scalastyle:ignore
      Await.result(fabric.monitorJob(artifactId, jobInstanceId), notebookTimeout)
    }

  // Start the existing parallel workload only after the first selected test passes preflight.
  private lazy val submissions = captureFabricSetup {
    ensureFabricPreflight()
    val storeId = storeArtifactId
    selectedPythonFiles.map { notebookFile =>
      (Future(runNotebook(notebookFile, storeId)), notebookFile.getName)
    }
  }

  lazy val futures: Array[(Future[String], String)] = getFabricSetup(submissions)

  selectedPythonFiles.zipWithIndex.foreach { case (notebookFile, index) =>
    val notebookName = notebookFile.getName
    test(notebookName) {
      ensureFabricPreflight()
      val (future, submittedNotebookName) = futures(index)
      try {
        Await.result(future, notebookTimeout)
      } catch {
        case error: InterruptedException =>
          Thread.currentThread().interrupt()
          throw error
        case NonFatal(t) =>
          throw new RuntimeException(s"Job failed for $submittedNotebookName", t)
      }
    }
  }

  override def afterAll(): Unit = {
    try {
      FabricNotebookTests.shutdownAndCleanup(
        if (executorStarted) FabricNotebookTests.shutdownExecutor(executorService),
        cleanupTrackedArtifacts())
    } finally {
      super.afterAll()
    }
  }
}

object FabricNotebookTests {
  val MaxConcurrency: Int = 3

  // Include-based filtering: start with a small core set of self-contained notebooks
  // that don't require API keys (no Cognitive Services, OpenAI, etc.).
  // These cover the key SynapseML algorithms: LightGBM, VW, Causal Inference,
  // Classification, Regression, and Responsible AI.
  val IncludedNotebooks: Seq[String] = Seq(
    "ExploreAlgorithmsClassificationQuickstartTrainClassifier",
    "ExploreAlgorithmsRegressionQuickstartDataCleaning",
    "ExploreAlgorithmsRegressionQuickstartTrainRegressor",
    "ExploreAlgorithmsCausalInferenceQuickstartMeasureCausalEffects",
    "ExploreAlgorithmsResponsibleAIQuickstartDataBalanceAnalysis"
    // TODO: investigate InvalidPyFiles failures on Fabric SJDs:
    // "ExploreAlgorithmsLightGBMQuickstartClassificationRankingandRegression",
    // "ExploreAlgorithmsVowpalWabbitQuickstartClassificationQuantileRegressionandRegression",
    // "ExploreAlgorithmsVowpalWabbitQuickstartClassificationusingSparkMLVectors",
  )

  private val ExecutorShutdownTimeoutSeconds = 30L
  private val UniqueArtifactId = "[0-9a-fA-F]{32}"
  private val StoreArtifactName = s"^(Lakehouse|Warehouse)\\d{14}(?:$UniqueArtifactId)?$$".r
  private val SJDArtifactName =
    s"^(.+)-\\d{8}-\\d{2}-\\d{2}-\\d{2}(?:-$UniqueArtifactId)?$$".r
  private val TestSJDNames = (IncludedNotebooks :+ "OnePlusOne").toSet

  private[nbtest] def shutdownExecutor(executorService: ExecutorService): Unit = {
    shutdownExecutor(executorService, ExecutorShutdownTimeoutSeconds, TimeUnit.SECONDS)
  }

  private[nbtest] def shutdownExecutor(executorService: ExecutorService,
                                       timeout: Long,
                                       timeUnit: TimeUnit): Unit = {
    try {
      executorService.shutdown()
      if (!executorService.awaitTermination(timeout, timeUnit)) {
        executorService.shutdownNow()
        if (!executorService.awaitTermination(timeout, timeUnit)) {
          throw new IllegalStateException("Fabric notebook tasks did not stop before artifact cleanup")
        }
      }
    } catch {
      case e: InterruptedException =>
        executorService.shutdownNow()
        Thread.currentThread().interrupt()
        throw e
    }
  }

  private[nbtest] def shutdownAndCleanup(shutdown: => Unit, cleanup: => Unit): Unit = {
    val failures = ListBuffer.empty[Throwable]
    var interrupted = false
    try {
      shutdown
    } catch {
      case error: InterruptedException =>
        interrupted = true
        failures += error
      case NonFatal(error) => failures += error
    }
    try {
      cleanup
    } catch {
      case error: InterruptedException =>
        interrupted = true
        failures += error
      case NonFatal(error) => failures += error
    }
    if (interrupted) {
      Thread.currentThread().interrupt()
    }
    failures.headOption.foreach { failure =>
      failures.tail.foreach(failure.addSuppressed)
      throw failure
    }
  }

  private[nbtest] def isTestArtifactName(displayName: String): Boolean = {
    displayName match {
      case StoreArtifactName(_) => true
      case SJDArtifactName(name) => TestSJDNames(name)
      case _ => false
    }
  }
}
