// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.lightgbm

import java.io._
import java.net._

import com.microsoft.ml.lightgbm._
import com.microsoft.ml.spark.core.env.StreamUtilities._
import com.microsoft.ml.spark.core.utils.FaultToleranceUtils
import com.microsoft.ml.spark.lightgbm.booster.LightGBMBooster
import com.microsoft.ml.spark.lightgbm.dataset.LightGBMDataset
import com.microsoft.ml.spark.lightgbm.params.{ClassifierTrainParams, TrainParams}
import org.apache.spark.{BarrierTaskContext, TaskContext}
import org.apache.spark.sql.types.StructType
import org.slf4j.Logger

case class NetworkParams(defaultListenPort: Int, addr: String, port: Int, barrierExecutionMode: Boolean)
case class ColumnParams(labelColumn: String, featuresColumn: String, weightColumn: Option[String],
                        initScoreColumn: Option[String], groupColumn: Option[String])

private object TrainUtils extends Serializable {

  def createBooster(trainParams: TrainParams, trainDatasetPtr: LightGBMDataset,
                    validDatasetPtr: Option[LightGBMDataset]): LightGBMBooster = {
    // Create the booster
    val parameters = trainParams.toString()
    val booster = new LightGBMBooster(trainDatasetPtr, parameters)
    trainParams.modelString.foreach { modelStr =>
      booster.mergeBooster(modelStr)
    }
    validDatasetPtr.foreach { lgbmdataset =>
      booster.addValidationDataset(lgbmdataset)
    }
    booster
  }

  def beforeTrainIteration(batchIndex: Int, partitionId: Int, curIters: Int, log: Logger,
                           trainParams: TrainParams, booster: LightGBMBooster, hasValid: Boolean): Unit = {
    if (trainParams.delegate.isDefined) {
      trainParams.delegate.get.beforeTrainIteration(batchIndex, partitionId, curIters, log, trainParams, booster,
        hasValid)
    }
  }

  def afterTrainIteration(batchIndex: Int, partitionId: Int, curIters: Int, log: Logger,
                          trainParams: TrainParams, booster: LightGBMBooster, hasValid: Boolean,
                          isFinished: Boolean,
                          trainEvalResults: Option[Map[String, Double]],
                          validEvalResults: Option[Map[String, Double]]): Unit = {
    if (trainParams.delegate.isDefined) {
      trainParams.delegate.get.afterTrainIteration(batchIndex, partitionId, curIters, log, trainParams, booster,
        hasValid, isFinished, trainEvalResults, validEvalResults)
    }
  }

  def getLearningRate(batchIndex: Int, partitionId: Int, curIters: Int, log: Logger, trainParams: TrainParams,
                      previousLearningRate: Double): Double = {
    trainParams.delegate match {
      case Some(delegate) => delegate.getLearningRate(batchIndex, partitionId, curIters, log, trainParams,
          previousLearningRate)
      case None => previousLearningRate
    }
  }

  def updateOneIteration(trainParams: TrainParams,
                         booster: LightGBMBooster,
                         log: Logger,
                         iters: Int): Boolean = {
    var isFinished = false
    try {
        if (trainParams.objectiveParams.fobj.isDefined) {
          val classification = trainParams.isInstanceOf[ClassifierTrainParams]
          val (gradient, hessian) = trainParams.objectiveParams.fobj.get.getGradient(
            booster.innerPredict(0, classification), booster.trainDataset.get)
          isFinished = booster.updateOneIterationCustom(gradient, hessian)
        } else {
          isFinished = booster.updateOneIteration()
        }
      log.info("LightGBM running iteration: " + iters + " with is finished: " + isFinished)
    } catch {
      case e: java.lang.Exception =>
        log.warn("LightGBM reached early termination on one task," +
          " stopping training on task. This message should rarely occur." +
          " Inner exception: " + e.toString)
        isFinished = true
    }
    isFinished
  }

  def trainCore(batchIndex: Int, trainParams: TrainParams, booster: LightGBMBooster,
                log: Logger, hasValid: Boolean): Option[Int] = {
    var isFinished = false
    var iters = 0
    val evalNames = booster.getEvalNames()
    val evalCounts = evalNames.length
    val bestScore = new Array[Double](evalCounts)
    val bestScores = new Array[Array[Double]](evalCounts)
    val bestIter = new Array[Int](evalCounts)
    val partitionId = TaskContext.getPartitionId
    var learningRate: Double = trainParams.learningRate
    var bestIterResult: Option[Int] = None
    while (!isFinished && iters < trainParams.numIterations) {
      beforeTrainIteration(batchIndex, partitionId, iters, log, trainParams, booster, hasValid)
      val newLearningRate = getLearningRate(batchIndex, partitionId, iters, log, trainParams,
        learningRate)
      if (newLearningRate != learningRate) {
        log.info(s"LightGBM task calling booster.resetParameter to reset learningRate" +
          s" (newLearningRate: $newLearningRate)")
        booster.resetParameter(s"learning_rate=$newLearningRate")
        learningRate = newLearningRate
      }

      isFinished = updateOneIteration(trainParams, booster, log, iters)

      val trainEvalResults: Option[Map[String, Double]] = if (trainParams.isProvideTrainingMetric && !isFinished) {
        val evalResults: Array[(String, Double)] = booster.getEvalResults(evalNames, 0)
        evalResults.foreach { case (evalName: String, score: Double) => log.info(s"Train $evalName=$score") }
        Option(Map(evalResults:_*))
      } else {
        None
      }

      val validEvalResults: Option[Map[String, Double]] = if (hasValid && !isFinished) {
        val evalResults: Array[(String, Double)] = booster.getEvalResults(evalNames, 1)
        val results: Array[(String, Double)] = evalResults.zipWithIndex.map { case ((evalName, evalScore), index) =>
          log.info(s"Valid $evalName=$evalScore")
          val cmp =
            if (evalName.startsWith("auc") || evalName.startsWith("ndcg@") || evalName.startsWith("map@") ||
              evalName.startsWith("average_precision"))
              (x: Double, y: Double, tol: Double) => x - y > tol
            else
              (x: Double, y: Double, tol: Double) => x - y < tol
          if (bestScores(index) == null || cmp(evalScore, bestScore(index), trainParams.improvementTolerance)) {
            bestScore(index) = evalScore
            bestIter(index) = iters
            bestScores(index) = evalResults.map(_._2)
          } else if (iters - bestIter(index) >= trainParams.earlyStoppingRound) {
            isFinished = true
            log.info("Early stopping, best iteration is " + bestIter(index))
            bestIterResult = Some(bestIter(index))
          }

          (evalName, evalScore)
        }

        Option(Map(results:_*))
      } else {
        None
      }

      afterTrainIteration(batchIndex, partitionId, iters, log, trainParams, booster, hasValid, isFinished,
        trainEvalResults, validEvalResults)

      iters = iters + 1
    }
    bestIterResult
  }

  def beforeGenerateTrainDataset(batchIndex: Int, columnParams: ColumnParams, schema: StructType,
                                 log: Logger, trainParams: TrainParams): Unit = {
    if(trainParams.delegate.isDefined) {
      trainParams.delegate.get.beforeGenerateTrainDataset(batchIndex, TaskContext.getPartitionId, columnParams,
        schema, log, trainParams)
    }
  }

  def afterGenerateTrainDataset(batchIndex: Int, columnParams: ColumnParams, schema: StructType,
                                 log: Logger, trainParams: TrainParams): Unit = {
    if(trainParams.delegate.isDefined) {
      trainParams.delegate.get.afterGenerateTrainDataset(batchIndex, TaskContext.getPartitionId, columnParams,
        schema, log, trainParams)
    }
  }

  def beforeGenerateValidDataset(batchIndex: Int, columnParams: ColumnParams, schema: StructType,
                                 log: Logger, trainParams: TrainParams): Unit = {
    if(trainParams.delegate.isDefined) {
      trainParams.delegate.get.beforeGenerateValidDataset(batchIndex, TaskContext.getPartitionId, columnParams,
        schema, log, trainParams)
    }
  }

  def afterGenerateValidDataset(batchIndex: Int, columnParams: ColumnParams, schema: StructType,
                                log: Logger, trainParams: TrainParams): Unit = {
    if (trainParams.delegate.isDefined) {
      trainParams.delegate.get.afterGenerateValidDataset(batchIndex, TaskContext.getPartitionId, columnParams,
        schema, log, trainParams)
    }
  }

  private def findOpenPort(defaultListenPort: Int, numTasksPerExec: Int, log: Logger): Socket = {
    val basePort = defaultListenPort + (LightGBMUtils.getWorkerId() * numTasksPerExec)
    if (basePort > LightGBMConstants.MaxPort) {
      throw new Exception(s"Error: port $basePort out of range, possibly due to too many executors or unknown error")
    }
    var localListenPort = basePort
    var foundPort = false
    var taskServerSocket: Socket = null
    while (!foundPort) {
      try {
        taskServerSocket = new Socket()
        taskServerSocket.bind(new InetSocketAddress(localListenPort))
        foundPort = true
      } catch {
        case _: IOException =>
          log.warn(s"Could not bind to port $localListenPort...")
          localListenPort += 1
          if (localListenPort > LightGBMConstants.MaxPort) {
            throw new Exception(s"Error: port $basePort out of range, possibly due to networking or firewall issues")
          }
          if (localListenPort - basePort > 1000) {
            throw new Exception("Error: Could not find open port after 1k tries")
          }
      }
    }
    log.info(s"Successfully bound to port $localListenPort")
    taskServerSocket
  }

  def setFinishedStatus(networkParams: NetworkParams,
                        localListenPort: Int, log: Logger): Unit = {
    using(new Socket(networkParams.addr, networkParams.port)) {
      driverSocket =>
        using(new BufferedWriter(new OutputStreamWriter(driverSocket.getOutputStream))) {
          driverOutput =>
            log.info("sending finished status to driver")
            // If barrier execution mode enabled, create a barrier across tasks
            driverOutput.write(s"${LightGBMConstants.FinishedStatus}\n")
            driverOutput.flush()
        }.get
    }.get
  }

  def getNetworkInitNodes(networkParams: NetworkParams,
                          localListenPort: Int, log: Logger,
                          ignoreTask: Boolean): String = {
    using(new Socket(networkParams.addr, networkParams.port)) {
      driverSocket =>
        usingMany(Seq(new BufferedReader(new InputStreamReader(driverSocket.getInputStream)),
          new BufferedWriter(new OutputStreamWriter(driverSocket.getOutputStream)))) {
          io =>
            val driverInput = io(0).asInstanceOf[BufferedReader]
            val driverOutput = io(1).asInstanceOf[BufferedWriter]
            val taskStatus =
              if (ignoreTask) {
                log.info("send empty status to driver")
                LightGBMConstants.IgnoreStatus
              } else {
                val taskHost = driverSocket.getLocalAddress.getHostAddress
                val taskInfo = s"$taskHost:$localListenPort"
                log.info(s"send current task info to driver: $taskInfo ")
                taskInfo
              }
            // Send the current host:port to the driver
            driverOutput.write(s"$taskStatus\n")
            driverOutput.flush()
            // If barrier execution mode enabled, create a barrier across tasks
            if (networkParams.barrierExecutionMode) {
              val context = BarrierTaskContext.get()
              context.barrier()
              if (context.partitionId() == 0) {
                setFinishedStatus(networkParams, localListenPort, log)
              }
            }
            if (taskStatus != LightGBMConstants.IgnoreStatus) {
              // Wait to get the list of nodes from the driver
              val nodes = driverInput.readLine()
              log.info(s"LightGBM worker got nodes for network init: $nodes")
              nodes
            } else {
              taskStatus
            }
        }.get
    }.get
  }

  def networkInit(nodes: String, localListenPort: Int, log: Logger, retry: Int, delay: Long): Unit = {
    try {
      LightGBMUtils.validate(lightgbmlib.LGBM_NetworkInit(nodes, localListenPort,
        LightGBMConstants.DefaultListenTimeout, nodes.split(",").length), "Network init")
    } catch {
      case ex@(_: Exception | _: Throwable) =>
        log.info(s"NetworkInit failed with exception on local port $localListenPort with exception: $ex")
        Thread.sleep(delay)
        if (retry > 0) {
          log.info(s"Retrying NetworkInit with local port $localListenPort")
          networkInit(nodes, localListenPort, log, retry - 1, delay * 2)
        } else {
          log.info(s"NetworkInit reached maximum exceptions on retry: $ex")
          throw ex
        }
    }
  }

  /**
    * Gets the main node's port that will return the LightGBM Booster.
    * Used to minimize network communication overhead in reduce step.
    * @return The main node's port number.
    */
  def getMainWorkerPort(nodes: String, log: Logger): Int = {
    val nodesList = nodes.split(",")
    if (nodesList.length == 0) {
      throw new Exception("Error: could not split nodes list correctly")
    }
    val mainNode = nodesList(0)
    val hostAndPort = mainNode.split(":")
    if (hostAndPort.length != 2) {
      throw new Exception("Error: could not parse main worker host and port correctly")
    }
    val mainHost = hostAndPort(0)
    val mainPort = hostAndPort(1)
    log.info(s"LightGBM setting main worker host: $mainHost and port: $mainPort")
    mainPort.toInt
  }

  /** Retrieve the network nodes and current port information.
    *
    * Establish local socket connection.
    *
    * Note: Ideally we would start the socket connections in the C layer, this opens us up for
    * race conditions in case other applications open sockets on cluster, but usually this
    * should not be a problem
    *
    * @param networkParams The network parameters.
    * @param numTasksPerExec The number of tasks per executor.
    * @param log The logger.
    * @param isEnabledWorker True if the current worker is enabled, including whether the partition
    *                        was enabled and this is the chosen worker to initialize the network connection.
    * @return A tuple containing the string with all nodes and the current worker's open socket connection.
    */
  def getNetworkInfo(networkParams: NetworkParams, numTasksPerExec: Int,
                     log: Logger, isEnabledWorker: Boolean): (String, Int) = {
    using(findOpenPort(networkParams.defaultListenPort, numTasksPerExec, log)) {
      openPort =>
        val localListenPort = openPort.getLocalPort
        log.info(s"LightGBM task connecting to host: ${networkParams.addr} and port: ${networkParams.port}")
        FaultToleranceUtils.retryWithTimeout() {
          (getNetworkInitNodes(networkParams, localListenPort, log, !isEnabledWorker), localListenPort)
        }
    }.get
  }

  /** Return true if the current thread will return the booster after training is complete.
    *
    * @param isEnabledWorker True if the current worker is enabled, including whether the partition
    *                        was enabled and this is the chosen worker to initialize the network connection.
    * @param nodes The string representation of all nodes communicating in the network.
    * @param log The logger.
    * @param numTasksPerExec The number of tasks per executor.
    * @param localListenPort The local port used to setup the network ring of communication.
    * @return Boolean representing whether the current task will return the booster or not.
    */
  def getReturnBooster(isEnabledWorker: Boolean, nodes: String, log: Logger,
                       numTasksPerExec: Int, localListenPort: Int): Boolean = {
    if (!isEnabledWorker) {
      false
    } else {
      val mainWorkerPort = getMainWorkerPort(nodes, log)
      mainWorkerPort == localListenPort
    }
  }
}
