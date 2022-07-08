// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm

import com.microsoft.azure.synapse.ml.core.env.StreamUtilities.{using, usingMany}
import com.microsoft.azure.synapse.ml.core.utils.{ClusterUtil, FaultToleranceUtils}
import com.microsoft.azure.synapse.ml.lightgbm.NetworkManager.parseWorkerMessage
import com.microsoft.ml.lightgbm.lightgbmlib
import org.apache.spark.BarrierTaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.slf4j.Logger

import java.io.{BufferedReader, BufferedWriter, IOException, InputStreamReader, OutputStreamWriter}
import java.net.{InetSocketAddress, ServerSocket, Socket}
import java.util.concurrent.Executors
import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.concurrent.duration.{Duration, SECONDS}
import scala.language.existentials

case class TaskMessageInfo(status: String,
                           taskHost: String,
                           localListenPort: Int,
                           partitionId: Int,
                           executorId: String) {
  def this(status: String) = this(status, "", -1, -1, "") // Constructor for general messages, not Task-connected

  val isForTraining: Boolean = status == LightGBMConstants.EnabledTask
  val isForLoadOnly: Boolean = status == LightGBMConstants.IgnoreStatus
  val isFinished: Boolean = status == LightGBMConstants.FinishedStatus

  // Format all the information as a delimited string to send to driver
  override def toString: String = s"$status:$taskHost:$localListenPort:$partitionId:$executorId"
}

case class NetworkTopologyInfo(lightgbmNetworkString: String,
                               executorPartitionIdList: Array[Int],
                               localListenPort: Int)

object NetworkManager {
  /**
    * Create a NetworkManager, which will encapsulate all network operations.
    * This method will opens a socket communications channel on the driver, and then initialize
    * the network manager itself.
    * The NetworkManager object will start a thread that waits for the host:port from the executors,
    * and then sends back the information to the executors.
    *
    * @param numTasks The total number of training tasks to wait for.
    * @return The NetworkTopology.
    */
  def create(numTasks: Int,
             spark: SparkSession,
             driverListenPort: Int,
             timeout: Double,
             useBarrierExecutionMode: Boolean): NetworkManager = {
    // Start a thread and open port to listen on
    implicit val context: ExecutionContextExecutor =
      ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())
    val driverServerSocket = new ServerSocket(driverListenPort)
    // Set timeout on socket
    val duration = Duration(timeout, SECONDS)
    if (duration.isFinite()) {
      driverServerSocket.setSoTimeout(duration.toMillis.toInt)
    }

    val host = ClusterUtil.getDriverHost(spark)
    val port = driverServerSocket.getLocalPort

    new NetworkManager(
      numTasks,
      driverServerSocket,
      host,
      port,
      timeout,
      useBarrierExecutionMode)
  }

  /** Retrieve the network nodes and current port information from the driver.
    *
    * Establish local socket connection.
    *
    * Note: Ideally we would start the socket connections in the C layer, this opens us up for
    * race conditions in case other applications open sockets on cluster, but usually this
    * should not be a problem
    *
    * @param ctx Information about the current training session.
    * @param shouldExecuteTraining True if the current worker will participate in the LightGBM training network.
    */
  def getGlobalNetworkInfo(ctx: TrainingContext,
                           log: Logger,
                           partitionId: Int,
                           shouldExecuteTraining: Boolean,
                           measures: TaskInstrumentationMeasures): NetworkTopologyInfo = {
    measures.markNetworkInitializationStart()
    val networkParams = ctx.networkParams
    val out = using(findOpenPort(ctx, log).get) {
      openPort =>
        val localListenPort = openPort.getLocalPort
        log.info(s"LightGBM task connecting to host: ${networkParams.ipAddress} and port: ${networkParams.port}")
        FaultToleranceUtils.retryWithTimeout() {
          getNetworkTopologyInfoFromDriver(networkParams, partitionId, localListenPort, log, shouldExecuteTraining)
        }
    }.get
    measures.markNetworkInitializationStop()
    out
  }

  def getNetworkTopologyInfoFromDriver(networkParams: NetworkParams,
                                       partitionId: Int,
                                       localListenPort: Int,
                                       log: Logger,
                                       shouldExecuteTraining: Boolean): NetworkTopologyInfo = {
    using(new Socket(networkParams.ipAddress, networkParams.port)) {
      driverSocket =>
        usingMany(Seq(new BufferedReader(new InputStreamReader(driverSocket.getInputStream)),
          new BufferedWriter(new OutputStreamWriter(driverSocket.getOutputStream)))) {
          io =>
            val driverInput = io.head.asInstanceOf[BufferedReader]
            val driverOutput = io(1).asInstanceOf[BufferedWriter]

            // Get message to send to driver with info about this task
            val taskStatus = TaskMessageInfo(
              if (shouldExecuteTraining) LightGBMConstants.EnabledTask else LightGBMConstants.IgnoreStatus,
              driverSocket.getLocalAddress.getHostAddress,
              localListenPort,
              partitionId,
              LightGBMUtils.getExecutorId) // TODO can we use host for this?
            val message = taskStatus.toString()
            log.info(s"task sending status message to driver: $message ")
            driverOutput.write(s"$message\n")
            driverOutput.flush()

            // If barrier execution mode enabled, create a barrier across tasks and send message when finished
            if (networkParams.barrierExecutionMode) {
              val context = BarrierTaskContext.get()
              context.barrier()
              if (context.partitionId() == 0) {
                 setFinishedStatus(networkParams, log)
              }
            }

            // Wait for response from driver.  It should send the final LightGBM network string,
            // and a list of partition ids in this executor.
            val lightGbmMachineList = driverInput.readLine()
            val partitionsByExecutorStr = driverInput.readLine()
            val executorPartitionIds: Array[Int] =
              parseExecutorPartitionList(partitionsByExecutorStr, taskStatus.executorId)
            log.info(s"task with partition id $partitionId received nodes for network init: $lightGbmMachineList")
            log.info(s"task with partition id $partitionId received partition topology: $partitionsByExecutorStr")
            NetworkTopologyInfo(lightGbmMachineList, executorPartitionIds, localListenPort)
        }.get
    }.get
  }

  def parseExecutorPartitionList(partitionsByExecutorStr: String, executorId: String): Array[Int] = {
    // extract this executors partition ids as an array, from a string that is formatter like this:
    // executor1=partition1,partition2:executor2=partition3,partition4
    val partitionsByExecutor = partitionsByExecutorStr.split(":")
    val executorListStr = partitionsByExecutor.find(line => line.startsWith(executorId + "="))
    if (executorListStr.isEmpty)
      throw new Exception(s"Could not find partitions for executor $executorListStr. List: $partitionsByExecutorStr")
    val partitionList = executorListStr.get.split("=")(1)
    partitionList.split(",").map(str => str.toInt).sorted
  }

  def initLightGBMNetwork(ctx: PartitionTaskContext,
                          log: Logger,
                          retry: Int = LightGBMConstants.NetworkRetries,
                          delay: Long = LightGBMConstants.InitialDelay): Unit = {
    log.info(s"Calling NetworkInit on local port ${ctx.localListenPort} with value ${ctx.lightGBMNetworkString}")
    try {
      LightGBMUtils.validate(lightgbmlib.LGBM_NetworkInit(
        ctx.lightGBMNetworkString,
        ctx.localListenPort,
        LightGBMConstants.DefaultListenTimeout,
        ctx.lightGBMNetworkMachineCount), "Network init")
      log.info(s"NetworkInit succeeded. LightGBM task listening on: ${ctx.localListenPort}")
    } catch {
      case ex@(_: Exception | _: Throwable) =>
        log.info(s"NetworkInit failed with exception on local port ${ctx.localListenPort} with exception: $ex")
        Thread.sleep(delay)
        if (retry == 0) {
          log.info(s"NetworkInit reached maximum exceptions on retry: $ex")
          throw ex
        }
        log.info(s"Retrying NetworkInit with local port ${ctx.localListenPort}")
        initLightGBMNetwork(ctx, log, retry - 1, delay * 2)
    }
  }

  /**
    * Gets the main node's port that will return the LightGBM Booster.
    * Used to minimize network communication overhead in reduce step.
    * @return The main node's port number.
    */
  def getMainWorkerPort(nodes: String, log: Logger): Int = {
    val nodesList = nodes.split(",")
    if (nodesList.isEmpty) {
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

  def findOpenPort(ctx: TrainingContext, log: Logger): Option[Socket] = {
    val defaultListenPort: Int = ctx.networkParams.defaultListenPort
    val basePort = defaultListenPort + (LightGBMUtils.getWorkerId * ctx.numTasksPerExecutor)
    if (basePort > LightGBMConstants.MaxPort) {
      throw new Exception(s"Error: port $basePort out of range, possibly due to too many executors or unknown error")
    }
    var localListenPort = basePort
    var taskServerSocket: Option[Socket] = None

    @tailrec
    def findPort(): Unit = {
      try {
        taskServerSocket = Option(new Socket())
        taskServerSocket.get.bind(new InetSocketAddress(localListenPort))
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
          findPort()
      }
    }
    findPort()
    log.info(s"Successfully bound to port $localListenPort")
    taskServerSocket
  }

  def setFinishedStatus(networkParams: NetworkParams, log: Logger): Unit = {
    using(new Socket(networkParams.ipAddress, networkParams.port)) {
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

  def parseWorkerMessage(message: String): TaskMessageInfo = {
    val components = message.split(":")
    val status = components(0)

    if (status == LightGBMConstants.FinishedStatus) new TaskMessageInfo(status)
    else {
      if (components.length != 5) throw new Exception(s"Unexpected message: $message")

      val host = components(1)
      val port = components(2).toInt
      val partitionId: Int = components(3).toInt
      val executorId = components(4)  //scalastyle:ignore magic.number
      TaskMessageInfo(status, host, port, partitionId, executorId)
    }
  }
}

/**
  * Object to encapsulate all Spark/LightGBM network topology information,
  * along with operations on the network.
  */
case class NetworkManager(numTasks: Int,
                          driverServerSocket: ServerSocket,
                          host: String,
                          port: Int,
                          timeout: Double,
                          useBarrierExecutionMode: Boolean) extends Logging {

  // Arrays to store network topology in as it arrives
  private val hostAndPorts = ListBuffer[(Socket, String)]()
  private val loadOnlyHostAndPorts = ListBuffer[Socket]() // TODO we know this count right?
  private val hostToMinPartition = mutable.Map[String, Int]()
  private val partitionsByExecutor = mutable.Map[String, List[Int]]()

  // Concatenate with commas, eg: host1:port1,host2:port2, ... etc
  // Also make sure the order is deterministic by sorting on minimum partition id
  lazy val networkTopologyAsString: String = {
    val hostPortsList = hostAndPorts.map(_._2).sortBy(hostPort => {
      val host = hostPort.split(":")(0)
      hostToMinPartition(host)
    })
    hostPortsList.mkString(",")
  }

  // Create a string representing of the partitionsByExecutor map
  // e.g. executor1=partition1,partition2:executor2=partition3,partition4
  lazy val partitionsByExecutorAsString: String = {
    val executorList = partitionsByExecutor.map { case (executor, partitionList) =>
      executor + "=" + partitionList.mkString(",")
    }
    executorList.mkString(":")
  }

  // This will be kicked off at object creation time, and can be waited on by waitForNetworkDone()
  private val networkCommunicationThread: Future[Unit] = Future {
    log.info(s"driver waiting for connections on host: $host and port: $port")
    waitForAllTasksToReport()

    // We have all the information now, so report back to workers
    sendDataToExecutors(networkTopologyAsString, partitionsByExecutorAsString)

    closeConnections()
  } (ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor()))

  def waitForNetworkCommunicationsDone(): Unit = {
    Await.result(networkCommunicationThread, Duration(timeout, SECONDS))
  }

  def waitForAllTasksToReport(): Unit = {
    if (useBarrierExecutionMode) {
      log.info(s"driver using barrier execution mode for $numTasks tasks...")

      @tailrec
      def connectToWorkersUntilBarrier(): Unit = {
        val done = handleNextWorkerConnection()
        if (!done) connectToWorkersUntilBarrier()
      }

      connectToWorkersUntilBarrier()
    } else {
      log.info(s"driver expecting $numTasks connections...")

      @tailrec
      def connectToWorkers(numProcessedTasks: Int): Unit = {
        handleNextWorkerConnection()
        val newNumProcessedTasks = numProcessedTasks + 1
        if (newNumProcessedTasks != numTasks) connectToWorkers(newNumProcessedTasks)
      }

      connectToWorkers(0)
    }
  }

  /** Handles the connection to a task from the driver.
    *
    * @return Whether the response was a "Finished" response for barrier mode.
    *         Always false for non-barrier mode.
    */
  private def handleNextWorkerConnection(): Boolean = {
    log.info("driver accepting a new connection...")

    val socket = driverServerSocket.accept()  // block until connection is made

    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream))
    val messageStr = reader.readLine()
    log.info(s"received worker message string: $messageStr")
    val message: TaskMessageInfo = parseWorkerMessage(messageStr)

    if (message.isFinished) {
      log.info("driver received all tasks from barrier stage")
      true
    } else {
      message match {
        case m if m.isForLoadOnly =>
          log.info("driver received load-only status from task")
          loadOnlyHostAndPorts += socket
        case m if m.isForTraining =>
          val networkInfoString = s"${message.taskHost}:${message.localListenPort}"
          log.info(s"driver received socket from task: $networkInfoString")
          val socketAndMessage = (socket, networkInfoString)
          hostAndPorts += socketAndMessage
        case _ => throw new Exception(s"Unknown message type: ${message.toString()}")
      }

      // Update the min partition/executor tracking
      if (!hostToMinPartition.contains(message.taskHost)
        || hostToMinPartition(message.taskHost) > message.partitionId) {
        hostToMinPartition(message.taskHost) = message.partitionId
      }

      // Update the tracking of which partitions are on which executor
      if (!partitionsByExecutor.contains(message.executorId)) {
        partitionsByExecutor(message.executorId) = List { message.partitionId }
      } else {
        val currentPartitionList = partitionsByExecutor(message.executorId)
        partitionsByExecutor(message.executorId) = currentPartitionList :+ message.partitionId
      }
      false
    }
  }

  def sendDataToExecutors(lightGBMNetworkTopology: String, partitionsByExecutor: String): Unit = {
    // TODO optimize and not send for bulk mode helpers
    // Send aggregated network information back to all tasks and helper tasks on executors
    val count = hostAndPorts.length + loadOnlyHostAndPorts.length
    log.info(s"driver writing back network topology to $count connections: $lightGBMNetworkTopology")
    log.info(s"driver writing back partition topology to $count connections: $partitionsByExecutor")
    hostAndPorts.foreach(hostAndPort => {
      val writer = new BufferedWriter(new OutputStreamWriter(hostAndPort._1.getOutputStream))
      writer.write(lightGBMNetworkTopology + "\n")
      writer.write(partitionsByExecutor + "\n")
      writer.flush()
    })
    loadOnlyHostAndPorts.foreach(hostAndPort => {
      val writer = new BufferedWriter(new OutputStreamWriter(hostAndPort.getOutputStream))
      writer.write(lightGBMNetworkTopology + "\n")
      writer.write(partitionsByExecutor + "\n")
      writer.flush()
    })
  }

  def closeConnections(): Unit = {
    log.info("driver closing all sockets and server socket")
    hostAndPorts.foreach(_._1.close())
    driverServerSocket.close()
    log.info("driver done closing all sockets and server socket")
  }
}
