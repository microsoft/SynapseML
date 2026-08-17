// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm

import com.microsoft.azure.synapse.ml.core.env.StreamUtilities.{using, usingMany}
import com.microsoft.azure.synapse.ml.core.utils.{ClusterUtil, FaultToleranceUtils}
import com.microsoft.ml.lightgbm.lightgbmlib
import org.apache.spark.BarrierTaskContext
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.slf4j.Logger

import java.io.{BufferedReader, BufferedWriter, IOException, InputStreamReader, OutputStreamWriter}
import java.net.{ConnectException, ServerSocket, Socket, SocketException, SocketTimeoutException}
import java.util.concurrent.{ExecutorService, Executors}
import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.concurrent.duration.{Duration, SECONDS}
import scala.language.existentials
import scala.util.control.NonFatal

object NetworkManager {
  private def addSuppressed(primaryFailure: Throwable, secondaryFailure: Throwable): Unit =
    NetworkManagerSocketSupport.addSuppressed(primaryFailure, secondaryFailure)

  private[lightgbm] def closeSocketWithRetry(socket: Socket): Unit =
    NetworkManagerSocketSupport.closeSocketWithRetry(socket)

  private[lightgbm] def withCleanupPreservingPrimary[T](cleanup: => Unit)(operation: => T): T =
    NetworkManagerSocketSupport.withCleanupPreservingPrimary(cleanup)(operation)

  private[lightgbm] def withCleanupOnFailurePreservingPrimary[T](cleanup: => Unit)(operation: => T): T =
    NetworkManagerSocketSupport.withCleanupOnFailurePreservingPrimary(cleanup)(operation)

  /**
    * Create a NetworkManager, which will encapsulate all network operations.
    * This method will opens a socket communications channel on the driver, and then initialize
    * the network manager itself.
    * The NetworkManager object will start a thread that waits for the host:port from the executors,
    * and then sends back the information to the executors.
    *
    * @param numTasks The total number of training tasks to wait for.
    * @param spark The Spark session.
    * @param driverListenPort The port to listen for the driver on.
    * @param timeout The timeout (in seconds).
    * @param useBarrierExecutionMode Whether to use barrier mode.
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
    if (duration.isFinite) {
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
    * The JVM reserves the selected port until immediately before LightGBM binds it in the C layer,
    * limiting competition to the unavoidable handoff between the two socket implementations.
    *
    * @param ctx Information about the current training session.
    * @param log The Logger.
    * @param taskId The task id.
    * @param partitionId The partition id.
    * @param shouldExecuteTraining Whether this task should be a part of the training network.
    * @param measures Instrumentation for perf measurements.
    * @return Information about the network topology.
    */
  def getGlobalNetworkInfo(ctx: TrainingContext,
                           log: Logger,
                           taskId: Long,
                           partitionId: Int,
                           shouldExecuteTraining: Boolean,
                           measures: TaskInstrumentationMeasures): NetworkTopologyInfo = {
    measures.markNetworkInitializationStart()
    val networkParams = ctx.networkParams
    try {
      val reservation = findOpenPort(ctx, log)
      withPortReservation(reservation, shouldExecuteTraining) {
        localListenPort =>
          log.info(s"LightGBM task $taskId connecting to host: " +
            s"${networkParams.ipAddress}, port: ${networkParams.port}")
          try {
            FaultToleranceUtils.retryWithTimeout() {
              getNetworkTopologyInfoFromDriver(networkParams,
                                               taskId,
                                               partitionId,
                                               localListenPort,
                                               log,
                                               shouldExecuteTraining)
            }
          } catch {
            case connectFailure: ConnectException =>
              throw driverUnreachableException(networkParams, taskId, partitionId, log, connectFailure)
          }
      }
    } finally {
      measures.markNetworkInitializationStop()
    }
  }

  /** Explain why a task could not reach the driver's network topology endpoint.
    *
    * The driver serves the topology exchange exactly once per training round and then closes its
    * server socket. A task that Spark retries after that point can therefore only ever see
    * "connection refused", which silently replaces the failure that caused the retry in the first
    * place. Naming that explicitly keeps the original failure discoverable.
    */
  private[lightgbm] def driverUnreachableException(networkParams: NetworkParams,
                                                   taskId: Long,
                                                   partitionId: Int,
                                                   log: Logger,
                                                   cause: ConnectException): Exception = {
    val attemptNumber = Option(TaskContext.get()).map(_.attemptNumber()).getOrElse(0)
    val endpoint = s"${networkParams.ipAddress}:${networkParams.port}"
    val message = if (attemptNumber > 0) {
      s"LightGBM task $taskId (partition $partitionId) could not reach the driver network topology endpoint " +
        s"$endpoint on retry attempt $attemptNumber. The driver serves the topology exchange once per training " +
        "round and has already closed it, so a retried task can never rejoin the LightGBM network. This error " +
        s"is therefore a consequence of an earlier failure: inspect the logs of the first failed attempt of " +
        s"partition $partitionId to find the real cause. Distributed LightGBM training cannot recover from a " +
        "partial task retry."
    } else {
      s"LightGBM task $taskId (partition $partitionId) could not reach the driver network topology endpoint " +
        s"$endpoint on its first attempt. Verify that executors are allowed to open connections to the driver " +
        "on that port, and that the driver was not shut down before training started."
    }
    log.error(message, cause)
    new Exception(message, cause)
  }

  private def getNetworkTopologyInfoFromDriver(networkParams: NetworkParams,
                                               taskId: Long,
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
            val stageAttemptNumber = Option(TaskContext.get()).map(_.stageAttemptNumber()).getOrElse(0)
            // A numeric IPv6 scope is an interface index that only means anything here, so it is
            // replaced with the interface name before any peer sees it.
            val taskStatus = TaskMessageInfo(
              if (shouldExecuteTraining) LightGBMConstants.EnabledTask else LightGBMConstants.IgnoreStatus,
              WorkerEndpoint.normalizeHost(driverSocket.getLocalAddress.getHostAddress),
              localListenPort,
              partitionId,
              LightGBMUtils.getExecutorId) // TODO can we use host for this?
            val message = WorkerMessage.format(taskStatus, stageAttemptNumber)
            log.info(s"task $taskId sending status message to driver: $message ")
            driverOutput.write(s"$message\n")
            driverOutput.flush()

            // If barrier execution mode enabled, create a barrier across tasks and send message when finished
            if (networkParams.barrierExecutionMode) {
              val context = BarrierTaskContext.get()
              context.barrier()
              if (context.partitionId() == 0) {
                 setFinishedStatus(networkParams, stageAttemptNumber, context.getTaskInfos().length, log)
              }
            }

            // Wait for response from driver.  It should send the final LightGBM network string,
            // and a list of partition ids in this executor.
            val lightGbmMachineList = driverInput.readLine()
            val partitionsByExecutorStr = driverInput.readLine()
            if (partitionsByExecutorStr == null || lightGbmMachineList == null) {
              val message = s"Received bad network information. Task $taskId, partition $partitionId received" +
                s"partition topology: '$partitionsByExecutorStr', nodes for network init: '$lightGbmMachineList'"
              throw new Exception(message)
            }

            log.info(s"task $taskId, partition $partitionId received partition topology: '$partitionsByExecutorStr'")
            log.info(s"task $taskId, partition $partitionId received nodes for network init: '$lightGbmMachineList'")
            val executorPartitionIds: Array[Int] =
              parseExecutorPartitionList(partitionsByExecutorStr, taskStatus.executorId, log)
            NetworkTopologyInfo(lightGbmMachineList, executorPartitionIds, localListenPort)
              .withAdvertisedHost(taskStatus.taskHost)
        }.get
    }.get
  }

  private def parseExecutorPartitionList(partitionsByExecutorStr: String,
                                         executorId: String,
                                         log: Logger): Array[Int] = {
    // extract this executors partition ids as an array, from a string that is formatter like this:
    // executor1=partition1,partition2:executor2=partition3,partition4
    val partitionsByExecutor = partitionsByExecutorStr.split(":")
    val executorListStr = partitionsByExecutor.find(line => line.startsWith(executorId + "="))
    if (executorListStr.isEmpty)
      throw new Exception(s"Could not find partitions for executor $executorId. List: $partitionsByExecutorStr")
    log.info(s"executor $executorId received partitions: '$executorListStr'")
    val partitionList = executorListStr.get.split("=")(1)
    partitionList.split(",").map(str => str.toInt).sorted
  }

  def initLightGBMNetwork(ctx: PartitionTaskContext,
                          log: Logger,
                          retry: Int = LightGBMConstants.NetworkRetries,
                          delay: Long = LightGBMConstants.InitialDelay): Unit = {
    initLightGBMNetworkWithRetry(
      ctx.networkTopologyInfo,
      log,
      retry,
      delay,
      () => initNativeNetwork(ctx.networkTopologyInfo, ctx.lightGBMNetworkMachineCount, log),
      port => reserveExactPort(port, log),
      delayMillis => Thread.sleep(delayMillis))
  }

  /** Initialize the native LightGBM network, bridging the transport when the topology is IPv6.
    *
    * Native LightGBM only speaks IPv4, so an IPv6 topology is relayed by [[LightGBMNetworkBridge]]
    * and the native library is given an equivalent loopback machine list. An IPv4 topology takes
    * exactly the same path it always has, with no bridge, no relay threads, and no extra sockets.
    */
  private[lightgbm] def initNativeNetwork(networkTopologyInfo: NetworkTopologyInfo,
                                          machineCount: Int,
                                          log: Logger,
                                          nativeInit: (String, Int, Int) => Unit = nativeNetworkInit): Unit = {
    val machineList = networkTopologyInfo.lightgbmNetworkString
    if (!LightGBMNetworkBridge.requiresBridge(machineList)) {
      nativeInit(machineList, networkTopologyInfo.localListenPort, machineCount)
    } else {
      log.info(s"LightGBM network $machineList contains IPv6 endpoints, which the native library cannot " +
        "dial, so this task is bridging the transport")
      val bridge = LightGBMNetworkBridge.open(machineList,
                                              networkTopologyInfo.taskHost,
                                              networkTopologyInfo.localListenPort,
                                              log)
      // A failed attempt has to give the advertised port back, because the retry re-reserves it.
      withCleanupOnFailurePreservingPrimary(bridge.close()) {
        val bridged = bridge.bridgedNetwork
        // A relay that has already failed can never carry this network, and the native call would
        // wait on links that will not arrive, so the attempt fails here instead.
        failIfBridgeIsBroken(bridge)
        nativeInit(bridged.machineList, bridged.localListenPort, bridged.machineCount)
        failIfBridgeIsBroken(bridge)
        networkTopologyInfo.retainNetworkBridge(bridge)
      }
    }
  }

  /** Surface a relay failure as this task's failure, rather than training on a dead transport. */
  private[lightgbm] def failIfBridgeIsBroken(bridge: LightGBMNetworkBridge): Unit = {
    bridge.terminalFailure.foreach { failure =>
      throw new Exception("The LightGBM IPv6 network bridge for this task failed, so its training " +
        s"network cannot be established: ${failure.getMessage}", failure)
    }
  }

  private def nativeNetworkInit(machineList: String, localListenPort: Int, machineCount: Int): Unit = {
    LightGBMUtils.validate(lightgbmlib.LGBM_NetworkInit(machineList,
                                                        localListenPort,
                                                        LightGBMConstants.DefaultListenTimeout,
                                                        machineCount), "Network init")
  }

  /** Retry native network initialization without leaving the advertised port open during backoff. */
  private[lightgbm] def initLightGBMNetworkWithRetry(networkTopologyInfo: NetworkTopologyInfo,
                                                     log: Logger,
                                                     retry: Int,
                                                     delay: Long,
                                                     networkInit: () => Unit,
                                                     reservePort: Int => Socket,
                                                     sleep: Long => Unit): Unit = {
    initLightGBMNetworkWithRetry(
      networkTopologyInfo,
      log,
      retry,
      delay,
      networkInit,
      reservePort,
      sleep,
      None)
  }

  private def initLightGBMNetworkWithRetry(networkTopologyInfo: NetworkTopologyInfo,
                                           log: Logger,
                                           retry: Int,
                                           delay: Long,
                                           networkInit: () => Unit,
                                           reservePort: Int => Socket,
                                           sleep: Long => Unit,
                                           previousNativeFailure: Option[Exception]): Unit = {
    val localListenPort = networkTopologyInfo.localListenPort
    log.info(s"Calling NetworkInit on local port $localListenPort " +
      s"with value ${networkTopologyInfo.lightgbmNetworkString}")
    releasePortReservationForNetworkInit(networkTopologyInfo, previousNativeFailure, log)
    val nativeFailure = try {
      networkInit()
      None
    } catch {
      case failure: Exception => Option(failure)
    }

    nativeFailure match {
      case None =>
        log.info(s"NetworkInit succeeded. LightGBM task listening on: $localListenPort")
      case Some(failure) =>
        log.info(s"NetworkInit failed with exception on local port $localListenPort " +
          s"with exception: $failure")
        if (retry == 0) {
          log.info(s"NetworkInit reached maximum exceptions on retry: $failure")
          throw failure
        }

        // Every peer already knows this port, so changing it would corrupt the negotiated topology.
        // If exact re-reservation loses the handoff race, fail the task and let Spark renegotiate.
        try {
          val retryReservation = reservePort(localListenPort)
          withPortReservation(retryReservation, shouldExecuteTraining = true) { _ =>
            networkTopologyInfo
          }
        } catch {
          case reservationFailure: Exception =>
            addSuppressed(failure, reservationFailure)
            throw failure
        }

        log.info(s"Retrying NetworkInit with local port $localListenPort")
        sleep(delay)
        initLightGBMNetworkWithRetry(
          networkTopologyInfo,
          log,
          retry - 1,
          delay * 2,
          networkInit,
          reservePort,
          sleep,
          Option(failure))
    }
  }

  private def releasePortReservationForNetworkInit(networkTopologyInfo: NetworkTopologyInfo,
                                                    previousNativeFailure: Option[Exception],
                                                    log: Logger): Unit = {
    try {
      networkTopologyInfo.releasePortReservation()
    } catch {
      case releaseFailure: IOException =>
        previousNativeFailure match {
          case Some(nativeFailure) =>
            addSuppressed(nativeFailure, releaseFailure)
            if (networkTopologyInfo.hasPortReservation) {
              throw nativeFailure
            }
            log.warn("Port reservation close reported a failure but ultimately closed; " +
              "continuing the native network-init retry", releaseFailure)
          case None => throw releaseFailure
        }
    }
  }

  /**
    * Gets the main node's port that will return the LightGBM Booster.
    * Used to minimize network communication overhead in reduce step.
    * @return The main node's port number.
    */
  private[lightgbm] def parseHostAndPort(endpoint: String): (String, Int) = {
    val parsed = WorkerEndpoint.parse(endpoint)
    (parsed.host, parsed.port)
  }

  def getMainWorkerPort(nodes: String, log: Logger): Int = {
    val mainWorker = WorkerEndpoint.parseFirst(nodes)
    log.info(s"LightGBM setting main worker host: ${mainWorker.host} and port: ${mainWorker.port}")
    mainWorker.port
  }

  private def findOpenPort(ctx: TrainingContext, log: Logger): Socket = {
    val defaultListenPort: Int = ctx.networkParams.defaultListenPort
    val basePort = defaultListenPort + (LightGBMUtils.getWorkerId * ctx.numTasksPerExecutor)
    reserveOpenPort(basePort, log)
  }

  private[lightgbm] def reserveOpenPort(basePort: Int, log: Logger): Socket =
    NetworkManagerSocketSupport.reserveOpenPort(basePort, log)

  private[lightgbm] def reserveOpenPort(basePort: Int,
                                        log: Logger,
                                        createSocket: () => Socket): Socket =
    NetworkManagerSocketSupport.reserveOpenPort(basePort, log, createSocket)

  private[lightgbm] def reserveExactPort(localListenPort: Int, log: Logger): Socket =
    NetworkManagerSocketSupport.reserveExactPort(localListenPort, log)

  private[lightgbm] def withPortReservation(reservation: Socket,
                                            shouldExecuteTraining: Boolean)
                                           (getTopology: Int => NetworkTopologyInfo): NetworkTopologyInfo =
    NetworkManagerSocketSupport.withPortReservation(reservation, shouldExecuteTraining)(getTopology)

  private def setFinishedStatus(networkParams: NetworkParams,
                                stageAttemptNumber: Int,
                                barrierTaskCount: Int,
                                log: Logger): Unit = {
    using(new Socket(networkParams.ipAddress, networkParams.port)) {
      driverSocket =>
        using(new BufferedWriter(new OutputStreamWriter(driverSocket.getOutputStream))) {
          driverOutput =>
            log.info(s"sending finished status to driver for $barrierTaskCount barrier tasks")
            // The barrier task count tells the driver how many topology reports to expect. It can be
            // smaller than numTasks, because barrier mode never repartitions upwards when the input
            // has fewer partitions than numTasks.
            driverOutput.write(s"${WorkerMessage.formatFinished(stageAttemptNumber, barrierTaskCount)}\n")
            driverOutput.flush()
        }.get
    }.get
  }

  def parseWorkerMessage(message: String): TaskMessageInfo = {
    WorkerMessage.parse(message).toTaskMessage
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

  private final class TaskConnection(val socket: Socket, val message: WorkerMessage) {
    // The machine list is comma delimited and every entry is host:port, so an IPv6 host has to be
    // bracketed here or peers would read its trailing group as the port.
    def networkInfoString: String = WorkerEndpoint.wireString(message.taskHost, message.localListenPort)
  }

  // Spark can retry a task report within the same stage attempt. Keeping one connection per
  // partition prevents duplicates from satisfying or permanently overshooting the round count.
  private val taskConnectionsByPartition = mutable.Map[Int, TaskConnection]()

  // The Spark stage attempt whose topology is currently being collected. A barrier stage restarts
  // as a whole, so a higher attempt number means everything gathered so far is obsolete.
  private var currentStageAttempt = -1
  private var finishedForCurrentStageAttempt = false
  private var expectedTaskCountForCurrentStageAttempt: Option[Int] = None
  private var acceptedSocket: Option[Socket] = None
  @volatile private var shutdownRequested = false

  // Concatenate with commas, eg: host1:port1,host2:port2, ... etc
  // Also make sure the order is deterministic by sorting on minimum partition id
  private def networkTopologyAsString: String = synchronized {
    val connections = taskConnectionsByPartition.values.toSeq
    val minPartitionByHost = connections.groupBy(_.message.taskHost).map { case (taskHost, hostConnections) =>
      taskHost -> hostConnections.map(_.message.partitionId).min
    }
    connections
      .filter(_.message.isForTraining)
      .sortBy(connection =>
        (minPartitionByHost(connection.message.taskHost), connection.message.partitionId))
      .map(_.networkInfoString)
      .mkString(",")
  }

  // Create a string representing of the partitionsByExecutor map
  // e.g. executor1=partition1,partition2:executor2=partition3,partition4
  private def partitionsByExecutorAsString: String = synchronized {
    taskConnectionsByPartition.values
      .groupBy(_.message.executorId)
      .toSeq
      .sortBy(_._1)
      .map { case (executorId, connections) =>
        s"$executorId=${connections.map(_.message.partitionId).toSeq.sorted.mkString(",")}"
      }
      .mkString(":")
  }

  private val networkCommunicationExecutor: ExecutorService = Executors.newSingleThreadExecutor()

  // This will be kicked off at object creation time, and can be waited on by waitForNetworkDone()
  private val networkCommunicationThread: Future[Unit] = Future {
    try {
      log.info(s"driver waiting for connections on host: $host and port: $port")
      if (useBarrierExecutionMode) serveTopologyRoundsUntilShutdown() else serveTopologyRound()
    } finally {
      // Always release the sockets, including when the topology exchange fails or times out.
      closeConnections()
      // Release the dedicated thread so repeated fits on a long-lived driver do not leak threads.
      networkCommunicationExecutor.shutdown()
    }
  } (ExecutionContext.fromExecutor(networkCommunicationExecutor))

  private def serveTopologyRound(): Unit = {
    waitForAllTasksToReport()

    // We have all the information now, so report back to workers
    sendDataToExecutors(networkTopologyAsString, partitionsByExecutorAsString)
  }

  /** Serves topology rounds until the training job says it is done.
    *
    * A barrier stage is restarted in its entirety when any of its tasks fails, which is the only way
    * a LightGBM network can legitimately re-form. Serving a single round means the restarted stage
    * finds a closed port and dies with "connection refused", so keep listening instead.
    */
  @tailrec
  private def serveTopologyRoundsUntilShutdown(): Unit = {
    val keepServing =
      try {
        serveTopologyRound()
        resetRoundState()
        true
      } catch {
        case _: SocketTimeoutException =>
          // Training can easily outlast the socket timeout, so an idle driver is not an error here.
          log.info("driver saw no task connections within the timeout, still listening for a stage restart")
          true
        case socketFailure: SocketException =>
          if (!shutdownRequested) throw socketFailure
          log.info("driver stopped serving topology rounds")
          false
      }
    if (keepServing) serveTopologyRoundsUntilShutdown()
  }

  def waitForNetworkCommunicationsDone(): Unit = {
    // In barrier mode the driver keeps listening for a restarted stage, so it never stops on its own.
    if (useBarrierExecutionMode) closeConnections()
    Await.result(networkCommunicationThread, Duration(timeout, SECONDS))
  }

  private def waitForAllTasksToReport(): Unit = {
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

      // Count the tasks actually recorded rather than the connections seen, so that connections
      // discarded as belonging to a superseded stage attempt do not end the round early.
      @tailrec
      def connectToWorkers(): Unit = {
        handleNextWorkerConnection()
        if (reportedTaskCount < numTasks) connectToWorkers()
      }

      connectToWorkers()
    }
  }

  /** Handles the connection to a task from the driver.
    *
    * @return Whether the current barrier round has both its Finished marker and every task report.
    *         Always false for non-barrier mode.
    */
  private def handleNextWorkerConnection(): Boolean = {
    log.info("driver accepting a new connection...")

    val socket = driverServerSocket.accept()  // block until connection is made
    if (!registerAcceptedSocket(socket)) {
      closeQuietly(socket)
      false
    } else {
      try {
        val reader = new BufferedReader(new InputStreamReader(socket.getInputStream))
        val messageStr = reader.readLine()
        log.info(s"received worker message string: $messageStr")
        processWorkerConnection(socket, WorkerMessage.parse(messageStr))
      } catch {
        case failure: Throwable =>
          closeAcceptedSocket(socket)
          throw failure
      }
    }
  }

  private def processWorkerConnection(socket: Socket, message: WorkerMessage): Boolean = synchronized {
    if (shutdownRequested) {
      closeAcceptedSocket(socket)
      false
    } else if (message.stageAttemptNumber < currentStageAttempt) {
      // A straggler from a stage attempt that Spark has already abandoned. Recording it would put a
      // dead host:port into the topology that every surviving task then tries to connect to.
      log.info(s"driver ignoring message from superseded stage attempt ${message.stageAttemptNumber}")
      closeAcceptedSocket(socket)
      false
    } else {
      startNewStageAttemptIfNeeded(message.stageAttemptNumber)
      recordWorkerConnection(socket, message)
    }
  }

  private def startNewStageAttemptIfNeeded(stageAttemptNumber: Int): Unit = {
    if (stageAttemptNumber > currentStageAttempt) {
      if (currentStageAttempt >= 0) {
        log.info(s"driver starting topology round for stage attempt $stageAttemptNumber, " +
          s"discarding the partial topology collected for attempt $currentStageAttempt")
        resetRoundState()
      }
      currentStageAttempt = stageAttemptNumber
    }
  }

  private def recordWorkerConnection(socket: Socket, message: WorkerMessage): Boolean = {
    if (message.isFinished) {
      log.info(s"driver received finished marker from barrier stage for ${message.barrierTaskCount} tasks")
      finishedForCurrentStageAttempt = true
      message.barrierTaskCount.filter(_ > 0).foreach(count => expectedTaskCountForCurrentStageAttempt = Some(count))
      closeAcceptedSocket(socket)  // The finished message uses its own short-lived connection.
    } else {
      recordTaskConnection(socket, message)
    }

    val roundComplete = barrierRoundComplete
    if (roundComplete) log.info("driver received all task reports and the finished marker from barrier stage")
    roundComplete
  }

  /**
    * The finished marker only tells the driver that the barrier stage synchronized; the task reports
    * can still be sitting in the accept backlog, so completing on the marker alone risks broadcasting
    * a partial topology. Wait for as many reports as the barrier stage actually ran, which the marker
    * carries. That count is not always numTasks: barrier mode never repartitions upwards, so a user
    * who sets numTasks above the input's partition count runs fewer tasks, and waiting for numTasks
    * reports would hang until Spark's barrier timeout.
    */
  private def barrierRoundComplete: Boolean = {
    if (!useBarrierExecutionMode || !finishedForCurrentStageAttempt) {
      false
    } else {
      expectedTaskCountForCurrentStageAttempt match {
        case Some(expected) => reportedTaskCount >= math.min(expected, numTasks)
        // A sender that did not report a count leaves the marker as the only signal available.
        case None => true
      }
    }
  }

  private def recordTaskConnection(socket: Socket, message: WorkerMessage): Unit = {
    if (message.partitionId < 0 || message.partitionId >= numTasks) {
      throw new Exception(s"Unexpected partition id ${message.partitionId}; expected a value in [0, $numTasks)")
    }

    val connection = new TaskConnection(socket, message)
    message match {
      case m if m.isForLoadOnly =>
        log.info("driver received load-only status from task")
      case m if m.isForTraining =>
        log.info(s"driver received socket from task: ${connection.networkInfoString}")
      case _ => throw new Exception(s"Unknown message type: ${message.status}")
    }

    val previousConnection = taskConnectionsByPartition.put(message.partitionId, connection)
    acceptedSocket = None
    previousConnection.foreach { previous =>
      log.info(s"driver replacing duplicate report for partition ${message.partitionId}")
      if (previous.socket ne socket) closeQuietly(previous.socket)
    }
  }

  private def sendDataToExecutors(lightGBMNetworkTopology: String, partitionsByExecutor: String): Unit = {
    // TODO optimize and not send for bulk mode helpers
    // Send aggregated network information back to all tasks and helper tasks on executors
    val sockets = synchronized {
      taskConnectionsByPartition.values.map(_.socket).toSeq
    }
    val count = sockets.length
    log.info(s"driver writing back network topology to $count connections: $lightGBMNetworkTopology")
    log.info(s"driver writing back partition topology to $count connections: $partitionsByExecutor")
    sockets.foreach(socket => {
      val writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream))
      writer.write(lightGBMNetworkTopology + "\n")
      writer.write(partitionsByExecutor + "\n")
      writer.flush()
    })
  }

  /** Release every driver-side socket for this training round.
    *
    * Safe to call more than once and from more than one thread, so both the network thread and the
    * training job can guarantee cleanup. Closing the server socket also unblocks a network thread
    * that is still parked in accept() waiting for tasks that will never arrive.
    */
  private[lightgbm] def closeConnections(): Unit = synchronized {
    shutdownRequested = true
    log.info("driver closing all sockets and server socket")
    acceptedSocket.foreach(closeQuietly)
    acceptedSocket = None
    closeRoundSockets()
    closeQuietly(driverServerSocket)
    log.info("driver done closing all sockets and server socket")
  }

  /** Number of tasks whose topology has been recorded for the current stage attempt. */
  private def reportedTaskCount: Int = synchronized {
    taskConnectionsByPartition.size
  }

  /** Discards everything gathered for a stage attempt so the next one starts from a clean slate. */
  private def resetRoundState(): Unit = synchronized {
    closeRoundSockets()
    taskConnectionsByPartition.clear()
    finishedForCurrentStageAttempt = false
    expectedTaskCountForCurrentStageAttempt = None
  }

  private def closeRoundSockets(): Unit = synchronized {
    taskConnectionsByPartition.values.foreach(connection => closeQuietly(connection.socket))
  }

  /** Tracks the socket between accept() and its transfer into the current round's socket buffers. */
  private def registerAcceptedSocket(socket: Socket): Boolean = synchronized {
    if (shutdownRequested) {
      false
    } else {
      acceptedSocket = Some(socket)
      true
    }
  }

  private def closeAcceptedSocket(socket: Socket): Unit = synchronized {
    if (acceptedSocket.contains(socket)) {
      acceptedSocket = None
      closeQuietly(socket)
    }
  }

  private def closeQuietly(closeable: java.io.Closeable): Unit = {
    try {
      closeable.close()
    } catch {
      case NonFatal(closeFailure) =>
        // One socket refusing to close must not strand the others, especially the server socket.
        log.warn("driver could not close a network socket cleanly", closeFailure)
    }
  }
}
