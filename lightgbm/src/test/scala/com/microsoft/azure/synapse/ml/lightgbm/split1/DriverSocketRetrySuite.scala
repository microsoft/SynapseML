// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split1

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.lightgbm.{LightGBMConstants, LightGBMMissingTasksException, NetworkManager,
  NetworkParams, TaskMessageInfo, WorkerMessage, WorkerTaskIdentity}
import org.apache.spark.TaskContext
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.LoggerFactory

import java.io.{BufferedReader, BufferedWriter, IOException, InputStreamReader, OutputStreamWriter}
import java.net.{ConnectException, InetSocketAddress, ServerSocket, Socket, SocketException, SocketTimeoutException}
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.{Duration, SECONDS}

/** Covers the driver topology socket lifecycle behind repeated
  * "java.net.ConnectException: Connection refused" failures in distributed LightGBM training.
  */
class DriverSocketRetrySuite extends AnyFunSuite {

  private val timeout = 30.0
  private val socketTimeoutMillis = 30000
  private val host = "127.0.0.1"

  private class FakeTask(host: String,
                         port: Int,
                         partitionId: Int,
                         loadOnly: Boolean = false,
                         listenPortOffset: Int = 0,
                         executorId: String = "")
    extends AutoCloseable {
    private val socket = {
      val result = new Socket()
      try {
        result.connect(new InetSocketAddress(host, port), socketTimeoutMillis)
        result.setSoTimeout(socketTimeoutMillis)
        result
      } catch {
        case failure: Throwable =>
          try result.close()
          catch {
            case closeFailure: IOException => failure.addSuppressed(closeFailure)
          }
          throw failure
      }
    }
    private val reader = new BufferedReader(new InputStreamReader(socket.getInputStream))
    private val writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream))

    def report(): Unit = {
      val status = if (loadOnly) LightGBMConstants.IgnoreStatus else LightGBMConstants.EnabledTask
      val reportedExecutorId = if (executorId.isEmpty) partitionId.toString else executorId
      writer.write(s"$status:127.0.0.1:$listenPort:$partitionId:$reportedExecutorId\n")
      writer.flush()
    }

    val listenPort: Int = LightGBMConstants.DefaultLocalListenPort + partitionId + listenPortOffset
    def address: String = s"127.0.0.1:$listenPort"

    def readTopology(): (String, String) = (reader.readLine(), reader.readLine())

    /** The driver closing its end is observed here as end-of-stream. */
    def isClosedByDriver: Boolean = {
      try {
        reader.readLine() == null
      } catch {
        case _: SocketException => true
      }
    }

    override def close(): Unit = socket.close()
  }

  private class SignalSecondAcceptServerSocket extends ServerSocket(0) {
    private val acceptCount = new AtomicInteger()
    private val secondAcceptStarted = new CountDownLatch(1)
    setSoTimeout(socketTimeoutMillis)

    override def accept(): Socket = {
      if (acceptCount.incrementAndGet() == 2) secondAcceptStarted.countDown()
      super.accept()
    }

    def awaitSecondAccept(): Unit = {
      assert(secondAcceptStarted.await(socketTimeoutMillis, TimeUnit.MILLISECONDS),
        "The driver never registered the first task socket")
    }
  }

  private class GateAfterAcceptServerSocket extends ServerSocket(0) {
    private val socketAccepted = new CountDownLatch(1)
    private val releaseAcceptedSocket = new CountDownLatch(1)
    @volatile private var socket: Socket = _
    setSoTimeout(socketTimeoutMillis)

    override def accept(): Socket = {
      val result = super.accept()
      socket = result
      socketAccepted.countDown()
      if (!releaseAcceptedSocket.await(socketTimeoutMillis, TimeUnit.MILLISECONDS)) {
        result.close()
        throw new SocketTimeoutException("Timed out waiting to release the accepted socket")
      }
      result
    }

    def awaitAccepted(): Unit = {
      assert(socketAccepted.await(socketTimeoutMillis, TimeUnit.MILLISECONDS),
        "The driver never accepted the task socket")
    }

    def release(): Unit = releaseAcceptedSocket.countDown()

    def acceptedSocketIsClosed: Boolean = socket != null && socket.isClosed
  }

  private class GateUnexpectedAcceptFailureServerSocket extends SignalSecondAcceptServerSocket {
    private val acceptFailureObserved = new CountDownLatch(1)
    private val releaseAcceptFailure = new CountDownLatch(1)

    override def accept(): Socket = {
      try {
        super.accept()
      } catch {
        case failure: SocketException =>
          acceptFailureObserved.countDown()
          if (!releaseAcceptFailure.await(socketTimeoutMillis, TimeUnit.MILLISECONDS)) {
            failure.addSuppressed(new SocketTimeoutException("Timed out waiting to release accept failure"))
          }
          throw failure
      }
    }

    def awaitAcceptFailure(): Unit = {
      assert(acceptFailureObserved.await(socketTimeoutMillis, TimeUnit.MILLISECONDS),
        "The driver's blocked accept did not observe the unexpected server close")
    }

    def releaseFailure(): Unit = releaseAcceptFailure.countDown()
  }

  private def newManager(numTasks: Int,
                         useBarrierExecutionMode: Boolean = false,
                         serverSocket: ServerSocket = new ServerSocket(0)): (NetworkManager, String, Int) = {
    serverSocket.setSoTimeout(socketTimeoutMillis)
    val port = serverSocket.getLocalPort
    (NetworkManager(numTasks, serverSocket, host, port, timeout, useBarrierExecutionMode), host, port)
  }

  private def closeTasks(tasks: Iterable[FakeTask]): Unit = {
    tasks.foreach { task =>
      try task.close()
      catch {
        case _: IOException => ()
      }
    }
  }

  private def partitionIds(executorTopology: String): Seq[Int] = {
    executorTopology.split(":").flatMap { executorEntry =>
      executorEntry.split("=")(1).split(",").map(_.toInt)
    }.toSeq
  }

  private def runTopologyRound(numTasks: Int,
                               numLoadOnlyTasks: Int = 0): (NetworkManager, String, Int, Seq[FakeTask]) = {
    val (manager, host, port) = newManager(numTasks + numLoadOnlyTasks)
    val tasks = ListBuffer.empty[FakeTask]
    try {
      (0 until numTasks).foreach(partitionId => tasks += new FakeTask(host, port, partitionId))
      (0 until numLoadOnlyTasks).foreach { index =>
        tasks += new FakeTask(host, port, numTasks + index, loadOnly = true)
      }
      tasks.foreach(_.report())
      tasks.foreach { task =>
        val (machineList, partitionList) = task.readTopology()
        assert(machineList != null && machineList.nonEmpty)
        assert(partitionList != null && partitionList.nonEmpty)
      }
      manager.waitForNetworkCommunicationsDone()
      (manager, host, port, tasks.toList)
    } catch {
      case failure: Throwable =>
        manager.closeConnections()
        closeTasks(tasks)
        throw failure
    }
  }

  test("A retried task is refused by the driver once the topology round has completed") {
    val (manager, host, port, tasks) = runTopologyRound(numTasks = 2)
    try {
      // A Spark task retry re-enters getGlobalNetworkInfo and reconnects to the same driver endpoint.
      val retriedSocket = new Socket()
      val thrown = try {
        intercept[ConnectException] {
          retriedSocket.connect(new InetSocketAddress(host, port), socketTimeoutMillis)
        }
      } finally {
        retriedSocket.close()
      }

      assert(thrown.getMessage.toLowerCase.contains("refused"),
        s"Expected a connection-refused failure but got: ${thrown.getMessage}")
    } finally {
      manager.closeConnections()
      closeTasks(tasks)
    }
  }

  test("Helper task sockets are released along with training task sockets") {
    val (manager, _, _, tasks) = runTopologyRound(numTasks = 2, numLoadOnlyTasks = 2)
    try {
      tasks.zipWithIndex.foreach { case (task, index) =>
        assert(task.isClosedByDriver, s"Driver leaked the socket for task $index")
      }
    } finally {
      manager.closeConnections()
      closeTasks(tasks)
    }
  }

  test("Non-barrier topology waits for every unique partition when a report is retried") {
    val (manager, _, port) = newManager(numTasks = 2)
    var tasks = Seq.empty[FakeTask]
    try {
      val original = new FakeTask(
        host, port, partitionId = 0, loadOnly = true, executorId = "old-executor")
      tasks :+= original
      val replacement = new FakeTask(
        host, port, partitionId = 0, listenPortOffset = 1000, executorId = "new-executor")
      tasks :+= replacement

      original.report()
      replacement.report()
      assert(original.isClosedByDriver, "The superseded partition report socket was not closed")

      val secondPartition = new FakeTask(host, port, partitionId = 1)
      tasks :+= secondPartition
      secondPartition.report()

      val expectedNetwork = Set(replacement.address, secondPartition.address)
      Seq(replacement, secondPartition).foreach { task =>
        val (networkTopology, executorTopology) = task.readTopology()
        val networkNodes = networkTopology.split(",").toSeq
        assert(networkNodes.size == expectedNetwork.size)
        assert(networkNodes.toSet == expectedNetwork)
        assert(partitionIds(executorTopology).sorted == Seq(0, 1))
        assert(!executorTopology.contains("old-executor"))
        assert(executorTopology.contains("new-executor=0"))
      }
      manager.waitForNetworkCommunicationsDone()
    } finally {
      manager.closeConnections()
      closeTasks(tasks)
    }
  }

  test("closeConnections remains idempotent while a topology round owns task sockets") {
    val serverSocket = new SignalSecondAcceptServerSocket()
    val (manager, _, port) =
      newManager(numTasks = 2, useBarrierExecutionMode = true, serverSocket = serverSocket)
    var task = Option.empty[FakeTask]
    try {
      task = Some(new FakeTask(host, port, partitionId = 0))
      task.get.report()
      serverSocket.awaitSecondAccept()

      manager.closeConnections()
      manager.closeConnections()

      assert(task.get.isClosedByDriver, "The driver leaked a task socket during repeated cleanup")
      manager.waitForNetworkCommunicationsDone()
    } finally {
      manager.closeConnections()
      closeTasks(task)
    }
  }

  test("An unexpected server close preserves the accept failure and still closes task sockets") {
    val serverSocket = new GateUnexpectedAcceptFailureServerSocket()
    val (manager, _, port) =
      newManager(numTasks = 2, useBarrierExecutionMode = true, serverSocket = serverSocket)
    var task = Option.empty[FakeTask]
    try {
      task = Some(new FakeTask(host, port, partitionId = 0))
      task.get.report()
      serverSocket.awaitSecondAccept()

      serverSocket.close()
      serverSocket.awaitAcceptFailure()
      serverSocket.releaseFailure()

      assert(task.get.isClosedByDriver, "The unexpected server close leaked a task socket")
      intercept[SocketException] {
        manager.waitForNetworkCommunicationsDone()
      }
    } finally {
      serverSocket.releaseFailure()
      manager.closeConnections()
      closeTasks(task)
    }
  }

  test("A socket accepted during shutdown is rejected before round registration") {
    val serverSocket = new GateAfterAcceptServerSocket()
    val (manager, _, port) =
      newManager(numTasks = 1, useBarrierExecutionMode = true, serverSocket = serverSocket)
    var task = Option.empty[FakeTask]
    try {
      task = Some(new FakeTask(host, port, partitionId = 0))
      task.get.report()
      serverSocket.awaitAccepted()

      manager.closeConnections()
      serverSocket.release()

      assert(task.get.isClosedByDriver, "The socket accepted during shutdown was retained")
      manager.waitForNetworkCommunicationsDone()
      assert(serverSocket.acceptedSocketIsClosed, "The driver did not close the accepted socket")
    } finally {
      serverSocket.release()
      manager.closeConnections()
      closeTasks(task)
    }
  }

  test("The legacy TaskMessageInfo constructors, extractor, and product shape remain unchanged") {
    val message = TaskMessageInfo(
      LightGBMConstants.EnabledTask,
      "127.0.0.1",
      LightGBMConstants.DefaultLocalListenPort,
      3,
      "executor-1")
    val generalMessage = new TaskMessageInfo(LightGBMConstants.FinishedStatus)

    val TaskMessageInfo(status, taskHost, listenPort, partitionId, executorId) = message
    assert(status == LightGBMConstants.EnabledTask)
    assert(taskHost == "127.0.0.1")
    assert(listenPort == LightGBMConstants.DefaultLocalListenPort)
    assert(partitionId == 3)
    assert(executorId == "executor-1")
    assert(message.productArity == 5)
    assert(message.toString ==
      s"${LightGBMConstants.EnabledTask}:127.0.0.1:${LightGBMConstants.DefaultLocalListenPort}:3:executor-1")
    assert(NetworkManager.parseWorkerMessage(s"${message.toString}:7") == message)
    assert(generalMessage.isFinished)
    assert(generalMessage.productArity == 5)
    val constructorArities = classOf[TaskMessageInfo].getConstructors.map(_.getParameterCount).toSet
    assert(constructorArities.contains(1))
    assert(constructorArities.contains(5))
    assert(!constructorArities.contains(6))
  }

  test("Worker diagnostics name every Spark attempt coordinate without relabeling fields") {
    val previous = WorkerMessage(LightGBMConstants.EnabledTask, "10.0.0.4", 12400, 3, "executor-1", 7,
      stageId = Some(41), taskAttemptId = Some(9876543210123L), attemptNumber = Some(1))
    val replacement = previous.copy(taskAttemptId = Some(9876543210456L), attemptNumber = Some(2))
    val expectedPrevious = "stageId=41, stageAttemptNumber=7, partitionId=3, " +
      "taskAttemptId=9876543210123, attemptNumber=1"
    val expectedReplacement = "stageId=41, stageAttemptNumber=7, partitionId=3, " +
      "taskAttemptId=9876543210456, attemptNumber=2"

    assert(NetworkManager.supersededWorkerDiagnostic(previous) ==
      s"driver ignoring message from superseded stage attempt 7; $expectedPrevious")
    assert(NetworkManager.duplicateWorkerDiagnostic(previous, replacement) ==
      s"driver replacing duplicate report for partition 3; previous $expectedPrevious; " +
        s"replacement $expectedReplacement")
  }

  test("An unreachable driver reports the same complete Spark attempt identity") {
    val identity = WorkerTaskIdentity(Some(41), 7, Some(9876543210123L), Some(2))
    val cause = new ConnectException("Connection refused")
    val failure = NetworkManager.driverUnreachableException(
      NetworkParams(12400, "127.0.0.1", 12401, barrierExecutionMode = false),
      3,
      identity,
      LoggerFactory.getLogger(getClass),
      cause)

    assert(failure.getCause eq cause)
    assert(failure.getMessage.contains(
      "stageId=41, stageAttemptNumber=7, partitionId=3, taskAttemptId=9876543210123, attemptNumber=2"))
    assert(failure.getMessage.contains("inspect the logs of the first failed attempt of partition 3"))
  }

  test("A real Spark task snapshots one self-consistent attempt identity") {
    TestBase.resetSparkSession(numCores = Some(1))
    try {
      val spark = TestBase.spark
      val identity = spark.sparkContext.parallelize(Seq(1), 1).mapPartitions { _ =>
        val taskContext = TaskContext.get()
        Iterator(NetworkManager.currentWorkerTaskIdentity(taskContext.taskAttemptId()))
      }.collect().head

      assert(identity.stageId.exists(_ >= 0))
      assert(identity.stageAttemptNumber == 0)
      assert(identity.taskAttemptId.exists(_ >= 0))
      assert(identity.attemptNumber.contains(0))
    } finally {
      TestBase.resetSparkSession()
    }
  }

  test("Worker status parsing preserves IPv6 hosts in current and legacy messages") {
    val hosts = Seq("2001:db8::1", "2001:db8:0:1:2:3:4:5", "fe80::a%3")
    hosts.foreach { taskHost =>
      val message = TaskMessageInfo(
        LightGBMConstants.EnabledTask,
        taskHost,
        LightGBMConstants.DefaultLocalListenPort,
        3,
        "executor-1")
      assert(NetworkManager.parseWorkerMessage(s"${message.toString}:7") == message)
    }

    val legacyMessage = TaskMessageInfo(
      LightGBMConstants.EnabledTask,
      "2001:db8::1",
      LightGBMConstants.DefaultLocalListenPort,
      3,
      "7")
    assert(NetworkManager.parseWorkerMessage(legacyMessage.toString) == legacyMessage)

    val ambiguousLegacyMessage = legacyMessage.copy(taskHost = "2001:db8::1:10")
    assert(NetworkManager.parseWorkerMessage(ambiguousLegacyMessage.toString) == ambiguousLegacyMessage)

    val lowPortCurrentMessage = legacyMessage.copy(localListenPort = 80, executorId = "executor-1")
    assert(NetworkManager.parseWorkerMessage(WorkerMessage.format(lowPortCurrentMessage, 7)) == lowPortCurrentMessage)
  }

  test("Finished worker messages tolerate omitted trailing fields") {
    val missingBarrierCount = WorkerMessage.parse(s"${LightGBMConstants.FinishedStatus}:7:")
    assert(missingBarrierCount.stageAttemptNumber == 7)
    assert(missingBarrierCount.barrierTaskCount.isEmpty)

    val missingSuffix = WorkerMessage.parse(s"${LightGBMConstants.FinishedStatus}::")
    assert(missingSuffix.stageAttemptNumber == 0)
    assert(missingSuffix.barrierTaskCount.isEmpty)
  }

  test("Malformed worker messages escape control characters in errors") {
    val nul = 0.toChar
    val failure = intercept[IllegalArgumentException] {
      NetworkManager.parseWorkerMessage(s"enabledTask:host:not-a-port:3:executor-1\r${nul}malformed")
    }
    assert(failure.getMessage.contains("\\r"))
    assert(failure.getMessage.contains("\\u0000"))
    assert(!failure.getMessage.contains("\r"))
    assert(!failure.getMessage.contains(nul))
  }

  test("A worker that disconnects before sending a message reports the disconnect, not a NullPointerException") {
    val failure = intercept[IOException](NetworkManager.parseWorkerMessage(null))  //scalastyle:ignore null
    assert(failure.getMessage.contains("closed the connection before sending a status message"))
  }

  test("Non-barrier topology names the missing partitions when the driver stops waiting") {
    val serverSocket = new ServerSocket(0)
    val port = serverSocket.getLocalPort
    // The accept timeout ends the round quickly; the manager timeout only bounds the wait below.
    serverSocket.setSoTimeout(500)
    val manager = NetworkManager(3, serverSocket, host, port, timeout, useBarrierExecutionMode = false)
    var task = Option.empty[FakeTask]
    try {
      task = Some(new FakeTask(host, port, partitionId = 1))
      task.get.report()

      val failure = intercept[LightGBMMissingTasksException] {
        manager.waitForNetworkCommunicationsDone()
      }
      assert(failure.getMessage.contains("from 1 of 3 training tasks"))
      assert(failure.getMessage.contains("Missing partitions: 0, 2."))
      assert(failure.getMessage.contains("numTasks"))
      assert(failure.getCause.isInstanceOf[SocketTimeoutException])
      assert(task.get.isClosedByDriver, "The driver kept the reported task waiting after giving up")
      assert(manager.missingTasksFailure(Duration(5, SECONDS)).contains(failure))
    } finally {
      manager.closeConnections()
      closeTasks(task)
    }
  }

  test("Other topology failures are not reported as missing tasks") {
    val (manager, _, port) = newManager(numTasks = 2)
    var task = Option.empty[FakeTask]
    try {
      task = Some(new FakeTask(host, port, partitionId = 0))
      task.get.report()

      // The training job failing first closes the driver socket, which is not a missing-task timeout.
      assert(manager.missingTasksFailure(Duration(5, SECONDS)).isEmpty)
    } finally {
      manager.closeConnections()
      closeTasks(task)
    }
  }

  test("The driver server socket is released when a training job fails before the round completes") {
    // Only one of the two expected tasks reports, so the network thread stays blocked in accept().
    val (manager, _, port) = newManager(numTasks = 2, useBarrierExecutionMode = true)
    var task = Option.empty[FakeTask]
    try {
      task = Some(new FakeTask(host, port, 0))
      task.get.report()

      // This is what executeTraining now does in its finally block when partition tasks fail.
      manager.closeConnections()
      manager.waitForNetworkCommunicationsDone()

      val retriedSocket = new Socket()
      try {
        intercept[ConnectException] {
          retriedSocket.connect(new InetSocketAddress(host, port), socketTimeoutMillis)
        }
      } finally {
        retriedSocket.close()
      }
    } finally {
      manager.closeConnections()
      closeTasks(task)
    }
  }
}
