// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split1

import com.microsoft.azure.synapse.ml.lightgbm.{LightGBMConstants, NetworkManager}
import org.scalatest.funsuite.AnyFunSuite

import java.io.{BufferedReader, BufferedWriter, IOException, InputStreamReader, OutputStreamWriter}
import java.net.{InetSocketAddress, ServerSocket, Socket, SocketException, SocketTimeoutException}

/** Covers recovery of the driver topology exchange when Spark restarts a barrier stage.
  *
  * A barrier stage is restarted in its entirety when any of its tasks fails, so it is the only case
  * where a LightGBM network can legitimately be re-formed. The driver has to serve a second topology
  * round for the new stage attempt, and must not mix it with the topology of the abandoned one.
  */
class BarrierNetworkRecoverySuite extends AnyFunSuite {

  private val timeout = 30.0
  private val socketTimeoutMillis = 30000
  private val noResponseTimeoutMillis = 250
  private val host = "127.0.0.1"

  private class FakeBarrierTask(port: Int,
                                partitionId: Int,
                                stageAttempt: Int,
                                loadOnly: Boolean = false,
                                listenPortOffset: Int = 0,
                                executorId: String = "") extends AutoCloseable {
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

    val listenPort: Int =
      LightGBMConstants.DefaultLocalListenPort + stageAttempt * 100 + partitionId + listenPortOffset
    def address: String = s"$host:$listenPort"

    def report(): Unit = {
      val status = if (loadOnly) LightGBMConstants.IgnoreStatus else LightGBMConstants.EnabledTask
      val reportedExecutorId = if (executorId.isEmpty) s"executor$partitionId" else executorId
      writer.write(
        s"$status:$host:$listenPort:$partitionId:$reportedExecutorId:$stageAttempt\n")
      writer.flush()
    }

    def readTopology(): String = reader.readLine()

    def readExecutorTopology(): String = reader.readLine()

    def assertNoTopologyYet(): Unit = {
      socket.setSoTimeout(noResponseTimeoutMillis)
      try {
        intercept[SocketTimeoutException](reader.readLine())
      } finally {
        socket.setSoTimeout(socketTimeoutMillis)
      }
    }

    def isClosedByDriver: Boolean = {
      try {
        reader.readLine() == null
      } catch {
        case _: SocketException => true
      }
    }

    override def close(): Unit = socket.close()
  }

  /** Partition 0 signals the end of a barrier round over its own short-lived connection. */
  private def sendFinished(port: Int, stageAttempt: Int, barrierTaskCount: Option[Int] = Some(2)): Unit = {
    val socket = new Socket()
    try {
      socket.connect(new InetSocketAddress(host, port), socketTimeoutMillis)
      socket.setSoTimeout(socketTimeoutMillis)
      val writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream))
      val countSuffix = barrierTaskCount.map(count => s":$count").getOrElse("")
      writer.write(s"${LightGBMConstants.FinishedStatus}:$stageAttempt$countSuffix\n")
      writer.flush()
      socket.shutdownOutput()
      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream))
      try {
        assert(reader.readLine() == null, "The driver unexpectedly replied to a Finished marker")
      } catch {
        case _: SocketException => () // A reset also proves that the driver processed and rejected the marker socket.
      }
    } finally {
      socket.close()
    }
  }

  private def newBarrierManager(numTasks: Int): (NetworkManager, Int) = {
    val serverSocket = new ServerSocket(0)
    serverSocket.setSoTimeout(socketTimeoutMillis)
    val port = serverSocket.getLocalPort
    (NetworkManager(numTasks, serverSocket, host, port, timeout, useBarrierExecutionMode = true), port)
  }

  private def closeAll(manager: NetworkManager, tasks: Seq[FakeBarrierTask]): Unit = {
    manager.closeConnections()
    try {
      tasks.foreach { task =>
        try task.close()
        catch {
          case _: IOException => ()
        }
      }
    } finally {
      manager.waitForNetworkCommunicationsDone()
    }
  }

  private def partitionIds(executorTopology: String): Seq[Int] = {
    executorTopology.split(":").flatMap { executorEntry =>
      executorEntry.split("=")(1).split(",").map(_.toInt)
    }.toSeq
  }

  test("A restarted barrier stage gets a fresh topology round instead of a closed port") {
    val (manager, port) = newBarrierManager(numTasks = 2)
    var tasks = Seq.empty[FakeBarrierTask]
    try {
      // First stage attempt reports but never reaches the barrier, so the round never completes.
      val abandoned = (0 until 2).map { partitionId =>
        val task = new FakeBarrierTask(port, partitionId, stageAttempt = 0)
        tasks :+= task
        task
      }
      abandoned.foreach(_.report())

      // Spark restarts the whole stage. Every task reconnects to the same driver endpoint.
      val restarted = (0 until 2).map { partitionId =>
        val task = new FakeBarrierTask(port, partitionId, stageAttempt = 1)
        tasks :+= task
        task
      }
      restarted.foreach(_.report())
      sendFinished(port, stageAttempt = 1)

      val topologies = restarted.map(_.readTopology())
      assert(topologies.forall(_ != null), "The restarted stage attempt never received a topology")

      val expected = restarted.map(_.address).toSet
      topologies.foreach { topology =>
        assert(topology.split(",").toSet == expected,
          s"Topology '$topology' should contain only the restarted stage attempt, expected $expected")
      }

    } finally {
      closeAll(manager, tasks)
    }
  }

  test("A straggler from a superseded stage attempt is kept out of the topology") {
    val (manager, port) = newBarrierManager(numTasks = 2)
    var tasks = Seq.empty[FakeBarrierTask]
    try {
      val abandoned = new FakeBarrierTask(port, 0, stageAttempt = 0)
      tasks :+= abandoned
      abandoned.report()

      val restarted = (0 until 2).map { partitionId =>
        val task = new FakeBarrierTask(port, partitionId, stageAttempt = 1)
        tasks :+= task
        task
      }
      restarted.foreach(_.report())

      // A task from the abandoned attempt reports late. Its host:port is already dead.
      val straggler = new FakeBarrierTask(port, 1, stageAttempt = 0)
      tasks :+= straggler
      straggler.report()
      assert(straggler.isClosedByDriver, "The straggler should have been rejected by the driver")

      sendFinished(port, stageAttempt = 1)

      val expected = restarted.map(_.address).toSet
      restarted.foreach { task =>
        assert(task.readTopology().split(",").toSet == expected,
          s"The straggler at ${straggler.address} should not appear in the topology")
      }
    } finally {
      closeAll(manager, tasks)
    }
  }

  test("A Finished marker before task reports waits for every report in that stage attempt") {
    val (manager, port) = newBarrierManager(numTasks = 2)
    var tasks = Seq.empty[FakeBarrierTask]
    try {
      sendFinished(port, stageAttempt = 0)

      val current = (0 until 2).map { partitionId =>
        val task = new FakeBarrierTask(port, partitionId, stageAttempt = 0)
        tasks :+= task
        task
      }
      current.foreach(_.report())

      val expected = current.map(_.address).toSet
      current.foreach(task => assert(task.readTopology().split(",").toSet == expected))
    } finally {
      closeAll(manager, tasks)
    }
  }

  test("Task reports before the Finished marker do not complete a barrier round early") {
    val (manager, port) = newBarrierManager(numTasks = 2)
    var tasks = Seq.empty[FakeBarrierTask]
    try {
      val current = (0 until 2).map { partitionId =>
        val task = new FakeBarrierTask(port, partitionId, stageAttempt = 0)
        tasks :+= task
        task
      }
      current.foreach(_.report())
      current.head.assertNoTopologyYet()

      sendFinished(port, stageAttempt = 0)

      val expected = current.map(_.address).toSet
      current.foreach(task => assert(task.readTopology().split(",").toSet == expected))
    } finally {
      closeAll(manager, tasks)
    }
  }

  test("A barrier stage smaller than numTasks completes instead of hanging") {
    // Barrier mode never repartitions upwards, so setNumTasks above the input's partition
    // count leaves the stage running fewer tasks than numTasks. Waiting for numTasks reports
    // would block until Spark's barrier timeout (365 days by default).
    val (manager, port) = newBarrierManager(numTasks = 4)
    var tasks = Seq.empty[FakeBarrierTask]
    try {
      val current = (0 until 2).map { partitionId =>
        val task = new FakeBarrierTask(port, partitionId, stageAttempt = 0)
        tasks :+= task
        task
      }
      current.foreach(_.report())
      current.head.assertNoTopologyYet()

      sendFinished(port, stageAttempt = 0, barrierTaskCount = Some(2))

      val expected = current.map(_.address).toSet
      current.foreach(task => assert(task.readTopology().split(",").toSet == expected))
    } finally {
      closeAll(manager, tasks)
    }
  }

  test("A Finished marker without a task count still completes the round") {
    // Defensive: the marker is the only completion signal available when no count is reported.
    val (manager, port) = newBarrierManager(numTasks = 2)
    var tasks = Seq.empty[FakeBarrierTask]
    try {
      val current = (0 until 2).map { partitionId =>
        val task = new FakeBarrierTask(port, partitionId, stageAttempt = 0)
        tasks :+= task
        task
      }
      current.foreach(_.report())

      sendFinished(port, stageAttempt = 0, barrierTaskCount = None)

      val expected = current.map(_.address).toSet
      current.foreach(task => assert(task.readTopology().split(",").toSet == expected))
    } finally {
      closeAll(manager, tasks)
    }
  }

  test("A duplicate partition report before Finished replaces the old socket without changing the count") {
    val (manager, port) = newBarrierManager(numTasks = 2)
    var tasks = Seq.empty[FakeBarrierTask]
    try {
      val original = new FakeBarrierTask(
        port, partitionId = 0, stageAttempt = 0, loadOnly = true, executorId = "old-executor")
      val replacement = new FakeBarrierTask(
        port, partitionId = 0, stageAttempt = 0, listenPortOffset = 1000, executorId = "new-executor")
      val secondPartition = new FakeBarrierTask(port, partitionId = 1, stageAttempt = 0)
      tasks = Seq(original, replacement, secondPartition)

      original.report()
      replacement.report()
      assert(original.isClosedByDriver, "The superseded partition report socket was not closed")
      secondPartition.report()
      sendFinished(port, stageAttempt = 0)

      val expectedNetwork = Set(replacement.address, secondPartition.address)
      Seq(replacement, secondPartition).foreach { task =>
        val networkNodes = task.readTopology().split(",").toSeq
        assert(networkNodes.size == expectedNetwork.size)
        assert(networkNodes.toSet == expectedNetwork)
        val executorTopology = task.readExecutorTopology()
        assert(partitionIds(executorTopology).sorted == Seq(0, 1))
        assert(!executorTopology.contains("old-executor"))
        assert(executorTopology.contains("new-executor=0"))
      }
    } finally {
      closeAll(manager, tasks)
    }
  }

  test("A duplicate partition report after Finished still waits for every unique partition") {
    val (manager, port) = newBarrierManager(numTasks = 2)
    var tasks = Seq.empty[FakeBarrierTask]
    try {
      sendFinished(port, stageAttempt = 0)

      val original = new FakeBarrierTask(
        port, partitionId = 0, stageAttempt = 0, executorId = "old-executor")
      val replacement = new FakeBarrierTask(
        port, partitionId = 0, stageAttempt = 0, loadOnly = true,
        listenPortOffset = 1000, executorId = "new-executor")
      val secondPartition = new FakeBarrierTask(port, partitionId = 1, stageAttempt = 0)
      tasks = Seq(original, replacement, secondPartition)

      original.report()
      replacement.report()
      assert(original.isClosedByDriver, "The superseded partition report socket was not closed")
      secondPartition.report()

      Seq(replacement, secondPartition).foreach { task =>
        assert(task.readTopology() == secondPartition.address)
        val executorTopology = task.readExecutorTopology()
        assert(partitionIds(executorTopology).sorted == Seq(0, 1))
        assert(!executorTopology.contains("old-executor"))
        assert(executorTopology.contains("new-executor=0"))
      }
    } finally {
      closeAll(manager, tasks)
    }
  }

  test("A stale Finished marker cannot complete the current stage attempt") {
    val (manager, port) = newBarrierManager(numTasks = 2)
    var tasks = Seq.empty[FakeBarrierTask]
    try {
      val current = (0 until 2).map { partitionId =>
        val task = new FakeBarrierTask(port, partitionId, stageAttempt = 1)
        tasks :+= task
        task
      }
      current.foreach(_.report())

      sendFinished(port, stageAttempt = 0)
      current.head.assertNoTopologyYet()

      sendFinished(port, stageAttempt = 1)
      val expected = current.map(_.address).toSet
      current.foreach(task => assert(task.readTopology().split(",").toSet == expected))
    } finally {
      closeAll(manager, tasks)
    }
  }
}
