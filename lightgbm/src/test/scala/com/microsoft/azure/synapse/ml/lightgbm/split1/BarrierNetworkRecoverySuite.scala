// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split1

import com.microsoft.azure.synapse.ml.lightgbm.{LightGBMConstants, NetworkManager}
import org.scalatest.funsuite.AnyFunSuite

import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}
import java.net.{ServerSocket, Socket}

/** Covers recovery of the driver topology exchange when Spark restarts a barrier stage.
  *
  * A barrier stage is restarted in its entirety when any of its tasks fails, so it is the only case
  * where a LightGBM network can legitimately be re-formed. The driver has to serve a second topology
  * round for the new stage attempt, and must not mix it with the topology of the abandoned one.
  */
class BarrierNetworkRecoverySuite extends AnyFunSuite {

  private val timeout = 30.0
  private val host = "127.0.0.1"

  private class FakeBarrierTask(port: Int, partitionId: Int, stageAttempt: Int) {
    private val socket = new Socket(host, port)
    private val reader = new BufferedReader(new InputStreamReader(socket.getInputStream))
    private val writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream))

    val listenPort: Int = LightGBMConstants.DefaultLocalListenPort + stageAttempt * 100 + partitionId
    def address: String = s"$host:$listenPort"

    def report(): Unit = {
      writer.write(
        s"${LightGBMConstants.EnabledTask}:$host:$listenPort:$partitionId:executor$partitionId:$stageAttempt\n")
      writer.flush()
    }

    def readTopology(): String = reader.readLine()

    def isClosedByDriver: Boolean = reader.readLine() == null

    def close(): Unit = socket.close()
  }

  /** Partition 0 signals the end of a barrier round over its own short-lived connection. */
  private def sendFinished(port: Int, stageAttempt: Int): Unit = {
    val socket = new Socket(host, port)
    val writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream))
    writer.write(s"${LightGBMConstants.FinishedStatus}:$stageAttempt\n")
    writer.flush()
    socket.close()
  }

  private def newBarrierManager(numTasks: Int): (NetworkManager, Int) = {
    val serverSocket = new ServerSocket(0)
    val port = serverSocket.getLocalPort
    (NetworkManager(numTasks, serverSocket, host, port, timeout, useBarrierExecutionMode = true), port)
  }

  test("A restarted barrier stage gets a fresh topology round instead of a closed port") {
    val (manager, port) = newBarrierManager(numTasks = 2)

    // First stage attempt reports but never reaches the barrier, so the round never completes.
    val abandoned = (0 until 2).map(new FakeBarrierTask(port, _, stageAttempt = 0))
    abandoned.foreach(_.report())

    // Spark restarts the whole stage. Every task reconnects to the same driver endpoint.
    val restarted = (0 until 2).map(new FakeBarrierTask(port, _, stageAttempt = 1))
    restarted.foreach(_.report())
    sendFinished(port, stageAttempt = 1)

    val topologies = restarted.map(_.readTopology())
    assert(topologies.forall(_ != null), "The restarted stage attempt never received a topology")

    val expected = restarted.map(_.address).toSet
    topologies.foreach { topology =>
      assert(topology.split(",").toSet == expected,
        s"Topology '$topology' should contain only the restarted stage attempt, expected $expected")
    }

    abandoned.foreach(task =>
      assert(task.isClosedByDriver, "The abandoned stage attempt should have had its sockets released"))

    (abandoned ++ restarted).foreach(_.close())
    manager.waitForNetworkCommunicationsDone()
  }

  test("A straggler from a superseded stage attempt is kept out of the topology") {
    val (manager, port) = newBarrierManager(numTasks = 2)

    val abandoned = new FakeBarrierTask(port, 0, stageAttempt = 0)
    abandoned.report()

    val restarted = (0 until 2).map(new FakeBarrierTask(port, _, stageAttempt = 1))
    restarted.foreach(_.report())

    // A task from the abandoned attempt reports late. Its host:port is already dead.
    val straggler = new FakeBarrierTask(port, 1, stageAttempt = 0)
    straggler.report()
    assert(straggler.isClosedByDriver, "The straggler should have been rejected by the driver")

    sendFinished(port, stageAttempt = 1)

    val expected = restarted.map(_.address).toSet
    restarted.foreach { task =>
      assert(task.readTopology().split(",").toSet == expected,
        s"The straggler at ${straggler.address} should not appear in the topology")
    }

    (abandoned +: straggler +: restarted).foreach(_.close())
    manager.waitForNetworkCommunicationsDone()
  }

  test("A barrier round that completes normally still returns the full topology") {
    val (manager, port) = newBarrierManager(numTasks = 2)

    val tasks = (0 until 2).map(new FakeBarrierTask(port, _, stageAttempt = 0))
    tasks.foreach(_.report())
    sendFinished(port, stageAttempt = 0)

    val expected = tasks.map(_.address).toSet
    tasks.foreach(task => assert(task.readTopology().split(",").toSet == expected))

    tasks.foreach(_.close())
    manager.waitForNetworkCommunicationsDone()
  }
}
