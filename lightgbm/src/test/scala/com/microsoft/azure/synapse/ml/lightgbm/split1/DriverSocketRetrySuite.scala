// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split1

import com.microsoft.azure.synapse.ml.lightgbm.{LightGBMConstants, NetworkManager}
import org.scalatest.funsuite.AnyFunSuite

import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}
import java.net.{ConnectException, ServerSocket, Socket}

/** Covers the driver topology socket lifecycle behind repeated
  * "java.net.ConnectException: Connection refused" failures in distributed LightGBM training.
  */
class DriverSocketRetrySuite extends AnyFunSuite {

  private val timeout = 30.0

  private class FakeTask(host: String, port: Int, partitionId: Int, loadOnly: Boolean = false) {
    private val socket = new Socket(host, port)
    private val reader = new BufferedReader(new InputStreamReader(socket.getInputStream))
    private val writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream))

    def report(): Unit = {
      val status = if (loadOnly) LightGBMConstants.IgnoreStatus else LightGBMConstants.EnabledTask
      val listenPort = LightGBMConstants.DefaultLocalListenPort + partitionId
      writer.write(s"$status:127.0.0.1:$listenPort:$partitionId:$partitionId\n")
      writer.flush()
    }

    def readTopology(): (String, String) = (reader.readLine(), reader.readLine())

    /** The driver closing its end is observed here as end-of-stream. */
    def isClosedByDriver: Boolean = reader.readLine() == null

    def close(): Unit = socket.close()
  }

  private def newManager(numTasks: Int): (NetworkManager, String, Int) = {
    val serverSocket = new ServerSocket(0)
    val host = "127.0.0.1"
    val port = serverSocket.getLocalPort
    (NetworkManager(numTasks, serverSocket, host, port, timeout, useBarrierExecutionMode = false), host, port)
  }

  private def runTopologyRound(numTasks: Int,
                               numLoadOnlyTasks: Int = 0): (NetworkManager, String, Int, Seq[FakeTask]) = {
    val (manager, host, port) = newManager(numTasks + numLoadOnlyTasks)
    val trainingTasks = (0 until numTasks).map(new FakeTask(host, port, _))
    val loadOnlyTasks = (0 until numLoadOnlyTasks).map(i => new FakeTask(host, port, numTasks + i, loadOnly = true))
    val tasks = trainingTasks ++ loadOnlyTasks

    tasks.foreach(_.report())
    tasks.foreach { task =>
      val (machineList, partitionList) = task.readTopology()
      assert(machineList != null && machineList.nonEmpty)
      assert(partitionList != null && partitionList.nonEmpty)
    }
    manager.waitForNetworkCommunicationsDone()
    (manager, host, port, tasks)
  }

  test("A retried task is refused by the driver once the topology round has completed") {
    val (_, host, port, tasks) = runTopologyRound(numTasks = 2)
    tasks.foreach(_.close())

    // A Spark task retry re-enters getGlobalNetworkInfo and reconnects to the same driver endpoint.
    val thrown = intercept[ConnectException] {
      new Socket(host, port).close()
    }

    assert(thrown.getMessage.toLowerCase.contains("refused"),
      s"Expected a connection-refused failure but got: ${thrown.getMessage}")
  }

  test("Helper task sockets are released along with training task sockets") {
    val (_, _, _, tasks) = runTopologyRound(numTasks = 2, numLoadOnlyTasks = 2)

    tasks.zipWithIndex.foreach { case (task, index) =>
      assert(task.isClosedByDriver, s"Driver leaked the socket for task $index")
      task.close()
    }
  }

  test("closeConnections is idempotent so both the network thread and the training job can call it") {
    val (manager, host, port, tasks) = runTopologyRound(numTasks = 2)
    tasks.foreach(_.close())

    manager.closeConnections()
    manager.closeConnections()

    intercept[ConnectException] {
      new Socket(host, port).close()
    }
  }

  test("The driver server socket is released when a training job fails before the round completes") {
    // Only one of the two expected tasks reports, so the network thread stays blocked in accept().
    val (manager, host, port) = newManager(numTasks = 2)
    val task = new FakeTask(host, port, 0)
    task.report()

    // This is what executeTraining now does in its finally block when partition tasks fail.
    manager.closeConnections()
    task.close()

    intercept[ConnectException] {
      new Socket(host, port).close()
    }
  }
}
