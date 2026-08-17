// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split1

import com.microsoft.azure.synapse.ml.lightgbm.{LightGBMConstants, NetworkManager, TaskMessageInfo,
  WorkerEndpoint, WorkerMessage}
import org.scalatest.funsuite.AnyFunSuite

import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}
import java.net.{InetSocketAddress, ServerSocket, Socket}
import scala.collection.mutable.ListBuffer
import scala.util.Try

/** Covers the wire form of every endpoint LightGBM components exchange. */
class WorkerWireFormatSuite extends AnyFunSuite {

  private val socketTimeoutMillis = 30000
  private val driverHost = "127.0.0.1"
  private val timeout = 30.0

  private class ReportingTask(host: String, port: Int, taskHost: String, listenPort: Int, partitionId: Int)
    extends AutoCloseable {
    private val socket = new Socket()
    socket.connect(new InetSocketAddress(host, port), socketTimeoutMillis)
    socket.setSoTimeout(socketTimeoutMillis)
    private val reader = new BufferedReader(new InputStreamReader(socket.getInputStream))
    private val writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream))

    def report(): Unit = {
      val message = WorkerMessage.format(
        TaskMessageInfo(LightGBMConstants.EnabledTask, taskHost, listenPort, partitionId, partitionId.toString), 0)
      writer.write(s"$message\n")
      writer.flush()
    }

    def readMachineList(): String = reader.readLine()

    override def close(): Unit = socket.close()
  }

  test("Only an IPv6 host is bracketed on the wire") {
    assert(WorkerEndpoint.wireString("10.0.0.4", 12400) == "10.0.0.4:12400")
    assert(WorkerEndpoint.wireString("worker-1", 12400) == "worker-1:12400")
    assert(WorkerEndpoint.wireString("2001:db8::1", 12400) == "[2001:db8::1]:12400")
    // A zone identifier belongs inside the brackets, with the address it scopes.
    assert(WorkerEndpoint.wireString("fe80::1%eth0", 12400) == "[fe80::1%eth0]:12400")
    // An already bracketed host is not bracketed twice.
    assert(WorkerEndpoint.wireString("[2001:db8::1]", 12400) == "[2001:db8::1]:12400")
  }

  test("A wire endpoint round trips back to the host it was built from") {
    Seq("10.0.0.4", "worker-1", "2001:db8::1", "fe80::1%eth0", "::1").foreach { host =>
      val parsed = WorkerEndpoint.parse(WorkerEndpoint.wireString(host, 12400))
      assert(parsed.host == host)
      assert(parsed.port == 12400)
      assert(parsed.wireString == WorkerEndpoint.wireString(host, 12400))
    }
  }

  test("A host carrying a control character or a delimiter never reaches the wire") {
    // The topology exchange is a line protocol over a comma delimited machine list, so any of these
    // would either split one endpoint into two or forge an extra protocol line.
    Seq("10.0.0.4\n", "10.0.0.4\r", "10.0.0.4\t", "10.0.0.4\u0000", "10.0.0.4 ", "10.0.0.4,10.0.0.5",
      "10.0.0.4]", "[10.0.0.4").foreach { host =>
      val failure = intercept[IllegalArgumentException](WorkerEndpoint.wireString(host, 12400))
      assert(failure.getMessage.contains("Invalid LightGBM worker endpoint"), s"unexpected error for '$host'")
    }
  }

  test("A rejected endpoint reports control characters as escapes rather than raw bytes") {
    val failure = intercept[IllegalArgumentException](WorkerEndpoint.wireString("10.0.0.4\r\nignore:1", 12400))
    assert(failure.getMessage.contains("\\r\\n"))
    assert(!failure.getMessage.contains("\r"))
    assert(!failure.getMessage.contains("\n"))
    val nullByte = intercept[IllegalArgumentException](WorkerEndpoint.wireString("10.0.0.4\u0000", 12400))
    assert(nullByte.getMessage.contains("\\u0000"))
  }

  test("A port outside the valid range never reaches the wire") {
    Seq(0, -1, LightGBMConstants.MaxPort + 1).foreach { port =>
      val failure = intercept[IllegalArgumentException](WorkerEndpoint.wireString("10.0.0.4", port))
      assert(failure.getMessage.contains("Invalid LightGBM worker endpoint"))
    }
  }

  test("A task message round trips for every host form, including a scoped IPv6 literal") {
    Seq("10.0.0.4", "2001:db8::1", "fe80::1%eth0").foreach { host =>
      val status = TaskMessageInfo(LightGBMConstants.EnabledTask, host, 12400, 3, "executor-2")
      val message = WorkerMessage.format(status, 7)
      val parsed = WorkerMessage.parse(message)
      assert(parsed.taskHost == host)
      assert(parsed.localListenPort == 12400)
      assert(parsed.partitionId == 3)
      assert(parsed.executorId == "executor-2")
      assert(parsed.stageAttemptNumber == 7)
    }
  }

  test("A task message with a forged extra line is rejected before it is sent") {
    val status = TaskMessageInfo(LightGBMConstants.EnabledTask, "10.0.0.4\nenabledTask:10.0.0.9", 12400, 0, "e")
    assert(intercept[IllegalArgumentException](WorkerMessage.format(status, 0))
      .getMessage.contains("Invalid LightGBM worker endpoint"))
  }

  test("The driver publishes a machine list the LightGBM network layer can split on commas") {
    val serverSocket = new ServerSocket(0)
    serverSocket.setSoTimeout(socketTimeoutMillis)
    val manager = NetworkManager(2, serverSocket, driverHost, serverSocket.getLocalPort, timeout,
      useBarrierExecutionMode = false)
    val tasks = ListBuffer.empty[ReportingTask]
    try {
      tasks += new ReportingTask(driverHost, serverSocket.getLocalPort, "2001:db8::1", 12400, 0)
      tasks += new ReportingTask(driverHost, serverSocket.getLocalPort, "2001:db8::2", 12401, 1)
      tasks.foreach(_.report())

      val machineList = tasks.head.readMachineList()
      assert(machineList == "[2001:db8::1]:12400,[2001:db8::2]:12401",
        "An unbracketed IPv6 machine list cannot be split into host and port by any peer")
      // Every consumer of the machine list has to agree on where the first endpoint ends.
      assert(NetworkManager.getMainWorkerPort(machineList, org.slf4j.LoggerFactory.getLogger(getClass)) == 12400)
      manager.waitForNetworkCommunicationsDone()
    } finally {
      manager.closeConnections()
      tasks.foreach(task => Try(task.close()))
    }
  }
}
