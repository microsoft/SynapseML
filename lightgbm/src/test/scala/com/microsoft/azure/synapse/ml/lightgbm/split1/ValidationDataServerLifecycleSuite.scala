// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split1

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.lightgbm.{InstrumentationMeasures, LightGBMValidationDataSupport}
import com.microsoft.azure.synapse.ml.lightgbm.ValidationDataParams
import com.microsoft.azure.synapse.ml.lightgbm.ValidationDataServer
import com.microsoft.azure.synapse.ml.lightgbm.ValidationDataServerResourceFactory
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row

import java.io.{DataInputStream, DataOutputStream, File, FileOutputStream, IOException, OutputStream}
import java.net.{BindException, InetSocketAddress, ServerSocket, Socket, SocketException}
import java.nio.file.Files
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{AbstractExecutorService, Callable, CountDownLatch, ExecutorService}
import java.util.concurrent.{RejectedExecutionException}
import java.util.concurrent.{TimeUnit, TimeoutException}
import scala.annotation.tailrec

// scalastyle:off magic.number
class ValidationDataServerLifecycleSuite extends TestBase {
  private val host = "127.0.0.1"
  private val timeoutSeconds = 60.0

  test("ingest socket construction failure deletes the newly created spool") {
    val spool = scratchDirectory("ingest-bind-failure")
    val resources = new DelegatingResources {
      override def openServerSocket(host: String, timeoutSeconds: Double, backlog: Int): ServerSocket = {
        throw new BindException("synthetic ingest bind failure")
      }
    }

    try {
      intercept[BindException] {
        ValidationDataServer.create(spark.emptyDataFrame, host, 1, timeoutSeconds, spool, resources)
      }
      assert(!spool.exists())
    } finally {
      deleteIfPresent(spool)
    }
  }

  test("ingest listener backlog covers every validation partition") {
    val spool = scratchDirectory("ingest-backlog")
    val partitionCount = 73
    val observedBacklog = new AtomicInteger()
    val resources = new DelegatingResources {
      override def openServerSocket(host: String, timeoutSeconds: Double, backlog: Int): ServerSocket = {
        observedBacklog.set(backlog)
        throw new BindException("stop after recording backlog")
      }
    }

    try {
      intercept[BindException] {
        ValidationDataServer.create(
          spark.range(0L, partitionCount.toLong, 1L, partitionCount).toDF(),
          host,
          1,
          timeoutSeconds,
          spool,
          resources)
      }
      assert(observedBacklog.get() == partitionCount)
      assert(!spool.exists())
    } finally {
      deleteIfPresent(spool)
    }
  }

  test("serving socket construction failure after ingest deletes the spool and closes ingest resources") {
    val spool = scratchDirectory("serve-bind-failure")
    val resources = new DelegatingResources {
      private val calls = new AtomicInteger()
      @volatile var ingestSocket: Option[ServerSocket] = None

      override def openServerSocket(host: String, timeoutSeconds: Double, backlog: Int): ServerSocket = {
        if (calls.incrementAndGet() == 1) {
          val socket = super.openServerSocket(host, timeoutSeconds, backlog)
          ingestSocket = Option(socket)
          socket
        } else {
          throw new BindException("synthetic serving bind failure")
        }
      }
    }

    try {
      intercept[BindException] {
        ValidationDataServer.create(spark.range(1).toDF(), host, 1, timeoutSeconds, spool, resources)
      }
      assert(resources.ingestSocket.exists(_.isClosed))
      assert(!spool.exists())
    } finally {
      deleteIfPresent(spool)
    }
  }

  test("ingest waits for DataFrame partitions rather than the requested training task count") {
    val spool = scratchDirectory("ingest-partition-count")
    val server = ValidationDataServer.create(
      spark.range(0L, 2L, 1L, 2).toDF(), host, 4, timeoutSeconds, spool, ValidationDataServerResourceFactory.Default)
    try {
      assert(server.params.rowCount == 2L)
    } finally {
      try server.close()
      finally deleteIfPresent(spool)
    }
  }

  test("ingest executor construction failure closes its socket and deletes the spool") {
    val spool = scratchDirectory("ingest-executor-failure")
    val resources = new DelegatingResources {
      @volatile var ingestSocket: Option[ServerSocket] = None

      override def openServerSocket(host: String, timeoutSeconds: Double, backlog: Int): ServerSocket = {
        val socket = super.openServerSocket(host, timeoutSeconds, backlog)
        ingestSocket = Option(socket)
        socket
      }

      override def createExecutor(threadCount: Int, threadNamePrefix: String): ExecutorService = {
        throw new RejectedExecutionException("synthetic ingest executor failure")
      }
    }

    try {
      intercept[RejectedExecutionException] {
        ValidationDataServer.create(spark.emptyDataFrame, host, 1, timeoutSeconds, spool, resources)
      }
      assert(resources.ingestSocket.exists(_.isClosed))
      assert(!spool.exists())
    } finally {
      deleteIfPresent(spool)
    }
  }

  test("serving executor construction failure closes its socket and deletes the spool") {
    val spool = scratchDirectory("serve-executor-failure")
    val resources = new DelegatingResources {
      private val executorCalls = new AtomicInteger()
      @volatile var servingSocket: Option[ServerSocket] = None

      override def openServerSocket(host: String, timeoutSeconds: Double, backlog: Int): ServerSocket = {
        val socket = super.openServerSocket(host, timeoutSeconds, backlog)
        servingSocket = Option(socket)
        socket
      }

      override def createExecutor(threadCount: Int, threadNamePrefix: String): ExecutorService = {
        if (executorCalls.incrementAndGet() == 1) {
          super.createExecutor(threadCount, threadNamePrefix)
        } else {
          throw new RejectedExecutionException("synthetic serving executor failure")
        }
      }
    }

    try {
      intercept[RejectedExecutionException] {
        ValidationDataServer.create(spark.range(1).toDF(), host, 1, timeoutSeconds, spool, resources)
      }
      assert(resources.servingSocket.exists(_.isClosed))
      assert(!spool.exists())
    } finally {
      deleteIfPresent(spool)
    }
  }

  test("serving start failure closes its socket and executor and deletes the spool") {
    val (spool, partitionFiles) = createSpool("serve-start-failure")
    val executor = new RejectingExecutor
    val resources = new DelegatingResources {
      @volatile var servingSocket: Option[ServerSocket] = None

      override def openServerSocket(host: String, timeoutSeconds: Double, backlog: Int): ServerSocket = {
        val socket = super.openServerSocket(host, timeoutSeconds, backlog)
        servingSocket = Option(socket)
        socket
      }

      override def createExecutor(threadCount: Int, threadNamePrefix: String): ExecutorService = executor
    }

    try {
      intercept[RejectedExecutionException] {
        ValidationDataServer.createFromSpool(
          spool, partitionFiles, 0L, host, timeoutSeconds, 1, resources)
      }
      assert(resources.servingSocket.exists(_.isClosed))
      assert(executor.isTerminated)
      assert(!spool.exists())
    } finally {
      deleteIfPresent(spool)
    }
  }

  test("close aborts a stalled serving client before deleting its spool") {
    val (spool, partitionFiles) = createSpool("stalled-client")
    val outputStarted = new CountDownLatch(1)
    val resources = new DelegatingResources {
      override def clientOutput(socket: Socket): OutputStream = new OutputStream {
        override def write(value: Int): Unit = blockUntilClosed()
        override def write(bytes: Array[Byte], offset: Int, length: Int): Unit = blockUntilClosed()

        private def blockUntilClosed(): Unit = {
          outputStarted.countDown()
          waitUntil(socket.isClosed)
          throw new SocketException("synthetic cancelled client")
        }
      }
    }
    val server = ValidationDataServer.createFromSpool(
      spool, partitionFiles, 0L, host, timeoutSeconds, 1, resources)
    val client = authenticatedClient(server.params)
    try {
      assert(outputStarted.await(5, TimeUnit.SECONDS))

      server.close()

      assert(!spool.exists())
    } finally {
      try client.close()
      finally deleteIfPresent(spool)
    }
  }

  test("a cancelled speculative client does not fail a completed validation transfer") {
    val (spool, partitionFiles) = createSpool("cancelled-client")
    val cancelledOutput = new CountDownLatch(1)
    val resources = new DelegatingResources {
      private val outputCalls = new AtomicInteger()

      override def clientOutput(socket: Socket): OutputStream = {
        if (outputCalls.incrementAndGet() == 1) {
          new OutputStream {
            override def write(value: Int): Unit = fail()
            override def write(bytes: Array[Byte], offset: Int, length: Int): Unit = fail()

            private def fail(): Unit = {
              cancelledOutput.countDown()
              throw new SocketException("synthetic speculative cancellation")
            }
          }
        } else {
          super.clientOutput(socket)
        }
      }
    }
    val server = ValidationDataServer.createFromSpool(
      spool, partitionFiles, 0L, host, timeoutSeconds, 2, resources)
    try {
      val cancelledClient = authenticatedClient(server.params)
      try {
        assert(cancelledOutput.await(5, TimeUnit.SECONDS))
      } finally {
        cancelledClient.close()
      }

      val completedClient = authenticatedClient(server.params)
      try {
        readToEnd(completedClient)
      } finally {
        completedClient.close()
      }

      server.close()
    } finally {
      try server.close()
      finally deleteIfPresent(spool)
    }
    assert(!spool.exists())
  }

  test("idle accept timeouts while workers train do not poison a later transfer") {
    val (spool, partitionFiles) = createSpool("idle-accept-timeout")
    val server = ValidationDataServer.createFromSpool(
      spool, partitionFiles, 0L, host, 0.01, 1, ValidationDataServerResourceFactory.Default)
    try {
      Thread.sleep(1200)
      val client = authenticatedClient(server.params)
      try {
        readToEnd(client)
      } finally {
        client.close()
      }
      server.close()
    } finally {
      try server.close()
      finally deleteIfPresent(spool)
    }
    assert(!spool.exists())
  }

  test("socket timeout converts seconds independently of the accept poll interval") {
    val socket = ValidationDataServer.openServerSocket(host, 2.5, 1)
    try {
      assert(socket.getSoTimeout == 2500)
    } finally {
      socket.close()
    }
  }

  test("socket timeout preserves sub-second configurations") {
    val socket = ValidationDataServer.openServerSocket(host, 0.25, 1)
    try {
      assert(socket.getSoTimeout == 250)
    } finally {
      socket.close()
    }
  }

  test("ingest deadline preserves fractional timeout milliseconds") {
    val start = 100L
    assert(ValidationDataServer.ingestDeadlineNanos(start, 2500) - start == TimeUnit.MILLISECONDS.toNanos(2500))
  }

  test("ingest rejects a negative row length other than the end marker") {
    val failure = malformedIngestFailure("length", partitionCount = 1) { output =>
      output.writeInt(0)
      output.writeInt(-2)
    }
    assert(failure.isInstanceOf[IOException])
    assert(failure.getMessage.contains("Invalid validation partition row length -2"))
  }

  test("ingest rejects partition ids outside the expected range") {
    val failure = malformedIngestFailure("partition", partitionCount = 1) { output =>
      output.writeInt(1)
    }
    assert(failure.isInstanceOf[IOException])
    assert(failure.getMessage.contains("Invalid validation partition id 1"))
  }

  test("executor read rejects a negative row length other than the end marker") {
    val (spool, partitionFiles) = createMalformedSpool("malformed-executor-length", -2)
    val server = ValidationDataServer.createFromSpool(
      spool, partitionFiles, 0L, host, timeoutSeconds, 1, ValidationDataServerResourceFactory.Default)
    val params = spark.sparkContext.broadcast(server.params.toRows)
    try {
      val failure = intercept[IOException](ValidationDataServer.read(params))
      assert(failure.getMessage.contains("Invalid validation data row length -2"))
    } finally {
      try params.destroy()
      finally {
        try server.close()
        finally deleteIfPresent(spool)
      }
    }
  }

  test("executor read setup closes its socket when authentication output fails") {
    val expected = new IOException("synthetic authentication output failure")
    val socket = new FailingOutputSocket(expected)

    val failure = intercept[IOException] {
      ValidationDataServer.openValidationInput(socket, "token", 5000)
    }

    assert(failure eq expected)
    assert(socket.isClosed)
  }

  test("validation row counts above the native Int limit fail clearly") {
    val params = ValidationDataParams(
      host,
      port = 1,
      rowCount = Int.MaxValue.toLong + 1L,
      timeoutMillis = 1000,
      token = "test")
    val broadcast = spark.sparkContext.broadcast(params.toRows)
    try {
      val failure = intercept[IllegalArgumentException](ValidationDataServer.rowCount(broadcast))
      assert(failure.getMessage.contains("outside the supported range"))
      assert(failure.getMessage.contains(Int.MaxValue.toString))
    } finally {
      broadcast.destroy()
    }
  }

  test("legacy validation row broadcasts remain readable") {
    val expected = Array(Row(1), Row(2))
    val broadcast = spark.sparkContext.broadcast(expected)
    try {
      assert(ValidationDataServer.rowCount(broadcast) == expected.length)
      val rows = ValidationDataServer.read(broadcast)
      val actual = ValidationDataServer.withRows(rows)(rows.toArray)
      assert(actual.sameElements(expected))
    } finally {
      broadcast.destroy()
    }
  }

  test("unknown validation descriptor versions fail clearly") {
    val broadcast = spark.sparkContext.broadcast(Array(Row("SynapseML.ValidationDataServer.v2")))
    try {
      val failure = intercept[IllegalArgumentException](ValidationDataServer.rowCount(broadcast))
      assert(failure.getMessage.contains("Unsupported validation data descriptor"))
    } finally {
      broadcast.destroy()
    }
  }

  test("training failure remains primary when broadcast and server cleanup fail") {
    val trainingFailure = new IllegalStateException("synthetic training failure")
    val broadcastFailure = new IOException("synthetic broadcast cleanup failure")
    val serverFailure = new IOException("synthetic server cleanup failure")

    val failure = intercept[IllegalStateException] {
      LightGBMValidationDataSupport.withResources[Unit, Unit](
        (),
        (_: Unit) => throw broadcastFailure,
        throw serverFailure) { _ =>
        throw trainingFailure
      }
    }

    assert(failure eq trainingFailure)
    assert(failure.getSuppressed.sameElements(Array(broadcastFailure, serverFailure)))
  }

  test("validation collection timing stops when server creation fails") {
    val measures = new InstrumentationMeasures()
    val expected = new IOException("synthetic server creation failure")

    val failure = intercept[IOException] {
      LightGBMValidationDataSupport.measureCollection(enabled = true, measures) {
        Thread.sleep(20)
        throw expected
      }
    }

    assert(failure eq expected)
    assert(measures.validationDataCollectionTime() > 0L)
  }

  test("validation row processing failure remains primary when iterator close fails") {
    val processingFailure = new IOException("synthetic row processing failure")
    val closeFailure = new IOException("synthetic iterator close failure")
    val rows = new com.microsoft.azure.synapse.ml.lightgbm.ValidationRowIterator {
      override def hasNext: Boolean = false
      override def next(): org.apache.spark.sql.Row = throw new NoSuchElementException
      override def close(): Unit = throw closeFailure
    }

    val failure = intercept[IOException] {
      ValidationDataServer.withRows(rows) {
        throw processingFailure
      }
    }

    assert(failure eq processingFailure)
    assert(failure.getSuppressed.sameElements(Array(closeFailure)))
  }

  test("close preserves a serving failure when stream cleanup also fails") {
    val (spool, partitionFiles) = createSpool("serving-failure")
    val outputFailed = new CountDownLatch(1)
    val writeFailure = new IOException("synthetic serving failure")
    val closeFailure = new IOException("synthetic serving cleanup failure")
    val resources = new DelegatingResources {
      override def clientOutput(socket: Socket): OutputStream = new OutputStream {
        override def write(value: Int): Unit = fail()
        override def write(bytes: Array[Byte], offset: Int, length: Int): Unit = fail()

        private def fail(): Unit = {
          outputFailed.countDown()
          throw writeFailure
        }

        override def close(): Unit = throw closeFailure
      }
    }
    val server = ValidationDataServer.createFromSpool(
      spool, partitionFiles, 0L, host, timeoutSeconds, 1, resources)
    val client = authenticatedClient(server.params)
    try {
      assert(outputFailed.await(5, TimeUnit.SECONDS))
      val failure = intercept[IOException](server.close())
      assert(failure eq writeFailure)
      assert(failure.getSuppressed.contains(closeFailure))
    } finally {
      try client.close()
      finally {
        try server.close()
        finally deleteIfPresent(spool)
      }
    }
    assert(!spool.exists())
  }

  test("close fails explicitly and retains the spool when executor termination is not confirmed") {
    val (spool, partitionFiles) = createSpool("termination-failure")
    val resources = new DelegatingResources {
      override def createExecutor(threadCount: Int, threadNamePrefix: String): ExecutorService = {
        new NonTerminatingExecutor(super.createExecutor(threadCount, threadNamePrefix))
      }
    }
    val server = ValidationDataServer.createFromSpool(
      spool, partitionFiles, 0L, host, timeoutSeconds, 1, resources)

    try {
      val failure = intercept[IOException](server.close())
      assert(failure.getMessage.contains("did not terminate"))
      assert(spool.exists())
    } finally {
      deleteIfPresent(spool)
    }
  }

  private class DelegatingResources extends ValidationDataServerResourceFactory {
    private val delegate = ValidationDataServerResourceFactory.Default

    override def openServerSocket(host: String, timeoutSeconds: Double, backlog: Int): ServerSocket = {
      delegate.openServerSocket(host, timeoutSeconds, backlog)
    }

    override def createExecutor(threadCount: Int, threadNamePrefix: String): ExecutorService = {
      delegate.createExecutor(threadCount, threadNamePrefix)
    }

    override def clientOutput(socket: Socket): OutputStream = delegate.clientOutput(socket)
  }

  private class NonTerminatingExecutor(delegate: ExecutorService) extends AbstractExecutorService {
    override def shutdown(): Unit = delegate.shutdown()
    override def shutdownNow(): java.util.List[Runnable] = delegate.shutdownNow()
    override def isShutdown: Boolean = delegate.isShutdown
    override def isTerminated: Boolean = delegate.isTerminated
    override def awaitTermination(timeout: Long, unit: TimeUnit): Boolean = {
      delegate.awaitTermination(timeout, unit)
      false
    }
    override def execute(command: Runnable): Unit = delegate.execute(command)
  }

  private class RejectingExecutor extends AbstractExecutorService {
    @volatile private var stopped = false

    override def shutdown(): Unit = stopped = true
    override def shutdownNow(): java.util.List[Runnable] = {
      stopped = true
      java.util.Collections.emptyList[Runnable]()
    }
    override def isShutdown: Boolean = stopped
    override def isTerminated: Boolean = stopped
    override def awaitTermination(timeout: Long, unit: TimeUnit): Boolean = stopped
    override def execute(command: Runnable): Unit = {
      throw new RejectedExecutionException("synthetic serving start failure")
    }
  }

  private class FailingOutputSocket(expected: IOException) extends Socket {
    @volatile private var closed = false

    override def setKeepAlive(on: Boolean): Unit = ()
    override def setTcpNoDelay(on: Boolean): Unit = ()
    override def setSoTimeout(timeout: Int): Unit = ()
    override def getOutputStream: OutputStream = throw expected
    override def close(): Unit = closed = true
    override def isClosed: Boolean = closed
  }

  private def malformedIngestFailure(name: String,
                                     partitionCount: Int)
                                    (writeFrame: DataOutputStream => Unit): Throwable = {
    val spool = scratchDirectory(s"malformed-ingest-$name")
    assert(spool.mkdir())
    val listener = ValidationDataServer.openServerSocket(host, timeoutSeconds, 1)
    val executor = ValidationDataServerResourceFactory.Default.createExecutor(1, s"malformed-$name-test")
    val token = UUID.randomUUID().toString
    val received = executor.submit(new Callable[Throwable] {
      override def call(): Throwable = {
        try {
          ValidationDataServer.receivePartition(listener.accept(), spool, token, 5000, partitionCount)
          new AssertionError("Malformed validation partition was accepted")
        } catch {
          case failure: Throwable => failure
        }
      }
    })
    val client = new Socket()
    try {
      client.connect(new InetSocketAddress(host, listener.getLocalPort), 5000)
      val output = new DataOutputStream(client.getOutputStream)
      output.writeUTF(token)
      writeFrame(output)
      output.flush()
      val failure = received.get(5, TimeUnit.SECONDS)
      assert(Option(spool.listFiles()).getOrElse(Array.empty).isEmpty)
      failure
    } finally {
      try client.close()
      finally {
        listener.close()
        executor.shutdownNow()
        executor.awaitTermination(5, TimeUnit.SECONDS)
        deleteIfPresent(spool)
      }
    }
  }

  private def authenticatedClient(params: ValidationDataParams): Socket = {
    val socket = new Socket()
    socket.connect(new InetSocketAddress(params.host, params.port), params.timeoutMillis)
    socket.setSoTimeout(params.timeoutMillis)
    val auth = new DataOutputStream(socket.getOutputStream)
    auth.writeUTF(params.token)
    auth.flush()
    socket
  }

  private def readToEnd(socket: Socket): Unit = {
    val input = new DataInputStream(socket.getInputStream)
    val buffer = new Array[Byte](1024)
    var count = input.read(buffer)
    while (count >= 0) { // scalastyle:ignore while
      count = input.read(buffer)
    }
  }

  private def createSpool(name: String): (File, Array[File]) = {
    val spool = scratchDirectory(name)
    assert(spool.mkdir())
    val partitionFile = new File(spool, "part-0")
    Files.write(partitionFile.toPath, Array[Byte](1, 2, 3, 4))
    (spool, Array(partitionFile))
  }

  private def createMalformedSpool(name: String, length: Int): (File, Array[File]) = {
    val spool = scratchDirectory(name)
    assert(spool.mkdir())
    val partitionFile = new File(spool, "part-0")
    val output = new DataOutputStream(new FileOutputStream(partitionFile))
    try output.writeInt(length)
    finally output.close()
    (spool, Array(partitionFile))
  }

  private def scratchDirectory(name: String): File = {
    new File(System.getProperty("user.dir"), s".synapseml-validation-$name-${UUID.randomUUID()}")
  }

  private def deleteIfPresent(directory: File): Unit = {
    if (directory.exists()) FileUtils.deleteDirectory(directory)
  }

  private def waitUntil(condition: => Boolean): Unit = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)

    @tailrec
    def loop(): Unit = {
      if (!condition) {
        if (System.nanoTime() >= deadline) throw new TimeoutException("Condition was not met before timeout")
        Thread.sleep(10)
        loop()
      }
    }

    loop()
  }
}
