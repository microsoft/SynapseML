// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split1

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.lightgbm.{LightGBMValidationDataSupport, ValidationDataParams}
import com.microsoft.azure.synapse.ml.lightgbm.ValidationDataServer
import com.microsoft.azure.synapse.ml.lightgbm.ValidationDataServerResourceFactory
import org.apache.commons.io.FileUtils

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
      waitUntil(server.activeClientCount == 1)

      server.close()

      assert(server.executorIsTerminated)
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

      server.await()
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
      server.await()
    } finally {
      try server.close()
      finally deleteIfPresent(spool)
    }
    assert(!spool.exists())
  }

  test("ingest rejects a negative row length other than the end marker") {
    val spool = scratchDirectory("malformed-ingest-length")
    assert(spool.mkdir())
    val listener = ValidationDataServer.openServerSocket(host, timeoutSeconds, 1)
    val executor = ValidationDataServerResourceFactory.Default.createExecutor(1, "malformed-ingest-test")
    val token = UUID.randomUUID().toString
    val received = executor.submit(new Callable[Throwable] {
      override def call(): Throwable = {
        try {
          ValidationDataServer.receivePartition(listener.accept(), spool, token, 5000)
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
      output.writeInt(0)
      output.writeInt(-2)
      output.flush()

      val failure = received.get(5, TimeUnit.SECONDS)
      assert(failure.isInstanceOf[IOException])
      assert(failure.getMessage.contains("Invalid validation partition row length -2"))
      assert(Option(spool.listFiles()).getOrElse(Array.empty).isEmpty)
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

  test("await failure remains primary when broadcast and server cleanup fail") {
    val awaitFailure = new IOException("synthetic await failure")
    val broadcastFailure = new IOException("synthetic broadcast cleanup failure")
    val serverFailure = new IOException("synthetic server cleanup failure")

    val failure = intercept[IOException] {
      LightGBMValidationDataSupport.withResources[Unit, Unit](
        (),
        (_: Unit) => throw broadcastFailure,
        throw serverFailure) { _ =>
        throw awaitFailure
      }
    }

    assert(failure eq awaitFailure)
    assert(failure.getSuppressed.sameElements(Array(broadcastFailure, serverFailure)))
  }

  test("an internal serving failure is retained by await") {
    val (spool, partitionFiles) = createSpool("serving-failure")
    val outputFailed = new CountDownLatch(1)
    val resources = new DelegatingResources {
      override def clientOutput(socket: Socket): OutputStream = new OutputStream {
        override def write(value: Int): Unit = fail()
        override def write(bytes: Array[Byte], offset: Int, length: Int): Unit = fail()

        private def fail(): Unit = {
          outputFailed.countDown()
          throw new IOException("synthetic serving failure")
        }
      }
    }
    val server = ValidationDataServer.createFromSpool(
      spool, partitionFiles, 0L, host, timeoutSeconds, 1, resources)
    val client = authenticatedClient(server.params)
    try {
      assert(outputFailed.await(5, TimeUnit.SECONDS))
      val failure = intercept[IOException](server.await())
      assert(failure.getMessage == "synthetic serving failure")
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
