// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm

import com.microsoft.azure.synapse.ml.core.env.StreamUtilities.using
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkEnv
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row}

import java.io.{BufferedInputStream, BufferedOutputStream, DataInputStream, DataOutputStream, EOFException}
import java.io.{File, FileInputStream, FileOutputStream, IOException, InputStream, OutputStream}
import java.net.{InetAddress, InetSocketAddress, ServerSocket, Socket}
import java.nio.ByteBuffer
import java.nio.file.Files
import java.util.UUID
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, ExecutorService, Future}
import java.util.concurrent.{Semaphore, ThreadFactory, TimeUnit}
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

private[lightgbm] case class ValidationDataParams(host: String,
                                                  port: Int,
                                                  rowCount: Long,
                                                  timeoutMillis: Int,
                                                  token: String) {
  def toRows: Array[Row] = {
    Array(Row(ValidationDataParams.Marker, host, port, rowCount, timeoutMillis, token))
  }
}

private[lightgbm] object ValidationDataParams {
  val Marker: String = "SynapseML.ValidationDataServer.v1"
  private val MetadataFieldCount = 6
  private val HostIndex = 1
  private val PortIndex = 2
  private val RowCountIndex = 3
  private val TimeoutIndex = 4
  private val TokenIndex = 5

  def fromBroadcast(data: Broadcast[Array[Row]]): Option[ValidationDataParams] = {
    data.value.headOption.filter(row => row.length == MetadataFieldCount && row.get(0) == Marker).map { row =>
      ValidationDataParams(
        row.getString(HostIndex),
        row.getInt(PortIndex),
        row.getLong(RowCountIndex),
        row.getInt(TimeoutIndex),
        row.getString(TokenIndex))
    }
  }
}

private[lightgbm] trait ValidationRowIterator extends Iterator[Row] with AutoCloseable

private[lightgbm] trait ValidationDataServerResourceFactory {
  def openServerSocket(host: String, timeoutSeconds: Double, backlog: Int): ServerSocket
  def createExecutor(threadCount: Int, threadNamePrefix: String): ExecutorService
  def clientOutput(socket: Socket): OutputStream
}

private[lightgbm] object ValidationDataServerResourceFactory {
  object Default extends ValidationDataServerResourceFactory {
    override def openServerSocket(host: String, timeoutSeconds: Double, backlog: Int): ServerSocket = {
      ValidationDataServer.openServerSocket(host, timeoutSeconds, backlog)
    }

    override def createExecutor(threadCount: Int, threadNamePrefix: String): ExecutorService = {
      java.util.concurrent.Executors.newFixedThreadPool(
        threadCount,
        ValidationDataServer.daemonThreadFactory(threadNamePrefix))
    }

    override def clientOutput(socket: Socket): OutputStream = socket.getOutputStream
  }
}

/**
  * Spools exact validation rows through bounded socket buffers, then streams the spool to each
  * executor that builds a native validation Dataset. No validation rows are returned to the driver
  * as Spark task results or retained on the driver heap.
  */
private[lightgbm] class ValidationDataServer private(serverSocket: ServerSocket,
                                                      partitionFiles: Array[File],
                                                      val params: ValidationDataParams,
                                                      executor: ExecutorService,
                                                      spoolDirectory: File,
                                                      resources: ValidationDataServerResourceFactory)
  extends AutoCloseable with Logging {

  private val terminalFailure = new AtomicReference[Throwable]()
  private val transfers = new ConcurrentLinkedQueue[Future[_]]()
  private val activeClients = ConcurrentHashMap.newKeySet[Socket]()
  private val transferSlots = new Semaphore(ValidationDataServer.MaxConcurrentTransfers)
  private val stopping = new AtomicBoolean(false)
  @volatile private var serving: Option[Future[_]] = None
  @volatile private var cleanupComplete = false

  private[lightgbm] def start(): ValidationDataServer = synchronized {
    require(serving.isEmpty, "Validation data server was already started")
    serving = Option(executor.submit(new Runnable {
      override def run(): Unit = acceptClients()
    }))
    this
  }

  private def acceptClients(): Unit = {
    while (!stopping.get()) { // scalastyle:ignore while
      try {
        acceptClient()
      } catch {
        case _: java.net.SocketTimeoutException => ()
        case _: InterruptedException if stopping.get() =>
          Thread.currentThread().interrupt()
        case _: java.net.SocketException if stopping.get() => ()
        case NonFatal(failure) =>
          terminalFailure.compareAndSet(null, failure) // scalastyle:ignore null
          stopping.set(true)
      }
    }
  }

  private def acceptClient(): Unit = {
    transferSlots.acquire()
    var releaseSlot = true
    try {
      if (!stopping.get()) {
        releaseSlot = !submitClient(serverSocket.accept())
      }
    } finally {
      if (releaseSlot) transferSlots.release()
    }
  }

  private def submitClient(socket: Socket): Boolean = {
    if (!registerClient(socket)) {
      false
    } else {
      try {
        transfers.add(executor.submit(new Runnable {
          override def run(): Unit = {
            try {
              serve(socket)
            } catch {
              case NonFatal(failure) => handleServingFailure(socket, failure)
            } finally {
              closeClient(socket)
              transferSlots.release()
            }
          }
        }))
        true
      } catch {
        case NonFatal(failure) =>
          closeClient(socket)
          throw failure
      }
    }
  }

  private def registerClient(socket: Socket): Boolean = {
    activeClients.add(socket)
    if (stopping.get() && activeClients.remove(socket)) {
      ValidationDataServer.closeSocket(socket)
      false
    } else {
      true
    }
  }

  private def serve(socket: Socket): Unit = {
    socket.setKeepAlive(true)
    socket.setTcpNoDelay(true)
    socket.setSoTimeout(params.timeoutMillis)
    using(new DataInputStream(new BufferedInputStream(socket.getInputStream))) { auth =>
      if (auth.readUTF() != params.token) throw new SecurityException("Invalid validation data token")
      using(new BufferedOutputStream(resources.clientOutput(socket))) { output =>
        partitionFiles.foreach { file =>
          using(new BufferedInputStream(new FileInputStream(file))) { input =>
            copy(input, output)
          }.get
        }
        val end = new DataOutputStream(output)
        end.writeInt(ValidationDataServer.EndOfStream)
        end.flush()
      }.get
    }.get
  }

  private def handleServingFailure(socket: Socket, failure: Throwable): Unit = {
    if (ValidationDataServer.isExpectedClientTermination(stopping.get(), failure)) {
      log.debug("Validation data client disconnected before completing its transfer", failure)
    } else {
      terminalFailure.compareAndSet(null, failure) // scalastyle:ignore null
    }
  }

  private def closeClient(socket: Socket): Unit = {
    activeClients.remove(socket)
    try {
      ValidationDataServer.closeSocket(socket)
    } catch {
      case NonFatal(failure) =>
        if (!stopping.get()) terminalFailure.compareAndSet(null, failure) // scalastyle:ignore null
    }
  }

  def await(): Unit = {
    var failure = stopAcceptingAndClients()
    failure = ValidationDataServer.waitForFuture(serving, failure)
    val iterator = transfers.iterator()
    while (iterator.hasNext) { // scalastyle:ignore while
      failure = ValidationDataServer.waitForFuture(Option(iterator.next()), failure)
    }
    failure = ValidationDataServer.addFailure(failure, Option(terminalFailure.get()))
    failure.foreach(throw _)
  }

  override def close(): Unit = synchronized {
    if (!cleanupComplete) {
      var failure = stopAcceptingAndClients()
      executor.shutdownNow()
      val terminated = try {
        executor.awaitTermination(ValidationDataServer.ShutdownTimeoutSeconds, TimeUnit.SECONDS)
      } catch {
        case interrupted: InterruptedException =>
          Thread.currentThread().interrupt()
          failure = ValidationDataServer.addFailure(failure, Option(interrupted))
          false
      }
      if (!terminated) {
        failure = ValidationDataServer.addFailure(
          failure,
          Option(new IOException(
            "Validation data server executor did not terminate after all client sockets closed")))
      } else {
        try {
          ValidationDataServer.deleteSpoolDirectory(spoolDirectory)
          cleanupComplete = true
        } catch {
          case NonFatal(cleanupFailure) =>
            failure = ValidationDataServer.addFailure(failure, Option(cleanupFailure))
        }
      }
      failure.foreach(throw _)
    }
  }

  private def stopAcceptingAndClients(): Option[Throwable] = {
    stopping.set(true)
    var failure: Option[Throwable] = None
    try {
      serverSocket.close()
    } catch {
      case NonFatal(closeFailure) =>
        failure = ValidationDataServer.addFailure(failure, Option(closeFailure))
    }
    activeClients.asScala.foreach { socket =>
      try {
        ValidationDataServer.closeSocket(socket)
      } catch {
        case NonFatal(closeFailure) =>
          failure = ValidationDataServer.addFailure(failure, Option(closeFailure))
      }
    }
    failure
  }

  private def copy(input: InputStream, output: OutputStream): Unit = {
    val buffer = new Array[Byte](ValidationDataServer.CopyBufferSize)
    var count = input.read(buffer)
    while (count >= 0) { // scalastyle:ignore while
      output.write(buffer, 0, count)
      count = input.read(buffer)
    }
  }

  private[lightgbm] def activeClientCount: Int = activeClients.size()
  private[lightgbm] def executorIsTerminated: Boolean = executor.isTerminated
}

private[lightgbm] object ValidationDataServer {
  private val CopyBufferSize = 64 * 1024 // scalastyle:ignore magic.number
  private val EndOfStream = -1
  private val MaxConcurrentTransfers = 8
  private val IngestPollTimeoutMillis = 1000
  private val MillisPerSecond = 1000.0
  private val ShutdownTimeoutSeconds = 10
  private val DefaultSocketBacklog = 50
  private val ThreadCounter = new AtomicInteger()

  def create(validationData: DataFrame,
             host: String,
             partitionCount: Int,
             timeoutSeconds: Double): ValidationDataServer = {
    val spoolDirectory = new File(
      System.getProperty("user.dir"),
      s".synapseml-lightgbm-validation-spool-${UUID.randomUUID()}")
    create(validationData,
      host,
      partitionCount,
      timeoutSeconds,
      spoolDirectory,
      ValidationDataServerResourceFactory.Default)
  }

  private[lightgbm] def create(validationData: DataFrame,
                               host: String,
                               partitionCount: Int,
                               timeoutSeconds: Double,
                               spoolDirectory: File,
                               resources: ValidationDataServerResourceFactory): ValidationDataServer = {
    if (!spoolDirectory.mkdir()) {
      throw new IOException(s"Could not create validation spool directory ${spoolDirectory.getAbsolutePath}")
    }

    var spoolTransferred = false
    NetworkManagerSocketSupport.withCleanupPreservingPrimary(
      if (!spoolTransferred) deleteSpoolDirectory(spoolDirectory)) {
      val result = ingest(validationData, host, partitionCount, timeoutSeconds, spoolDirectory, resources)
      val partitionFiles = Option(spoolDirectory.listFiles()).getOrElse(Array.empty)
        .filter(_.getName.startsWith("part-"))
        .sortBy(file => file.getName.stripPrefix("part-").toInt)
      spoolTransferred = true
      createFromSpool(
        spoolDirectory,
        partitionFiles,
        result.rowCount,
        host,
        timeoutSeconds,
        partitionCount,
        resources)
    }
  }

  private case class IngestResult(rowCount: Long)

  // scalastyle:off method.length
  private def ingest(validationData: DataFrame,
                     host: String,
                     partitionCount: Int,
                     timeoutSeconds: Double,
                     spoolDirectory: File,
                     resources: ValidationDataServerResourceFactory): IngestResult = {
    val completedPartitions = ConcurrentHashMap.newKeySet[Int]()
    val partitionRowCounts = new ConcurrentHashMap[Int, Long]()
    val lastIngestFailure = new AtomicReference[Throwable]()
    val activeSockets = ConcurrentHashMap.newKeySet[Socket]()
    val ingestStopping = new AtomicBoolean(false)
    val ingestSocket = resources.openServerSocket(host, timeoutSeconds, DefaultSocketBacklog)
    val timeoutMillis = ingestSocket.getSoTimeout
    val ingestToken = UUID.randomUUID().toString
    ingestSocket.setSoTimeout(IngestPollTimeoutMillis)
    val ingestPort = ingestSocket.getLocalPort
    val ingestExecutor = NetworkManagerSocketSupport.withCleanupOnFailurePreservingPrimary(
      closeServerSocket(ingestSocket)) {
      resources.createExecutor(MaxConcurrentTransfers + 1, "validation-ingest")
    }
    NetworkManagerSocketSupport.withCleanupPreservingPrimary(
      cleanupAll(
        () => {
          ingestStopping.set(true)
          closeServerSocket(ingestSocket)
        },
        () => closeSockets(activeSockets),
        () => shutdownExecutor(ingestExecutor, "validation ingest"))) {
      val ingestSlots = new Semaphore(MaxConcurrentTransfers)
      val accepting = ingestExecutor.submit(new Runnable {
        override def run(): Unit = {
          val writes = new ArrayBuffer[Future[_]]()
          val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(Math.max(1L, timeoutSeconds.toLong))
          while (completedPartitions.size() < partitionCount) { // scalastyle:ignore while
            var slotAcquired = false
            var releaseSlot = true
            try {
              ingestSlots.acquire()
              slotAcquired = true
              if (completedPartitions.size() < partitionCount) {
                val socket = ingestSocket.accept()
                activeSockets.add(socket)
                if (ingestStopping.get() && activeSockets.remove(socket)) {
                  closeSocket(socket)
                } else {
                  writes += ingestExecutor.submit(new Runnable {
                    override def run(): Unit = {
                      try {
                        val (partitionId, count, accepted) =
                          receivePartition(socket, spoolDirectory, ingestToken, timeoutMillis)
                        if (accepted) {
                          partitionRowCounts.put(partitionId, count)
                          completedPartitions.add(partitionId)
                        }
                      } catch {
                        case NonFatal(failure)
                          if isExpectedClientTermination(ingestSocket.isClosed, failure) => ()
                        case NonFatal(failure) =>
                          lastIngestFailure.compareAndSet(null, failure) // scalastyle:ignore null
                      } finally {
                        activeSockets.remove(socket)
                        closeSocketQuietly(socket)
                        ingestSlots.release()
                      }
                    }
                  })
                  releaseSlot = false
                }
              }
            } catch {
              case _: java.net.SocketTimeoutException if System.nanoTime() < deadline => ()
              case _: java.net.SocketTimeoutException =>
                throw Option(lastIngestFailure.get()).getOrElse(
                  new IOException("Timed out receiving validation partitions"))
            } finally {
              if (slotAcquired && releaseSlot) ingestSlots.release()
            }
          }
          closeSockets(activeSockets)
          writes.foreach(_.get())
        }
      })

      validationData.foreachPartition((rows: Iterator[Row]) =>
        writePartition(host, ingestPort, ingestToken, timeoutMillis, rows))
      accepting.get()
      IngestResult(partitionRowCounts.values().asScala.sum)
    }
  }
  // scalastyle:on method.length

  private[lightgbm] def createFromSpool(spoolDirectory: File,
                                        partitionFiles: Array[File],
                                        rowCount: Long,
                                        host: String,
                                        timeoutSeconds: Double,
                                        backlog: Int,
                                        resources: ValidationDataServerResourceFactory): ValidationDataServer = {
    var spoolRetained = false
    NetworkManagerSocketSupport.withCleanupPreservingPrimary(
      if (!spoolRetained) deleteSpoolDirectory(spoolDirectory)) {
      val serverSocket = resources.openServerSocket(host, timeoutSeconds, backlog)
      val executor = NetworkManagerSocketSupport.withCleanupOnFailurePreservingPrimary(
        closeServerSocket(serverSocket)) {
        resources.createExecutor(MaxConcurrentTransfers + 1, "validation-serve")
      }
      val server = NetworkManagerSocketSupport.withCleanupOnFailurePreservingPrimary(
        cleanupAll(
          () => closeServerSocket(serverSocket),
          () => shutdownExecutor(executor, "validation serving"))) {
        new ValidationDataServer(
          serverSocket,
          partitionFiles,
          ValidationDataParams(
            host,
            serverSocket.getLocalPort,
            rowCount,
            serverSocket.getSoTimeout,
            UUID.randomUUID().toString),
          executor,
          spoolDirectory,
          resources).start()
      }
      spoolRetained = true
      server
    }
  }

  private[lightgbm] def openServerSocket(host: String,
                                         timeoutSeconds: Double,
                                         backlog: Int = DefaultSocketBacklog): ServerSocket = {
    val socket = new ServerSocket()
    NetworkManagerSocketSupport.withCleanupOnFailurePreservingPrimary(closeServerSocket(socket)) {
      socket.bind(
        new InetSocketAddress(InetAddress.getByName(host), 0),
        Math.max(DefaultSocketBacklog, backlog))
      val timeoutMillis = (timeoutSeconds * MillisPerSecond).toLong
      socket.setSoTimeout(Math.max(IngestPollTimeoutMillis, timeoutMillis).min(Int.MaxValue).toInt)
      socket
    }
  }

  private[lightgbm] def daemonThreadFactory(prefix: String): ThreadFactory = {
    new ThreadFactory {
      override def newThread(runnable: Runnable): Thread = {
        val thread = new Thread(runnable, s"synapseml-$prefix-${ThreadCounter.incrementAndGet()}")
        thread.setDaemon(true)
        thread
      }
    }
  }

  private[lightgbm] def isExpectedClientTermination(stopping: Boolean,
                                                     failure: Throwable): Boolean = {
    failure match {
      case _: EOFException => true
      case _: java.net.SocketException => true
      case _: java.net.SocketTimeoutException => true
      case _: SecurityException => true
      case _: InterruptedException if stopping => true
      case _ => false
    }
  }

  private[lightgbm] def waitForFuture(future: Option[Future[_]],
                                      existingFailure: Option[Throwable]): Option[Throwable] = {
    future.fold(existingFailure) { pending =>
      try {
        pending.get()
        existingFailure
      } catch {
        case execution: java.util.concurrent.ExecutionException =>
          addFailure(existingFailure, Option(execution.getCause).orElse(Option(execution)))
        case interrupted: InterruptedException =>
          Thread.currentThread().interrupt()
          addFailure(existingFailure, Option(interrupted))
        case NonFatal(failure) =>
          addFailure(existingFailure, Option(failure))
      }
    }
  }

  private[lightgbm] def addFailure(existing: Option[Throwable],
                                    additional: Option[Throwable]): Option[Throwable] = {
    (existing, additional) match {
      case (None, failure) => failure
      case (failure, None) => failure
      case (Some(primary), Some(secondary)) =>
        NetworkManagerSocketSupport.addSuppressed(primary, secondary)
        existing
    }
  }

  private def cleanupAll(actions: (() => Unit)*): Unit = {
    var failure: Option[Throwable] = None
    actions.foreach { action =>
      try {
        action()
      } catch {
        case NonFatal(cleanupFailure) =>
          failure = addFailure(failure, Option(cleanupFailure))
      }
    }
    failure.foreach(throw _)
  }

  private def shutdownExecutor(executor: ExecutorService, description: String): Unit = {
    executor.shutdownNow()
    val terminated = try {
      executor.awaitTermination(ShutdownTimeoutSeconds, TimeUnit.SECONDS)
    } catch {
      case interrupted: InterruptedException =>
        Thread.currentThread().interrupt()
        throw interrupted
    }
    if (!terminated) {
      throw new IOException(s"$description executor did not terminate after socket cleanup")
    }
  }

  private[lightgbm] def closeSocket(socket: Socket): Unit = {
    NetworkManagerSocketSupport.closeSocketWithRetry(socket)
  }

  private def closeSocketQuietly(socket: Socket): Unit = {
    try {
      closeSocket(socket)
    } catch {
      case NonFatal(_) => ()
    }
  }

  private def closeSockets(sockets: java.util.Set[Socket]): Unit = {
    var failure: Option[Throwable] = None
    sockets.asScala.foreach { socket =>
      try {
        closeSocket(socket)
      } catch {
        case NonFatal(closeFailure) =>
          failure = addFailure(failure, Option(closeFailure))
      }
    }
    failure.foreach(throw _)
  }

  private def closeServerSocket(socket: ServerSocket): Unit = {
    if (!socket.isClosed) socket.close()
  }

  private[lightgbm] def deleteSpoolDirectory(spoolDirectory: File): Unit = {
    if (spoolDirectory.exists()) FileUtils.deleteDirectory(spoolDirectory)
  }

  private def writePartition(host: String,
                             port: Int,
                             token: String,
                             timeoutMillis: Int,
                             rows: Iterator[Row]): Unit = {
    using(connect(host, port, timeoutMillis)) { socket =>
      socket.setKeepAlive(true)
      socket.setTcpNoDelay(true)
      using(new DataOutputStream(new BufferedOutputStream(socket.getOutputStream))) { output =>
        output.writeUTF(token)
        output.writeInt(org.apache.spark.TaskContext.getPartitionId())
        val serializer = SparkEnv.get.serializer.newInstance()
        rows.foreach { row =>
          val bytes = toBytes(serializer.serialize(row))
          output.writeInt(bytes.length)
          output.write(bytes)
        }
        output.writeInt(EndOfStream)
        output.flush()
      }.get
    }.get
  }

  private[lightgbm] def receivePartition(socket: Socket,
                                         spoolDirectory: File,
                                         token: String,
                                         timeoutMillis: Int): (Int, Long, Boolean) = {
    using(socket) { client =>
      client.setKeepAlive(true)
      client.setTcpNoDelay(true)
      client.setSoTimeout(timeoutMillis)
      using(new DataInputStream(new BufferedInputStream(client.getInputStream))) { input =>
        if (input.readUTF() != token) throw new SecurityException("Invalid validation partition token")
        val partitionId = input.readInt()
        val attemptFile = new File(spoolDirectory, s".attempt-$partitionId-${UUID.randomUUID()}")
        var count = 0L
        try {
          using(new DataOutputStream(new BufferedOutputStream(new FileOutputStream(attemptFile)))) { output =>
            var length = readRowLength(input, "validation partition")
            while (length != EndOfStream) { // scalastyle:ignore while
              output.writeInt(length)
              copyExactly(input, output, length)
              count += 1
              length = readRowLength(input, "validation partition")
            }
          }.get
          val accepted = try {
            Files.move(attemptFile.toPath, new File(spoolDirectory, s"part-$partitionId").toPath)
            true
          } catch {
            case _: java.nio.file.FileAlreadyExistsException => false
          }
          (partitionId, count, accepted)
        } finally {
          FileUtils.deleteQuietly(attemptFile)
        }
      }.get
    }.get
  }

  def rowCount(data: Broadcast[Array[Row]]): Int = {
    ValidationDataParams.fromBroadcast(data)
      .map { params =>
        if (params.rowCount < 0 || params.rowCount > Int.MaxValue) {
          throw new IllegalArgumentException(
            s"Validation row count ${params.rowCount} is outside the supported range 0 to ${Int.MaxValue}")
        }
        params.rowCount.toInt
      }
      .getOrElse(data.value.length)
  }

  def read(data: Broadcast[Array[Row]]): ValidationRowIterator = {
    ValidationDataParams.fromBroadcast(data).map(read).getOrElse(new ValidationRowIterator {
      private val rows = data.value.iterator
      override def hasNext: Boolean = rows.hasNext
      override def next(): Row = rows.next()
      override def close(): Unit = ()
    })
  }

  private def read(params: ValidationDataParams): ValidationRowIterator = {
    new ValidationRowIterator {
      private val socket = connect(params.host, params.port, params.timeoutMillis)
      private val input = openValidationInput(socket, params.token, params.timeoutMillis)
      private val serializer = SparkEnv.get.serializer.newInstance()
      private var closed = false
      private var nextRow: Option[Row] = readNext()

      private def readNext(): Option[Row] = {
        try {
          val length = readRowLength(input, "validation data")
          if (length == EndOfStream) {
            close()
            None
          } else {
            val bytes = new Array[Byte](length)
            input.readFully(bytes)
            Some(serializer.deserialize[Row](ByteBuffer.wrap(bytes)))
          }
        } catch {
          case NonFatal(failure) =>
            close()
            throw failure
        }
      }

      override def hasNext: Boolean = nextRow.nonEmpty

      override def next(): Row = {
        val result = nextRow.getOrElse(throw new NoSuchElementException("Validation data stream is exhausted"))
        nextRow = readNext()
        result
      }

      override def close(): Unit = {
        if (!closed) {
          closed = true
          try input.close()
          finally socket.close()
        }
      }
    }
  }

  private def toBytes(buffer: ByteBuffer): Array[Byte] = {
    val bytes = new Array[Byte](buffer.remaining())
    buffer.get(bytes)
    bytes
  }

  private def connect(host: String, port: Int, timeoutMillis: Int): Socket = {
    val socket = new Socket()
    try {
      socket.connect(new InetSocketAddress(host, port), timeoutMillis)
      socket
    } catch {
      case NonFatal(failure) =>
        socket.close()
        throw failure
    }
  }

  private[lightgbm] def openValidationInput(socket: Socket,
                                             token: String,
                                             timeoutMillis: Int): DataInputStream = {
    NetworkManagerSocketSupport.withCleanupOnFailurePreservingPrimary(closeSocket(socket)) {
      socket.setKeepAlive(true)
      socket.setTcpNoDelay(true)
      socket.setSoTimeout(timeoutMillis)
      val auth = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream))
      auth.writeUTF(token)
      auth.flush()
      new DataInputStream(new BufferedInputStream(socket.getInputStream))
    }
  }

  private def copyExactly(input: InputStream, output: OutputStream, length: Int): Unit = {
    val buffer = new Array[Byte](Math.min(CopyBufferSize, length))
    var remaining = length
    while (remaining > 0) { // scalastyle:ignore while
      val count = input.read(buffer, 0, Math.min(buffer.length, remaining))
      if (count < 0) throw new EOFException("Validation partition stream ended before the current row")
      output.write(buffer, 0, count)
      remaining -= count
    }
  }

  private[lightgbm] def readRowLength(input: DataInputStream, streamDescription: String): Int = {
    val length = input.readInt()
    if (length < 0 && length != EndOfStream) {
      throw new IOException(
        s"Invalid $streamDescription row length $length; expected a non-negative length or $EndOfStream")
    }
    length
  }
}
