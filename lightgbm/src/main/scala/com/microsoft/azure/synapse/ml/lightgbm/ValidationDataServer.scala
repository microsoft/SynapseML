// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm

import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.sql.{DataFrame, Encoders, Row}

import java.io.{BufferedInputStream, BufferedOutputStream, DataInputStream, DataOutputStream, EOFException}
import java.io.{File, FileInputStream, IOException, InputStream, OutputStream}
import java.net.{InetAddress, InetSocketAddress, ServerSocket, Socket}
import java.nio.ByteBuffer
import java.nio.file.Files
import java.util.UUID
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}
import java.util.concurrent.{ConcurrentHashMap, ExecutorService}
import java.util.concurrent.{Semaphore, ThreadFactory, TimeUnit}
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
  private val MarkerPrefix = "SynapseML.ValidationDataServer."
  private val MetadataFieldCount = 6
  private val HostIndex = 1
  private val PortIndex = 2
  private val RowCountIndex = 3
  private val TimeoutIndex = 4
  private val TokenIndex = 5

  def fromBroadcast(data: Broadcast[Array[Row]]): Option[ValidationDataParams] = {
    data.value.headOption.flatMap { row =>
      val marker = if (row.length == 0) None else Option(row.get(0)).collect { case value: String => value }
      marker match {
        case Some(Marker) =>
          if (row.length != MetadataFieldCount) {
            throw new IllegalArgumentException(
              s"Invalid validation data descriptor: expected $MetadataFieldCount fields but found ${row.length}")
          }
          Some(ValidationDataParams(
            row.getString(HostIndex),
            row.getInt(PortIndex),
            row.getLong(RowCountIndex),
            row.getInt(TimeoutIndex),
            row.getString(TokenIndex)))
        case Some(unsupported) if unsupported.startsWith(MarkerPrefix) =>
          throw new IllegalArgumentException(s"Unsupported validation data descriptor '$unsupported'")
        case _ => None
      }
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
  * as Spark task results or retained on the driver heap. The native validation contract still
  * requires linear driver-disk usage and one full transfer per native worker; memory buffers and
  * concurrent transfer threads remain fixed-size.
  */
private[lightgbm] class ValidationDataServer private(serverSocket: ServerSocket,
                                                      partitionFiles: Array[File],
                                                      val params: ValidationDataParams,
                                                      executor: ExecutorService,
                                                      spoolDirectory: File,
                                                      resources: ValidationDataServerResourceFactory)
  extends AutoCloseable with Logging {

  private val terminalFailure = new AtomicReference[Throwable]()
  private val activeClients = ConcurrentHashMap.newKeySet[Socket]()
  private val transferSlots = new Semaphore(ValidationDataServer.MaxConcurrentTransfers)
  private val stopping = new AtomicBoolean(false)
  private var started = false
  @volatile private var cleanupComplete = false

  private[lightgbm] def start(): ValidationDataServer = synchronized {
    require(!started, "Validation data server was already started")
    executor.execute(new Runnable {
      override def run(): Unit = acceptClients()
    })
    started = true
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
          NetworkManagerSocketSupport.recordFailure(terminalFailure, failure)
          stopAcceptingAndClients().foreach(
            cleanupFailure => NetworkManagerSocketSupport.recordFailure(terminalFailure, cleanupFailure))
      }
    }
  }

  private def acceptClient(): Unit = {
    var releaseSlot = false
    try {
      transferSlots.acquire()
      releaseSlot = true
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
        executor.execute(new Runnable {
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
        })
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
    ValidationDataServer.withResource(new DataInputStream(new BufferedInputStream(socket.getInputStream))) { auth =>
      if (auth.readUTF() != params.token) throw new SecurityException("Invalid validation data token")
      NetworkManagerSocketSupport.withSocketWriteTimeout(socket, params.timeoutMillis) { reportProgress =>
        ValidationDataServer.withResource(new BufferedOutputStream(resources.clientOutput(socket))) { output =>
          partitionFiles.foreach { file =>
            ValidationDataServer.withResource(new BufferedInputStream(new FileInputStream(file))) { input =>
              copy(input, output, reportProgress)
            }
          }
          val end = new DataOutputStream(output)
          end.writeInt(ValidationDataServer.EndOfStream)
          end.flush()
          reportProgress()
        }
      }
    }
  }

  private def handleServingFailure(socket: Socket, failure: Throwable): Unit = {
    if (ValidationDataServer.isExpectedClientTermination(stopping.get(), failure)) {
      log.debug("Validation data client disconnected before completing its transfer", failure)
    } else {
      NetworkManagerSocketSupport.recordFailure(terminalFailure, failure)
    }
  }

  private def closeClient(socket: Socket): Unit = {
    activeClients.remove(socket)
    try {
      ValidationDataServer.closeSocket(socket)
    } catch {
      case NonFatal(failure) =>
        NetworkManagerSocketSupport.recordFailure(terminalFailure, failure)
    }
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
        failure = ValidationDataServer.addFailure(failure, Option(terminalFailure.get()))
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

  private def copy(input: InputStream,
                   output: OutputStream,
                   reportProgress: () => Unit): Unit = {
    val buffer = ValidationDataServer.ThreadCopyBuffer.get()
    var count = input.read(buffer)
    while (count >= 0) { // scalastyle:ignore while
      output.write(buffer, 0, count)
      reportProgress()
      count = input.read(buffer)
    }
  }
}

private[lightgbm] object ValidationDataServer {
  private val CopyBufferSize = 64 * 1024 // scalastyle:ignore magic.number
  private val ThreadCopyBuffer = new ThreadLocal[Array[Byte]] {
    override protected def initialValue(): Array[Byte] = new Array[Byte](CopyBufferSize)
  }
  private[lightgbm] val EndOfStream = -1
  private val MaxConcurrentTransfers = 8
  private val IngestSocketBacklog = 1024
  private val IngestPollTimeoutMillis = 1000
  private val MillisPerSecond = 1000.0
  private val ShutdownTimeoutSeconds = 10
  private val DefaultSocketBacklog = 50
  private val ThreadCounter = new AtomicInteger()

  def create(validationData: DataFrame,
             host: String,
             partitionCount: Int,
             timeoutSeconds: Double): ValidationDataServer = {
    createWithPreparedSpool(validationData,
      host,
      partitionCount,
      timeoutSeconds,
      createSpoolDirectory(new File(System.getProperty("user.dir"))),
      ValidationDataServerResourceFactory.Default)
  }

  private[lightgbm] def createSpoolDirectory(preferredParent: File): File = {
    try Files.createTempDirectory(preferredParent.toPath, ".synapseml-lightgbm-validation-spool-").toFile
    catch {
      case NonFatal(preferredFailure) =>
        try Files.createTempDirectory(".synapseml-lightgbm-validation-spool-").toFile
        catch {
          case NonFatal(fallbackFailure) =>
            fallbackFailure.addSuppressed(preferredFailure)
            throw fallbackFailure
        }
    }
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

    createWithPreparedSpool(
      validationData, host, partitionCount, timeoutSeconds, spoolDirectory, resources)
  }

  private def createWithPreparedSpool(validationData: DataFrame,
                                      host: String,
                                      partitionCount: Int,
                                      timeoutSeconds: Double,
                                      spoolDirectory: File,
                                      resources: ValidationDataServerResourceFactory): ValidationDataServer = {
    var spoolTransferred = false
    NetworkManagerSocketSupport.withCleanupPreservingPrimary(
      if (!spoolTransferred) deleteSpoolDirectory(spoolDirectory)) {
      val result = ingest(validationData, host, partitionCount, timeoutSeconds, spoolDirectory, resources)
      checkedRowCount(result.rowCount)
      val partitionFiles = ValidationDataSpool.listPartitionFiles(spoolDirectory, result.partitionCount)
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

  private case class IngestResult(rowCount: Long, partitionCount: Int)

  private def receiveIngestPartition(socket: Socket,
                                     spoolDirectory: File,
                                     token: String,
                                     timeoutMillis: Int,
                                     serverClosed: => Boolean,
                                     activeSockets: java.util.Set[Socket],
                                     ingestSlots: Semaphore,
                                     lastFailure: AtomicReference[Throwable]): Unit = {
    try {
      ValidationDataIngest.receivePartition(socket, spoolDirectory, token, timeoutMillis)
    } catch {
      case NonFatal(failure) if isExpectedClientTermination(serverClosed, failure) => ()
      case NonFatal(failure) => NetworkManagerSocketSupport.recordFailure(lastFailure, failure)
    } finally {
      activeSockets.remove(socket)
      try {
        closeSocket(socket)
      } catch {
        case NonFatal(closeFailure) => NetworkManagerSocketSupport.recordFailure(lastFailure, closeFailure)
      }
      ingestSlots.release()
    }
  }

  private def acceptIngestPartitions(ingestSocket: ServerSocket,
                                     spoolDirectory: File,
                                     token: String,
                                     timeoutMillis: Int,
                                     ingestExecutor: ExecutorService,
                                     ingestSlots: Semaphore,
                                     activeSockets: java.util.Set[Socket],
                                     stopping: AtomicBoolean,
                                     lastFailure: AtomicReference[Throwable]): Unit = {
    val deadline = ingestDeadlineNanos(System.nanoTime(), timeoutMillis)
    while (!stopping.get()) { // scalastyle:ignore while
      var slotAcquired = false
      var releaseSlot = true
      try {
        ingestSlots.acquire()
        slotAcquired = true
        releaseSlot = submitIngestPartition(
          ingestSocket,
          spoolDirectory,
          token,
          timeoutMillis,
          ingestExecutor,
          ingestSlots,
          activeSockets,
          stopping,
          lastFailure)
      } catch {
        case _: java.net.SocketTimeoutException if System.nanoTime() < deadline => ()
        case _: java.net.SocketTimeoutException =>
          throw Option(lastFailure.get()).getOrElse(new IOException("Timed out receiving validation partitions"))
        case _: java.net.SocketException if stopping.get() => ()
      } finally {
        if (slotAcquired && releaseSlot) ingestSlots.release()
      }
    }
    closeSockets(activeSockets)
    ingestSlots.acquire(MaxConcurrentTransfers)
  }

  private def submitIngestPartition(ingestSocket: ServerSocket,
                                    spoolDirectory: File,
                                    token: String,
                                    timeoutMillis: Int,
                                    ingestExecutor: ExecutorService,
                                    ingestSlots: Semaphore,
                                    activeSockets: java.util.Set[Socket],
                                    stopping: AtomicBoolean,
                                    lastFailure: AtomicReference[Throwable]): Boolean = {
    if (stopping.get()) {
      true
    } else {
      val socket = ingestSocket.accept()
      activeSockets.add(socket)
      if (stopping.get() && activeSockets.remove(socket)) {
        closeSocket(socket)
        true
      } else {
        ingestExecutor.submit(new Runnable {
          override def run(): Unit = receiveIngestPartition(
            socket,
            spoolDirectory,
            token,
            timeoutMillis,
            ingestSocket.isClosed,
            activeSockets,
            ingestSlots,
            lastFailure)
        })
        false
      }
    }
  }

  // scalastyle:off method.length
  private def ingest(validationData: DataFrame,
                     host: String,
                     servingBacklog: Int,
                     timeoutSeconds: Double,
                     spoolDirectory: File,
                     resources: ValidationDataServerResourceFactory): IngestResult = {
    val lastIngestFailure = new AtomicReference[Throwable]()
    val activeSockets = ConcurrentHashMap.newKeySet[Socket]()
    val ingestStopping = new AtomicBoolean(false)
    val ingestSocket = resources.openServerSocket(
      host, timeoutSeconds, Math.max(IngestSocketBacklog, servingBacklog))
    val timeoutMillis = ingestSocket.getSoTimeout
    val ingestToken = UUID.randomUUID().toString
    ingestSocket.setSoTimeout(Math.min(IngestPollTimeoutMillis, timeoutMillis))
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
        override def run(): Unit = acceptIngestPartitions(
          ingestSocket,
          spoolDirectory,
          ingestToken,
          timeoutMillis,
          ingestExecutor,
          ingestSlots,
          activeSockets,
          ingestStopping,
          lastIngestFailure)
      })

      val successfulAttempts = validationData.mapPartitions { rows =>
        val partitionId = TaskContext.getPartitionId()
        Iterator.single(
          ValidationDataIngest.writePartition(host, ingestPort, ingestToken, timeoutMillis, partitionId, rows))
      }(Encoders.product[ValidationPartitionAttempt]).collect()
      ingestStopping.set(true)
      closeServerSocket(ingestSocket)
      NetworkManagerSocketSupport.awaitFuture(accepting)
      Option(lastIngestFailure.get()).foreach(failure => throw failure)
      IngestResult(
        ValidationDataIngest.promoteSuccessfulAttempts(spoolDirectory, successfulAttempts),
        successfulAttempts.length)
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
      val timeoutMillis = Math.ceil(timeoutSeconds * MillisPerSecond).toLong
      socket.setSoTimeout(Math.max(1L, timeoutMillis).min(Int.MaxValue).toInt)
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
      case _: InterruptedException if stopping => true
      case _ => false
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

  private def withResource[R <: AutoCloseable, T](resource: R)(operation: R => T): T = {
    NetworkManagerSocketSupport.withCleanupPreservingPrimary(resource.close()) {
      operation(resource)
    }
  }

  private def withSocket[T](socket: Socket)(operation: Socket => T): T = {
    NetworkManagerSocketSupport.withCleanupPreservingPrimary(closeSocket(socket)) {
      operation(socket)
    }
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

  def rowCount(data: Broadcast[Array[Row]]): Int = {
    ValidationDataParams.fromBroadcast(data)
      .map(params => checkedRowCount(params.rowCount))
      .getOrElse(data.value.length)
  }

  private def checkedRowCount(rowCount: Long): Int = {
    if (rowCount < 0 || rowCount > Int.MaxValue) {
      throw new IllegalArgumentException(
        s"Validation row count $rowCount is outside the supported range 0 to ${Int.MaxValue}")
    }
    rowCount.toInt
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
    read(params, SparkEnv.get.serializer.newInstance())
  }

  private[lightgbm] def read(params: ValidationDataParams,
                             serializer: SerializerInstance): ValidationRowIterator = {
    new ValidationRowIterator {
      private val socket = connect(params.host, params.port, params.timeoutMillis)
      private val input = openValidationInput(socket, params.token, params.timeoutMillis)
      private var closed = false
      private var nextRow: Option[Row] = {
        socket.setSoTimeout(0)
        try readNext()
        finally if (!closed) socket.setSoTimeout(params.timeoutMillis)
      }

      private def readNext(): Option[Row] = {
        NetworkManagerSocketSupport.withCleanupOnFailurePreservingPrimary(close()) {
          val length = readRowLength(input, "validation data")
          if (length == EndOfStream) {
            close()
            None
          } else {
            val bytes = new Array[Byte](length)
            input.readFully(bytes)
            Some(serializer.deserialize[Row](ByteBuffer.wrap(bytes)))
          }
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
          cleanupAll(
            () => input.close(),
            () => closeSocket(socket))
        }
      }
    }
  }

  private[lightgbm] def connect(host: String, port: Int, timeoutMillis: Int): Socket = {
    val socket = new Socket()
    NetworkManagerSocketSupport.withCleanupOnFailurePreservingPrimary(closeSocket(socket)) {
      socket.connect(new InetSocketAddress(host, port), timeoutMillis)
      socket
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

  private[lightgbm] def copyExactly(input: InputStream, output: OutputStream, length: Int): Unit = {
    val buffer = ThreadCopyBuffer.get()
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

  private[lightgbm] def ingestDeadlineNanos(startNanos: Long, timeoutMillis: Int): Long = {
    startNanos + TimeUnit.MILLISECONDS.toNanos(timeoutMillis.toLong)
  }

  private[lightgbm] def withRows[T](rows: ValidationRowIterator)(operation: => T): T = {
    NetworkManagerSocketSupport.withCleanupPreservingPrimary(rows.close())(operation)
  }
}
