// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm

import com.microsoft.azure.synapse.ml.core.env.StreamUtilities.using
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkEnv
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Row}

import java.io._
import java.net.{InetAddress, InetSocketAddress, ServerSocket, Socket}
import java.nio.ByteBuffer
import java.nio.file.Files
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, ExecutorService}
import java.util.concurrent.{Executors, Future, Semaphore, TimeUnit}
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

/**
  * Spools exact validation rows through bounded socket buffers, then streams the spool to each
  * executor that builds a native validation Dataset. No validation rows are returned to the driver
  * as Spark task results or retained on the driver heap.
  */
private[lightgbm] class ValidationDataServer private(serverSocket: ServerSocket,
                                                      partitionFiles: Array[File],
                                                      val params: ValidationDataParams,
                                                      executor: ExecutorService,
                                                      spoolDirectory: File)
  extends AutoCloseable {

  private val terminalFailure = new AtomicReference[Throwable]()
  private val transfers = new ConcurrentLinkedQueue[Future[_]]()
  private val transferSlots = new Semaphore(ValidationDataServer.MaxConcurrentTransfers)
  private val serving: Future[_] = executor.submit(new Runnable {
    override def run(): Unit = {
      try {
        while (!serverSocket.isClosed) { // scalastyle:ignore while
          transferSlots.acquire()
          val socket = serverSocket.accept()
          transfers.add(executor.submit(new Runnable {
            override def run(): Unit = {
              try serve(socket)
              catch {
                case NonFatal(failure) =>
                  terminalFailure.compareAndSet(null, failure) // scalastyle:ignore null
              } finally {
                transferSlots.release()
              }
            }
          }))
        }
      } catch {
        case _: java.net.SocketException if serverSocket.isClosed => ()
        case NonFatal(failure) =>
          terminalFailure.compareAndSet(null, failure) // scalastyle:ignore null
      }
    }
  })

  private def serve(socket: Socket): Unit = {
    using(socket) { client =>
      client.setKeepAlive(true)
      client.setTcpNoDelay(true)
      client.setSoTimeout(params.timeoutMillis)
      using(new DataInputStream(new BufferedInputStream(client.getInputStream))) { auth =>
        if (auth.readUTF() != params.token) throw new SecurityException("Invalid validation data token")
        using(new BufferedOutputStream(client.getOutputStream)) { output =>
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
    }.get
  }

  def await(): Unit = {
    serverSocket.close()
    serving.get()
    val iterator = transfers.iterator()
    while (iterator.hasNext) iterator.next().get() // scalastyle:ignore while
    Option(terminalFailure.get()).foreach(throw _)
  }

  override def close(): Unit = {
    serverSocket.close()
    serving.cancel(true)
    executor.shutdownNow()
    executor.awaitTermination(ValidationDataServer.ShutdownTimeoutSeconds, TimeUnit.SECONDS)
    FileUtils.deleteQuietly(spoolDirectory)
  }

  private def copy(input: InputStream, output: OutputStream): Unit = {
    val buffer = new Array[Byte](ValidationDataServer.CopyBufferSize)
    var count = input.read(buffer)
    while (count >= 0) { // scalastyle:ignore while
      output.write(buffer, 0, count)
      count = input.read(buffer)
    }
  }
}

private[lightgbm] object ValidationDataServer {
  private val CopyBufferSize = 64 * 1024 // scalastyle:ignore magic.number
  private val EndOfStream = -1
  private val MaxConcurrentTransfers = 8
  private val IngestPollTimeoutMillis = 1000
  private val ShutdownTimeoutSeconds = 10
  private val DefaultSocketBacklog = 50

  // The lifecycle is intentionally kept together so every socket, thread, and spool file shares one cleanup path.
  // scalastyle:off method.length
  def create(validationData: DataFrame,
             host: String,
             partitionCount: Int,
             timeoutSeconds: Double): ValidationDataServer = {
    val spoolDirectory = new File(
      System.getProperty("user.dir"),
      s".synapseml-lightgbm-validation-spool-${UUID.randomUUID()}")
    if (!spoolDirectory.mkdir()) {
      throw new IOException(s"Could not create validation spool directory ${spoolDirectory.getAbsolutePath}")
    }

    val completedPartitions = ConcurrentHashMap.newKeySet[Int]()
    val partitionRowCounts = new ConcurrentHashMap[Int, Long]()
    val lastIngestFailure = new AtomicReference[Throwable]()
    val ingestSocket = openServerSocket(host, timeoutSeconds)
    val timeoutMillis = ingestSocket.getSoTimeout
    val ingestToken = UUID.randomUUID().toString
    ingestSocket.setSoTimeout(IngestPollTimeoutMillis)
    val ingestPort = ingestSocket.getLocalPort
    val ingestExecutor = Executors.newFixedThreadPool(MaxConcurrentTransfers + 1)
    val ingestSlots = new Semaphore(MaxConcurrentTransfers)
    try {
      val accepting = ingestExecutor.submit(new Runnable {
        override def run(): Unit = {
          val writes = new ArrayBuffer[Future[_]]()
          val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(Math.max(1L, timeoutSeconds.toLong))
          while (completedPartitions.size() < partitionCount) { // scalastyle:ignore while
            ingestSlots.acquire()
            if (completedPartitions.size() >= partitionCount) {
              ingestSlots.release()
            } else {
              try {
                val socket = ingestSocket.accept()
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
                      case NonFatal(failure) => lastIngestFailure.set(failure)
                    } finally {
                      ingestSlots.release()
                    }
                  }
                })
              } catch {
                case _: java.net.SocketTimeoutException =>
                  ingestSlots.release()
                  if (System.nanoTime() >= deadline) {
                    throw Option(lastIngestFailure.get()).getOrElse(
                      new IOException("Timed out receiving validation partitions"))
                  }
              }
            }
          }
          writes.foreach(_.get())
        }
      })

      validationData.foreachPartition((rows: Iterator[Row]) =>
        writePartition(host, ingestPort, ingestToken, timeoutMillis, rows))
      accepting.get()
    } catch {
      case NonFatal(failure) =>
        ingestSocket.close()
        FileUtils.deleteQuietly(spoolDirectory)
        throw failure
    } finally {
      ingestSocket.close()
      ingestExecutor.shutdownNow()
      ingestExecutor.awaitTermination(ShutdownTimeoutSeconds, TimeUnit.SECONDS)
    }

    val partitionFiles = Option(spoolDirectory.listFiles()).getOrElse(Array.empty)
      .filter(_.getName.startsWith("part-"))
      .sortBy(file => file.getName.stripPrefix("part-").toInt)
    val serverSocket = openServerSocket(host, timeoutSeconds, partitionCount)
    val executor = Executors.newFixedThreadPool(MaxConcurrentTransfers + 1)
    val serverToken = UUID.randomUUID().toString
    new ValidationDataServer(
      serverSocket,
      partitionFiles,
      ValidationDataParams(
        host,
        serverSocket.getLocalPort,
        partitionRowCounts.values().asScala.sum,
        serverSocket.getSoTimeout,
        serverToken),
      executor,
      spoolDirectory)
  }
  // scalastyle:on method.length

  private def openServerSocket(host: String,
                               timeoutSeconds: Double,
                               backlog: Int = DefaultSocketBacklog): ServerSocket = {
    val socket = new ServerSocket()
    socket.bind(
      new InetSocketAddress(InetAddress.getByName(host), 0),
      Math.max(DefaultSocketBacklog, backlog))
    val timeoutMillis = (timeoutSeconds * IngestPollTimeoutMillis).toLong
    socket.setSoTimeout(Math.max(IngestPollTimeoutMillis, timeoutMillis).min(Int.MaxValue).toInt)
    socket
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

  private def receivePartition(socket: Socket,
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
            var length = input.readInt()
            while (length != EndOfStream) { // scalastyle:ignore while
              output.writeInt(length)
              copyExactly(input, output, length)
              count += 1
              length = input.readInt()
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
      .map(params => Math.toIntExact(params.rowCount))
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
      socket.setKeepAlive(true)
      socket.setTcpNoDelay(true)
      socket.setSoTimeout(params.timeoutMillis)
      private val auth = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream))
      auth.writeUTF(params.token)
      auth.flush()
      private val input = new DataInputStream(new BufferedInputStream(socket.getInputStream))
      private val serializer = SparkEnv.get.serializer.newInstance()
      private var closed = false
      private var nextRow: Option[Row] = readNext()

      private def readNext(): Option[Row] = {
        try {
          val length = input.readInt()
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
}
