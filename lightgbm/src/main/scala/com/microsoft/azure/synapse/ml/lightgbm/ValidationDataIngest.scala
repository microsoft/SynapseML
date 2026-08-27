// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.sql.Row

import java.io.{BufferedInputStream, BufferedOutputStream, DataInputStream, DataOutputStream, File, IOException}
import java.net.Socket
import java.nio.ByteBuffer
import java.nio.file.{Files, StandardOpenOption}

private[lightgbm] case class ValidationPartitionAttempt(partitionId: Int,
                                                        attemptId: Long,
                                                        rowCount: Long)

private[lightgbm] object ValidationDataIngest {
  private val AckMarker = 0x53594e41
  private val AttemptFilePrefix = ".attempt-"
  private val TransferChunkSize = 64 * 1024 // scalastyle:ignore magic.number

  def writePartition(host: String,
                     port: Int,
                     token: String,
                     timeoutMillis: Int,
                     partitionId: Int,
                     rows: Iterator[Row]): ValidationPartitionAttempt = {
    val taskContext = TaskContext.get()
    if (taskContext == null) { // scalastyle:ignore null
      throw new IllegalStateException("Validation partition transfer requires an active Spark task")
    }
    val attemptId = taskContext.taskAttemptId()
    withSocket(ValidationDataServer.connect(host, port, timeoutMillis)) { socket =>
      socket.setKeepAlive(true)
      socket.setTcpNoDelay(true)
      socket.setSoTimeout(timeoutMillis)
      val output = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream))
      val serializer = SparkEnv.get.serializer.newInstance()
      val rowCount = NetworkManagerSocketSupport.withSocketWriteTimeout(socket, timeoutMillis) { reportProgress =>
        output.writeUTF(token)
        output.writeInt(partitionId)
        output.writeLong(attemptId)
        reportProgress()
        var count = 0L
        rows.foreach { row =>
          val bytes = toBytes(serializer.serialize(row))
          output.writeInt(bytes.length)
          writeBytes(output, bytes, reportProgress)
          count = Math.addExact(count, 1L)
        }
        output.writeInt(ValidationDataServer.EndOfStream)
        output.flush()
        reportProgress()
        count
      }

      val acknowledgement = new DataInputStream(new BufferedInputStream(socket.getInputStream))
      val marker = acknowledgement.readInt()
      val acknowledgedAttempt = acknowledgement.readLong()
      val acknowledgedRows = acknowledgement.readLong()
      if (marker != AckMarker || acknowledgedAttempt != attemptId || acknowledgedRows != rowCount) {
        throw new IOException(
          s"Invalid validation partition acknowledgement for partition $partitionId attempt $attemptId")
      }
      ValidationPartitionAttempt(partitionId, attemptId, rowCount)
    }
  }

  def receivePartition(socket: Socket,
                       spoolDirectory: File,
                       token: String,
                       timeoutMillis: Int): ValidationPartitionAttempt = {
    withSocket(socket) { client =>
      client.setKeepAlive(true)
      client.setTcpNoDelay(true)
      client.setSoTimeout(timeoutMillis)
      val input = new DataInputStream(new BufferedInputStream(client.getInputStream))
      if (input.readUTF() != token) throw new SecurityException("Invalid validation partition token")
      val partitionId = input.readInt()
      if (partitionId < 0) {
        throw new IOException(s"Invalid validation partition id $partitionId; expected a non-negative value")
      }
      val attemptId = input.readLong()
      if (attemptId < 0L) {
        throw new IOException(s"Invalid validation task attempt id $attemptId")
      }

      val attemptFile = fileForAttempt(spoolDirectory, partitionId, attemptId)
      var retained = false
      NetworkManagerSocketSupport.withCleanupPreservingPrimary(
        if (!retained) {
          Files.deleteIfExists(attemptFile.toPath)
          ()
        }) {
        val fileOutput = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(
          attemptFile.toPath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)))
        var rowCount = 0L
        NetworkManagerSocketSupport.withCleanupPreservingPrimary(fileOutput.close()) {
          var length = ValidationDataServer.readRowLength(input, "validation partition")
          while (length != ValidationDataServer.EndOfStream) { // scalastyle:ignore while
            fileOutput.writeInt(length)
            ValidationDataServer.copyExactly(input, fileOutput, length)
            rowCount += 1
            length = ValidationDataServer.readRowLength(input, "validation partition")
          }
        }
        retained = true
        NetworkManagerSocketSupport.withSocketWriteTimeout(client, timeoutMillis) { reportProgress =>
          val acknowledgement = new DataOutputStream(new BufferedOutputStream(client.getOutputStream))
          acknowledgement.writeInt(AckMarker)
          acknowledgement.writeLong(attemptId)
          acknowledgement.writeLong(rowCount)
          acknowledgement.flush()
          reportProgress()
        }
        ValidationPartitionAttempt(partitionId, attemptId, rowCount)
      }
    }
  }

  def promoteSuccessfulAttempts(spoolDirectory: File,
                                attempts: Array[ValidationPartitionAttempt]): Long = {
    val partitionCount = attempts.length
    val attemptsByPartition = attempts.groupBy(_.partitionId)
    val invalidPartitions = attemptsByPartition.keys.filter(id => id < 0 || id >= partitionCount).toSeq.sorted
    val missingPartitions = (0 until partitionCount).filterNot(attemptsByPartition.contains)
    val duplicatePartitions = attemptsByPartition.collect {
      case (partitionId, partitionAttempts) if partitionAttempts.length != 1 => partitionId
    }.toSeq.sorted
    if (attempts.length != partitionCount ||
      invalidPartitions.nonEmpty || missingPartitions.nonEmpty || duplicatePartitions.nonEmpty) {
      throw new IOException(
        s"Invalid successful validation attempts: expected $partitionCount, found ${attempts.length}; " +
          s"invalid=${invalidPartitions.mkString(",")}, missing=${missingPartitions.mkString(",")}, " +
          s"duplicate=${duplicatePartitions.mkString(",")}")
    }

    var totalRows = 0L
    attempts.sortBy(_.partitionId).foreach { attempt =>
      Files.move(
        fileForAttempt(spoolDirectory, attempt.partitionId, attempt.attemptId).toPath,
        new File(spoolDirectory, s"part-${attempt.partitionId}").toPath)
      totalRows = Math.addExact(totalRows, attempt.rowCount)
    }
    Option(spoolDirectory.listFiles()).getOrElse(Array.empty)
      .filter(_.getName.startsWith(AttemptFilePrefix))
      .foreach(file => Files.deleteIfExists(file.toPath))
    totalRows
  }

  private def fileForAttempt(spoolDirectory: File, partitionId: Int, attemptId: Long): File = {
    new File(spoolDirectory, s"$AttemptFilePrefix$partitionId-$attemptId")
  }

  private def withSocket[T](socket: Socket)(operation: Socket => T): T = {
    NetworkManagerSocketSupport.withCleanupPreservingPrimary(ValidationDataServer.closeSocket(socket)) {
      operation(socket)
    }
  }

  private def toBytes(buffer: ByteBuffer): Array[Byte] = {
    val bytes = new Array[Byte](buffer.remaining())
    buffer.get(bytes)
    bytes
  }

  private def writeBytes(output: DataOutputStream,
                         bytes: Array[Byte],
                         reportProgress: () => Unit): Unit = {
    var offset = 0
    while (offset < bytes.length) { // scalastyle:ignore while
      val count = Math.min(TransferChunkSize, bytes.length - offset)
      output.write(bytes, offset, count)
      reportProgress()
      offset += count
    }
  }
}
