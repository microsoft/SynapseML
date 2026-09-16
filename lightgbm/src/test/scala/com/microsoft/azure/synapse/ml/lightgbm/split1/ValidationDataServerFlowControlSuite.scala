// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split1

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.lightgbm.{ValidationDataServer, ValidationDataServerResourceFactory}
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkEnv
import org.apache.spark.sql.Row

import java.io.{DataOutputStream, File, FileOutputStream, FilterOutputStream, OutputStream}
import java.net.{ServerSocket, Socket}
import java.util.UUID
import java.util.concurrent.{Callable, CountDownLatch, ExecutorService, Executors, TimeUnit}

// scalastyle:off magic.number
class ValidationDataServerFlowControlSuite extends TestBase {
  private val host = "127.0.0.1"

  test("queued validation readers start their timeout only after transfer admission") {
    spark.sparkContext
    val expectedRows = 8
    val (spool, partitionFiles) = createSerializedSpool(expectedRows)
    val resources = new ValidationDataServerResourceFactory {
      override def openServerSocket(host: String, timeoutSeconds: Double, backlog: Int): ServerSocket = {
        ValidationDataServerResourceFactory.Default.openServerSocket(host, timeoutSeconds, backlog)
      }

      override def createExecutor(threadCount: Int, threadNamePrefix: String): ExecutorService = {
        ValidationDataServerResourceFactory.Default.createExecutor(threadCount, threadNamePrefix)
      }

      override def clientOutput(socket: Socket): OutputStream = {
        new FilterOutputStream(socket.getOutputStream) {
          override def write(value: Int): Unit = {
            Thread.sleep(40)
            out.write(value)
          }

          override def write(bytes: Array[Byte], offset: Int, length: Int): Unit = {
            Thread.sleep(40)
            out.write(bytes, offset, length)
          }
        }
      }
    }
    val server = ValidationDataServer.createFromSpool(
      spool, partitionFiles, expectedRows, host, 0.2, 9, resources)
    val clients = Executors.newFixedThreadPool(9)
    val ready = new CountDownLatch(9)
    val start = new CountDownLatch(1)
    try {
      val transfers = (0 until 9).map { _ =>
        clients.submit(new Callable[Int] {
          override def call(): Int = {
            ready.countDown()
            start.await()
            val rows = ValidationDataServer.read(server.params, SparkEnv.get.serializer.newInstance())
            try rows.foldLeft(0)((count, _) => count + 1)
            finally rows.close()
          }
        })
      }
      assert(ready.await(5, TimeUnit.SECONDS))
      start.countDown()
      assert(transfers.forall(_.get(10, TimeUnit.SECONDS) == expectedRows))
      server.close()
    } finally {
      clients.shutdownNow()
      clients.awaitTermination(5, TimeUnit.SECONDS)
      try server.close()
      finally deleteIfPresent(spool)
    }
    assert(!spool.exists())
  }

  private def createSerializedSpool(rowCount: Int): (File, Array[File]) = {
    val spool = new File(
      System.getProperty("user.dir"),
      s".synapseml-validation-queued-readers-${UUID.randomUUID()}")
    assert(spool.mkdir())
    val partitionFile = new File(spool, "part-0")
    val serializer = SparkEnv.get.serializer.newInstance()
    val rowBuffer = serializer.serialize(Row(Array.fill[Byte](64 * 1024)(1)))
    val rowBytes = new Array[Byte](rowBuffer.remaining())
    rowBuffer.get(rowBytes)
    val output = new DataOutputStream(new FileOutputStream(partitionFile))
    try {
      (0 until rowCount).foreach { _ =>
        output.writeInt(rowBytes.length)
        output.write(rowBytes)
      }
    } finally {
      output.close()
    }
    (spool, Array(partitionFile))
  }

  private def deleteIfPresent(directory: File): Unit = {
    if (directory.exists()) FileUtils.deleteDirectory(directory)
  }
}
