// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split1

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.lightgbm.{NetworkManagerSocketSupport, ValidationDataIngest}
import com.microsoft.azure.synapse.ml.lightgbm.{ValidationDataServer, ValidationDataSpool, ValidationPartitionAttempt}
import org.apache.commons.io.FileUtils

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File, IOException}
import java.net.{Socket, SocketException, SocketTimeoutException}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{Callable, FutureTask}
import scala.collection.mutable.ArrayBuffer

class ValidationDataServerSupportSuite extends TestBase {

  test("waiting for ingest exposes the accept loop's original failure") {
    val expected = new IOException("synthetic ingest accept failure")
    val failed = new FutureTask[Unit](new Callable[Unit] {
      override def call(): Unit = throw expected
    })
    failed.run()

    val failure = intercept[IOException] {
      NetworkManagerSocketSupport.awaitFuture(failed)
    }
    assert(failure eq expected)
  }

  test("waiting for ingest preserves caller interruption") {
    val waiting = new FutureTask[Unit](new Callable[Unit] {
      override def call(): Unit = ()
    })
    Thread.currentThread().interrupt()
    try {
      intercept[InterruptedException] {
        NetworkManagerSocketSupport.awaitFuture(waiting)
      }
      assert(Thread.currentThread().isInterrupted)
    } finally {
      Thread.interrupted()
    }
  }

  test("recording asynchronous failures preserves the first failure") {
    val first = new IOException("first failure")
    val second = new IOException("second failure")
    val recorded = new AtomicReference[Throwable]()

    NetworkManagerSocketSupport.recordFailure(recorded, first)
    NetworkManagerSocketSupport.recordFailure(recorded, second)

    assert(recorded.get() eq first)
    assert(first.getSuppressed.sameElements(Array(second)))
  }

  test("socket write watchdog closes a transfer that stops making progress") {
    val closed = new CountDownLatch(1)
    val socket = new Socket {
      @volatile private var closedState = false

      override def close(): Unit = {
        closedState = true
        closed.countDown()
      }

      override def isClosed: Boolean = closedState
    }

    val failure = intercept[SocketTimeoutException] {
      NetworkManagerSocketSupport.withSocketWriteTimeout(socket, 50) { _ =>
        assert(closed.await(5, java.util.concurrent.TimeUnit.SECONDS))
        throw new SocketException("synthetic blocked write interrupted by close")
      }
    }
    assert(failure.getMessage.contains("without validation socket write progress"))
    assert(failure.getSuppressed.exists(_.getMessage.contains("synthetic blocked write")))
  }

  test("validation row copies reuse the current thread's transfer buffer") {
    val rows = Seq("first validation row", "second validation row")
    val bytes = rows.mkString.getBytes(StandardCharsets.UTF_8)
    val observedBuffers = ArrayBuffer.empty[Array[Byte]]
    val input = new ByteArrayInputStream(bytes) {
      override def read(buffer: Array[Byte], offset: Int, length: Int): Int = {
        observedBuffers += buffer
        super.read(buffer, offset, length)
      }
    }
    val output = new ByteArrayOutputStream()

    rows.foreach { row =>
      ValidationDataServer.copyExactly(
        input,
        output,
        row.getBytes(StandardCharsets.UTF_8).length)
    }

    assert(output.toByteArray.sameElements(bytes))
    assert(observedBuffers.nonEmpty)
    assert(observedBuffers.forall(_ eq observedBuffers.head))
  }

  test("validation spool listing fails when the directory cannot be read") {
    val unreadable = new File("unreadable-validation-spool") {
      override def listFiles(): Array[File] = null // scalastyle:ignore null
    }

    val failure = intercept[IOException] {
      ValidationDataSpool.listPartitionFiles(unreadable, 1)
    }
    assert(failure.getMessage.contains("Could not list validation spool directory"))
  }

  test("validation spool listing requires every expected partition") {
    withSpoolDirectory { spool =>
      Files.createFile(new File(spool, "part-0").toPath)

      val failure = intercept[IOException] {
        ValidationDataSpool.listPartitionFiles(spool, 2)
      }
      assert(failure.getMessage.contains("Expected 2 validation partition files but found 1"))
    }
  }

  test("validation spool listing returns canonical partition files in numeric order") {
    withSpoolDirectory { spool =>
      Files.createFile(new File(spool, "part-1").toPath)
      Files.createFile(new File(spool, ".attempt-ignored").toPath)
      Files.createFile(new File(spool, "part-0").toPath)

      assert(ValidationDataSpool.listPartitionFiles(spool, 2).map(_.getName)
        .sameElements(Array("part-0", "part-1")))
    }
  }

  test("successful attempt promotion rejects non-contiguous partition ids") {
    withSpoolDirectory { spool =>
      val failure = intercept[IOException] {
        ValidationDataIngest.promoteSuccessfulAttempts(
          spool,
          Array(ValidationPartitionAttempt(partitionId = 1, attemptId = 0L, rowCount = 0L)))
      }
      assert(failure.getMessage.contains("invalid=1"))
      assert(failure.getMessage.contains("missing=0"))
    }
  }

  private def withSpoolDirectory(test: File => Unit): Unit = {
    val spool = Files.createTempDirectory("synapseml-validation-spool-test").toFile
    try test(spool)
    finally FileUtils.deleteDirectory(spool)
  }
}
