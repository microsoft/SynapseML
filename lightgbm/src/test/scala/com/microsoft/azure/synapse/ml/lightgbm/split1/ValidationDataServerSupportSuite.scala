// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split1

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.lightgbm.{NetworkManagerSocketSupport, ValidationDataSpool}
import org.apache.commons.io.FileUtils

import java.io.{File, IOException}
import java.nio.file.Files
import java.util.concurrent.{Callable, FutureTask}

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

  private def withSpoolDirectory(test: File => Unit): Unit = {
    val spool = Files.createTempDirectory("synapseml-validation-spool-test").toFile
    try test(spool)
    finally FileUtils.deleteDirectory(spool)
  }
}
