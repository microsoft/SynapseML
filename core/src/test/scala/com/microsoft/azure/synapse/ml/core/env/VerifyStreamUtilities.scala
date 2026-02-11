// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.env

import com.microsoft.azure.synapse.ml.core.test.base.TestBase

import scala.io.Source
import scala.util.{Failure, Success}

class TrackableCloseable extends AutoCloseable {
  var closed = false
  override def close(): Unit = { closed = true }
}

class VerifyStreamUtilities extends TestBase {

  test("using returns Success with result when task succeeds") {
    val result = StreamUtilities.using(new TrackableCloseable) { _ => 42 }
    assert(result.isSuccess)
    assert(result.get == 42)
  }

  test("using returns Failure when task throws and still closes resource") {
    val resource = new TrackableCloseable
    val result = StreamUtilities.using(resource) { _ =>
      throw new RuntimeException("test error")
    }
    assert(result.isFailure)
    assert(result.failed.get.getMessage == "test error")
    assert(resource.closed)
  }

  test("using closes the resource even on success") {
    val resource = new TrackableCloseable
    StreamUtilities.using(resource) { _ => "ok" }
    assert(resource.closed)
  }

  test("usingMany closes all resources") {
    val resources = Seq(new TrackableCloseable, new TrackableCloseable, new TrackableCloseable)
    val result = StreamUtilities.usingMany(resources) { rs => rs.size }
    assert(result.isSuccess)
    assert(result.get == 3)
    resources.foreach(r => assert(r.closed))
  }

  test("usingSource works with scala.io.Source") {
    val result = StreamUtilities.usingSource(Source.fromString("hello world")) { src =>
      src.mkString
    }
    assert(result.isSuccess)
    assert(result.get == "hello world")
  }

}
