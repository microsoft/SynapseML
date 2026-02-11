// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.utils

import com.microsoft.azure.synapse.ml.core.test.base.TestBase

import scala.concurrent.duration._

class VerifyFaultToleranceUtils extends TestBase {

  test("retryWithTimeout backoff version succeeds on first try") {
    val result = FaultToleranceUtils.retryWithTimeout(Seq(0, 0)) {
      "success"
    }
    assert(result === "success")
  }

  test("retryWithTimeout backoff version retries and succeeds on third try") {
    var counter = 0
    val result = FaultToleranceUtils.retryWithTimeout(Seq(0, 0, 0)) {
      counter += 1
      if (counter < 3) throw new RuntimeException(s"Attempt $counter failed")
      "success"
    }
    assert(result === "success")
    assert(counter === 3)
  }

  test("retryWithTimeout backoff version throws after exhausting all retries") {
    var counter = 0
    assertThrows[RuntimeException] {
      FaultToleranceUtils.retryWithTimeout(Seq(0, 0)) {
        counter += 1
        throw new RuntimeException(s"Attempt $counter failed")
      }
    }
    assert(counter === 3) // initial call + 2 retries
  }

  test("retryWithTimeout backoff version with empty Seq throws immediately") {
    var counter = 0
    assertThrows[RuntimeException] {
      FaultToleranceUtils.retryWithTimeout(Seq.empty[Int]) {
        counter += 1
        throw new RuntimeException("fail")
      }
    }
    assert(counter === 1)
  }

  test("retryWithTimeout timed version succeeds on first try") {
    val result = FaultToleranceUtils.retryWithTimeout(3, 10.seconds) {
      "success"
    }
    assert(result === "success")
  }

  test("retryWithTimeout timed version retries on failure and eventually succeeds") {
    var counter = 0
    val result = FaultToleranceUtils.retryWithTimeout(3, 10.seconds) {
      counter += 1
      if (counter < 3) throw new RuntimeException(s"Attempt $counter failed")
      "success"
    }
    assert(result === "success")
    assert(counter === 3)
  }
}
