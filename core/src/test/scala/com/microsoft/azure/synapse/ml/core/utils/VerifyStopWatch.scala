// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.utils

import com.microsoft.azure.synapse.ml.core.test.base.TestBase

class VerifyStopWatch extends TestBase {

  test("elapsed is 0 before any measurement") {
    val sw = new StopWatch
    assert(sw.elapsed() === 0L)
  }

  test("start/pause records positive elapsed time") {
    val sw = new StopWatch
    sw.start()
    Thread.sleep(10)
    sw.pause()
    assert(sw.elapsed() > 0)
  }

  test("multiple start/pause cycles accumulate elapsed time") {
    val sw = new StopWatch
    sw.start()
    Thread.sleep(10)
    sw.pause()
    val first = sw.elapsed()
    assert(first > 0)

    sw.start()
    Thread.sleep(10)
    sw.pause()
    assert(sw.elapsed() > first)
  }

  test("restart resets elapsed to 0") {
    val sw = new StopWatch
    sw.start()
    Thread.sleep(10)
    sw.pause()
    assert(sw.elapsed() > 0)

    val beforeRestart = sw.elapsed()
    sw.restart()
    sw.pause()
    // After restart, elapsed should be near zero (just the tiny time between restart and pause)
    assert(sw.elapsed() < beforeRestart)
  }

  test("measure returns the function result and records elapsed time") {
    val sw = new StopWatch
    val result = sw.measure {
      Thread.sleep(10)
      42
    }
    assert(result === 42)
    assert(sw.elapsed() > 0)
  }

  test("measure accumulates with previous elapsed") {
    val sw = new StopWatch
    sw.start()
    Thread.sleep(10)
    sw.pause()
    val first = sw.elapsed()
    assert(first > 0)

    val result = sw.measure {
      Thread.sleep(10)
      "hello"
    }
    assert(result === "hello")
    assert(sw.elapsed() > first)
  }
}
