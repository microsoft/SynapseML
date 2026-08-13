// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.io.http

import com.microsoft.azure.synapse.ml.core.test.base.TestBase

// scalastyle:off magic.number
class VerifyRESTHelpers extends TestBase {

  test("retry succeeds on first try with empty backoffs") {
    val result = RESTHelpers.retry(List.empty[Int], () => 42)
    assert(result === 42)
  }

  test("retry succeeds on first try with non-empty backoffs") {
    val result = RESTHelpers.retry(List(0, 0), () => "ok")
    assert(result === "ok")
  }

  test("retry retries on failure and eventually succeeds") {
    var attempts = 0
    val result = Console.withOut(new java.io.ByteArrayOutputStream()) {
      // Zero backoffs exercise the same retry path without a real Thread.sleep.
      RESTHelpers.retry(List(0, 0, 0), () => {
        attempts += 1
        if (attempts < 3) throw new RuntimeException("fail")
        "success"
      })
    }
    assert(result === "success")
    assert(attempts === 3)
  }

  test("retry throws when all retries exhausted") {
    Console.withOut(new java.io.ByteArrayOutputStream()) {
      intercept[RuntimeException] {
        RESTHelpers.retry(List(0), () => throw new RuntimeException("always fails"))
      }
    }
  }

  test("retry with empty backoff list throws immediately") {
    intercept[RuntimeException] {
      RESTHelpers.retry(List.empty[Int], () => throw new RuntimeException("immediate"))
    }
  }
}
// scalastyle:on magic.number
