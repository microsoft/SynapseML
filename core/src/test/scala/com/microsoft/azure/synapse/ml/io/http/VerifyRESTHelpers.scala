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
    val result = RESTHelpers.retry(List(100, 200), () => "ok")
    assert(result === "ok")
  }

  test("retry retries on failure and eventually succeeds") {
    var attempts = 0
    val result = RESTHelpers.retry(List(1, 1, 1), () => {
      attempts += 1
      if (attempts < 3) throw new RuntimeException("fail")
      "success"
    })
    assert(result === "success")
    assert(attempts === 3)
  }

  test("retry throws when all retries exhausted") {
    intercept[RuntimeException] {
      RESTHelpers.retry(List(1), () => throw new RuntimeException("always fails"))
    }
  }

  test("retry with empty backoff list throws immediately") {
    intercept[RuntimeException] {
      RESTHelpers.retry(List.empty[Int], () => throw new RuntimeException("immediate"))
    }
  }
}
// scalastyle:on magic.number
