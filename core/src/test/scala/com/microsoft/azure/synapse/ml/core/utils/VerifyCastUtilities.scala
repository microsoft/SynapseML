// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.utils

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.sql.types._
import CastUtilities._

class VerifyCastUtilities extends TestBase {

  test("cast Int to DoubleType") {
    val result = 42.toDataType(DoubleType)
    assert(result === 42.0)
  }

  test("cast String to IntegerType") {
    val result = "123".toDataType(IntegerType)
    assert(result === 123)
  }

  test("cast Int to StringType") {
    val result = 456.toDataType(StringType)
    assert(result.toString === "456")
  }

  test("cast Double to IntegerType truncates") {
    val result = 9.7.toDataType(IntegerType)
    assert(result === 9)
  }

  test("cast Long to DoubleType") {
    val result = 100L.toDataType(DoubleType)
    assert(result === 100.0)
  }

  test("cast null to StringType") {
    // scalastyle:off null
    val result = null.asInstanceOf[Any].toDataType(StringType)
    assert(result === null)
    // scalastyle:on null
  }
}
