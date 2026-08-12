// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.param

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.ml.param.{ParamMap, Params}

class VerifyByteArrayParam extends TestBase {

  private class TestParamsHolder extends Params {
    override val uid: String = "test-holder"
    val bytesParam = new ByteArrayParam(this, "bytes", "A byte array param")
    override def copy(extra: ParamMap): Params = this
  }

  test("ByteArrayParam can be created with basic constructor") {
    val holder = new TestParamsHolder
    assert(holder.bytesParam.name === "bytes")
    assert(holder.bytesParam.doc === "A byte array param")
  }

  test("ByteArrayParam accepts empty byte array") {
    val holder = new TestParamsHolder
    holder.set(holder.bytesParam, Array.empty[Byte])
    assert(holder.get(holder.bytesParam).exists(_.isEmpty))
  }

  test("ByteArrayParam accepts byte array with data") {
    val holder = new TestParamsHolder
    val data = Array[Byte](1, 2, 3, 4, 5)
    holder.set(holder.bytesParam, data)
    assert(holder.get(holder.bytesParam).exists(_.sameElements(data)))
  }

  test("ByteArrayParam accepts large byte array") {
    val holder = new TestParamsHolder
    val data = Array.fill(1000)(42.toByte)
    holder.set(holder.bytesParam, data)
    assert(holder.get(holder.bytesParam).exists(_.length === 1000))
  }

  test("ByteArrayParam accepts byte array with all byte values") {
    val holder = new TestParamsHolder
    val data = (-128 to 127).map(_.toByte).toArray
    holder.set(holder.bytesParam, data)
    assert(holder.get(holder.bytesParam).exists(_.length === 256))
  }

  test("ByteArrayParam with custom validator") {
    val holder = new Params {
      override val uid: String = "test"
      val nonEmptyBytes = new ByteArrayParam(
        this, "nonEmpty", "Non-empty byte array",
        (arr: Array[Byte]) => arr.nonEmpty
      )
      override def copy(extra: ParamMap): Params = this
    }
    holder.set(holder.nonEmptyBytes, Array[Byte](1, 2, 3))
  }

  test("ByteArrayParam can be cleared") {
    val holder = new TestParamsHolder
    holder.set(holder.bytesParam, Array[Byte](1, 2, 3))
    assert(holder.isSet(holder.bytesParam))
    holder.clear(holder.bytesParam)
    assert(!holder.isSet(holder.bytesParam))
  }

  test("ByteArrayParam returns None when not set") {
    val holder = new TestParamsHolder
    assert(holder.get(holder.bytesParam).isEmpty)
  }
}
