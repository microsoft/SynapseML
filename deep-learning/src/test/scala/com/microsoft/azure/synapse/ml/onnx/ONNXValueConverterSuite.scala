// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.onnx

import ai.onnxruntime.{OnnxValue, ValueInfo}
import org.scalatest.funsuite.AnyFunSuite

class ONNXValueConverterSuite extends AnyFunSuite {

  test("ONNX sequence conversion recursively materializes and closes nested values") {
    val leaf = new StubOnnxValue(java.lang.Long.valueOf(42L))
    val nestedValues = new java.util.ArrayList[OnnxValue]()
    nestedValues.add(leaf)
    val nestedSequence = new StubOnnxValue(nestedValues)
    val outerValues = new java.util.ArrayList[OnnxValue]()
    outerValues.add(nestedSequence)
    val outerSequence = new StubOnnxValue(outerValues)

    val converted = ONNXValueConverter.mapSequenceToArray(outerSequence)

    assert(converted == Vector(Vector(42L)))
    assert(nestedSequence.closed)
    assert(leaf.closed)
    assert(!outerSequence.closed)
  }

  private object StubValueInfo extends ValueInfo

  private class StubOnnxValue(rawValue: AnyRef) extends OnnxValue {
    var closed: Boolean = false

    override def getValue: AnyRef = rawValue

    override def getInfo: ValueInfo = StubValueInfo

    override def getType: OnnxValue.OnnxValueType = OnnxValue.OnnxValueType.ONNX_TYPE_SEQUENCE

    override def close(): Unit = {
      closed = true
    }
  }
}
