// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.param

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.util.Identifiable
import spray.json.DefaultJsonProtocol._

import scala.collection.JavaConverters._

class VerifyTypedArrayParam extends TestBase {

  private object TestParams extends Params {
    override val uid: String = Identifiable.randomUID("TestParams") // scalastyle:ignore field.name
    override def copy(extra: ParamMap): Params = this
  }

  test("TypedIntArrayParam jsonEncode/jsonDecode roundtrip") {
    val param = new TypedIntArrayParam(TestParams, "intArray", "test int array param")
    val input = Seq(1, 2, 3)
    val encoded = param.jsonEncode(input)
    val decoded = param.jsonDecode(encoded)
    assert(decoded === input)
  }

  test("TypedDoubleArrayParam jsonEncode/jsonDecode roundtrip") {
    val param = new TypedDoubleArrayParam(TestParams, "doubleArray", "test double array param")
    val input = Seq(1.1, 2.2, 3.3)
    val encoded = param.jsonEncode(input)
    val decoded = param.jsonDecode(encoded)
    assert(decoded === input)
  }

  test("TypedIntArrayParam with empty Seq") {
    val param = new TypedIntArrayParam(TestParams, "emptyIntArray", "test empty int array param")
    val input = Seq.empty[Int]
    val encoded = param.jsonEncode(input)
    val decoded = param.jsonDecode(encoded)
    assert(decoded === input)
  }

  test("TypedIntArrayParam w() with Java ArrayList") {
    val param = new TypedIntArrayParam(TestParams, "intArrayW", "test int array w()")
    val javaList = new java.util.ArrayList[Int]()
    javaList.add(10)
    javaList.add(20)
    val paramPair = param.w(javaList)
    assert(paramPair.param === param)
    assert(paramPair.value === Seq(10, 20))
  }

  test("TypedDoubleArrayParam w() with Java ArrayList") {
    val param = new TypedDoubleArrayParam(TestParams, "doubleArrayW", "test double array w()")
    val javaList = new java.util.ArrayList[Double]()
    javaList.add(1.5)
    javaList.add(2.5)
    val paramPair = param.w(javaList)
    assert(paramPair.param === param)
    assert(paramPair.value === Seq(1.5, 2.5))
  }

}
