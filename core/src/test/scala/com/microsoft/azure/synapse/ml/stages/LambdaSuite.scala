// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.stages

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.types.StructType

class LambdaSuite extends TestBase with TransformerFuzzing[Lambda] {

  test("basic functionality") {
    val input = makeBasicDF()
    val lt = new Lambda()
      .setTransform(df => df.select("numbers"))
      .setTransformSchema(schema => new StructType(Array(schema("numbers"))))
    val output = lt.transform(input)
    val output2 = makeBasicDF().select("numbers")

  }

  test("without setting transform schema") {
    val input = makeBasicDF()
    val lt = Lambda(_.select("numbers"))
    val output = lt.transform(input)
    val output2 = makeBasicDF().select("numbers")
    assert(output === output2)
  }

  def testObjects(): Seq[TestObject[Lambda]] = List(new TestObject(Lambda(_.select("numbers")), makeBasicDF()))

  override def reader: MLReadable[_] = Lambda

}
