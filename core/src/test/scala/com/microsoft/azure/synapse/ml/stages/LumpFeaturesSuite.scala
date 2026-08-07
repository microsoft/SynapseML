// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.stages

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.spark.ml.util.MLReadable

class LumpFeaturesSuite extends TestBase with TransformerFuzzing[LumpFeatures]{
  import spark.implicits._

  val value = "value"
  val color = "color"
  val shape = "shape"
  val size = "size"

  lazy val input = Seq(
    (0, "Blue", "Rectangle", 2),
    (0, "Blue", "Pentagon", 3),
    (0, "Blue", "Hexagon", 7),
    (1, "Blue", "Circle", 6),
    (1, "Yellow", "Circle", 2),
    (1, "Yellow", "Square", 5),
    (2, "Yellow", "Square",1),
    (2, "Yellow", "Square",7),
    (2, "White", "Triangle",4),
    (3, "Gray", "Triangle",3),
    (3, "Black", "Triangle",9),
    (3, "Cerulean", "Triangle", 8)
  ).toDF(value, color, shape, size)

  lazy val expectedLumpedDF = Seq(
    (0, "0", "3", 2),
    (0, "0", "3", 3),
    (0, "0", "3", 7),
    (1, "0", "2", 6),
    (1, "1", "2", 2),
    (1, "1", "1", 5),
    (2, "1", "1", 1),
    (2, "1", "1", 7),
    (2, "2", "0", 4),
    (3, "2", "0", 3),
    (3, "2", "0", 9),
    (3, "2", "0", 8)
  ).toDF(value, color, shape, size)

  test("basic functionality to lumping categorical columns") {

    val lt = new LumpFeatures()
      .setLumpRules("{\"color\":2, \"shape\":3}")
    val lumpedDF = lt.transform(input)

    lumpedDF.show()
    assert(verifyResult(expectedLumpedDF, lumpedDF))
  }

  def testObjects(): Seq[TestObject[LumpFeatures]] = List(new TestObject(
    new LumpFeatures(), makeBasicDF()))

  override def reader: MLReadable[_] = DropColumns
}
