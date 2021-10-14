// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.lime

import com.microsoft.ml.spark.core.test.fuzzing.{TestObject, TransformerFuzzing}
import com.microsoft.ml.spark.image.ImageTestUtils
import com.microsoft.ml.spark.io.split1.FileReaderUtils
import org.apache.spark.ml.util.MLReadable

class SuperpixelTransformerSuite extends TransformerFuzzing[SuperpixelTransformer]
  with ImageTestUtils with FileReaderUtils {
  lazy val spt: SuperpixelTransformer = new SuperpixelTransformer().setInputCol(inputCol)

  test("transform images"){
    val results = spt.transform(images)
    val superpixels = SuperpixelData.fromRow(results.collect()(0).getStruct(1))
    assert(superpixels.clusters.length === 9)
    assert(superpixels.clusters.head.length === 44)
  }

  test("transform binaries"){
    val results = spt.transform(binaryImages)
    val superpixels = SuperpixelData.fromRow(results.collect()(0).getStruct(1))
    assert(superpixels.clusters.length === 9)
    assert(superpixels.clusters.head.length === 44)
  }

  override def testObjects(): Seq[TestObject[SuperpixelTransformer]] =
    Seq(new TestObject(spt, images))

  override def reader: MLReadable[_] = SuperpixelTransformer
}
