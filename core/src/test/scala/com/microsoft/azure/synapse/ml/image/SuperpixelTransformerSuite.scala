// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.image

import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import com.microsoft.azure.synapse.ml.io.split1.FileReaderUtils
import org.apache.spark.ml.util.MLReadable

class SuperpixelTransformerSuite extends TransformerFuzzing[SuperpixelTransformer]
  with ImageTestUtils with FileReaderUtils {
  lazy val spt: SuperpixelTransformer = new SuperpixelTransformer().setInputCol(inputCol)

  test("basic functionality"){
    val results = spt.transform(images)
    val superpixels = SuperpixelData.fromRow(results.collect()(0).getStruct(1))
    assert(superpixels.clusters.length === 3)
    assert(superpixels.clusters.head.length == 310)
  }

  override def testObjects(): Seq[TestObject[SuperpixelTransformer]] =
    Seq(new TestObject(spt, images))

  override def reader: MLReadable[_] = SuperpixelTransformer
}
