// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.util.MLReadable

class SuperpixelTransformerSuite extends TransformerFuzzing[SuperpixelTransformer]
  with NetworkUtils with FileReaderUtils {

  test("basic functionality"){
    val spt = new SuperpixelTransformer().setInputCol(inputCol)
    spt.transform(images).show()
  }

  override def testObjects(): Seq[TestObject[SuperpixelTransformer]] = Seq(new TestObject[SuperpixelTransformer](
    new SuperpixelTransformer().setInputCol(inputCol), images
  ))

  override def reader: MLReadable[_] = SuperpixelTransformer
}
