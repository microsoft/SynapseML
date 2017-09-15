// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import com.microsoft.ml.spark.FileReaderSuiteUtils._
import com.microsoft.ml.spark.Readers.implicits._
import com.microsoft.ml.spark.schema.BinaryFileSchema.isBinaryFile

class BinaryFileReaderSuite extends TestBase {

  test("binary dataframe") {

    val data = session.readBinaryFiles(groceriesDirectory, recursive = true)

    println(time { data.count })

    assert(isBinaryFile(data, "value"))

    val paths = data.select("value.path") //make sure that SQL has access to the sub-fields
    assert(paths.count == 31)             //note that text file is also included
  }

  test("sample ratio test") {

    val all = session.readBinaryFiles(groceriesDirectory, recursive = true, sampleRatio = 1.0)
    val sampled = session.readBinaryFiles(groceriesDirectory, recursive = true, sampleRatio = 0.5)
    val count = sampled.count
    assert(count > 0 && count < all.count, "incorrect sampling behavior")
  }

  test("with zip file") {
    /* remove when datasets/Images is updated */
    createZips

    val images = session.readBinaryFiles(imagesDirectory, recursive = true)
    assert(images.count == 74)

    val images1 = session.readBinaryFiles(imagesDirectory, recursive = true, inspectZip = false)
    assert(images1.count == 39)
  }

}
