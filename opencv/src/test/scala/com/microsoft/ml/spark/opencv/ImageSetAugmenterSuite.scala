// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.opencv

import com.microsoft.ml.spark.build.BuildInfo
import com.microsoft.ml.spark.core.test.base.LinuxOnly
import com.microsoft.ml.spark.core.test.fuzzing.{TestObject, TransformerFuzzing}
import com.microsoft.ml.spark.io.IOImplicits._
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame

class ImageSetAugmenterSuite extends LinuxOnly with TransformerFuzzing[ImageSetAugmenter] {

  val groceriesDirectory = "/Images/CIFAR"
  private val fileLocation = s"${BuildInfo.datasetDir.toString}/$groceriesDirectory"

  private lazy val images: DataFrame = spark.read.image.load(fileLocation)

  override val sortInDataframeEquality: Boolean = true

  private val ia: ImageSetAugmenter = new ImageSetAugmenter()
    .setInputCol("image")
    .setOutputCol("augmented")
    .setFlipLeftRight(true)
    .setFlipUpDown(true)

  test("An augmenter should be abe to flip images") {

    // first image of the dataframe
    val original = images.take(1)(0).getStruct(0)

    val augmented = ia.transform(images)

    assert(augmented.count() === 3 * images.count())

  }

  override def testObjects(): Seq[TestObject[ImageSetAugmenter]] = Seq(new TestObject(ia,images))

  override def reader: MLReadable[_] = ImageSetAugmenter

}
