// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azureml

import com.microsoft.azureml.Readers.implicits._
import com.microsoft.azureml.schema.ImageSchema

class ImageAugmenterSuite extends LinuxOnly {

  val groceriesDirectory = "/Images/Grocery/"
  private val fileLocation = s"${sys.env("DATASETS_HOME")}/$groceriesDirectory"

  test("An augmenter should be abe to flip images") {
    val images = session.readImages(fileLocation, recursive = true)

    //first image of the dataframe
    val original = images.take(1)(0).getStruct(0)

    val ia = new ImageAugmenter()
      .setInputCol("image")
      .setOutputCol("augmented")
      .setFlipLeftRight(true)
      .setFlipUpDown(true)

    val augmented = ia.transform(images)

    assert(augmented.count() === 3*images.count())

  }

}
