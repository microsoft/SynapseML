// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import com.microsoft.ml.spark.Readers.implicits._
import com.microsoft.ml.spark.schema.ImageSchema
import org.apache.spark.image.ImageFileFormat

class ImageReaderSuite extends TestBase with FileReaderUtils {

  test("image dataframe") {
    val images = session.readImages(groceriesDirectory, recursive = true)
    println(time { images.count })
    assert(ImageSchema.isImage(images, "image")) // make sure the column "images" exists and has the right type
    val paths = images.select("image.path") //make sure that SQL has access to the sub-fields
    assert(paths.count == 30)
    val areas = images.select(images("image.width") * images("image.height")) //more complicated SQL statement
    println(s"   area of image 1 ${areas.take(1)(0)}")
  }

  test("read images with subsample"){
    val imageDF = session
      .read
      .format(classOf[ImageFileFormat].getName)
      .option("subsample", .5)
      .load(cifarDirectory)
    assert(imageDF.count() == 3)
  }

  test("structured streaming with images"){
    val schema = ImageSchema.schema
    val imageDF = session
      .readStream
      .format(classOf[ImageFileFormat].getName)
      .schema(schema)
      .load(cifarDirectory)

    val q1 = imageDF.select("image.path").writeStream
      .format("memory")
      .queryName("images")
      .start()

    tryWithRetries() {() =>
      val df = session.sql("select * from images")
      assert(df.count() == 6)
    }
    q1.stop()
  }

  test("with zip file") {
    /* remove when datasets/Images is updated */
    createZips()

    val images = session.readImages(imagesDirectory, recursive = true)
    assert(ImageSchema.isImage(images, "image"))
    assert(images.count == 72)

    val images1 = session.readImages(imagesDirectory, recursive = true, inspectZip = false)
    assert(images1.count == 36)
  }

  test("sample ratio test") {

    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    val f = sc.binaryFiles(groceriesDirectory)
    println(time { f.count })

    val images = session.readImages(groceriesDirectory, recursive = true, sampleRatio = 0.5)
    println(time { images.count })      //the count changes depending on random number generator
  }

}
