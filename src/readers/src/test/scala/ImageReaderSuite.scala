// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.sql._
import com.microsoft.ml.spark.schema.ImageSchema.isImage
import com.microsoft.ml.spark.schema.BinaryFileSchema.isBinaryFile
import org.apache.spark.input.PortableDataStream
import com.microsoft.ml.spark.Readers.implicits._
import com.microsoft.ml.spark.FileUtilities._

object FileReaderSuiteUtils {
  val fileLocation = s"${sys.env("DATASETS_HOME")}"
  val imagesDirectory = fileLocation + "/Images"
  val groceriesDirectory = imagesDirectory + "/Grocery"
  val cifarDirectory = imagesDirectory + "/CIFAR"

  def createZip(directory: String): Unit ={
      val dir = new File(directory)
      val zipfile = new File(directory + ".zip")
      if(!zipfile.exists())
        zipFolder(dir, zipfile)
  }

  def creatZips(): Unit ={
    createZip(groceriesDirectory)
    createZip(cifarDirectory)
  }
}

import FileReaderSuiteUtils._

class ImageReaderSuite extends TestBase {

  test("image dataframe") {

    val images = session.readImages(groceriesDirectory, recursive = true)

    println(time { images.count })

    assert(isImage(images, "image")) // make sure the column "images" exists and has the right type

    val paths = images.select("image.path") //make sure that SQL has access to the sub-fields
    assert(paths.count == 30)

    val areas = images.select(images("image.width") * images("image.height")) //more complicated SQL statement

    println(s"   area of image 1 ${areas.take(1)(0)}")
  }

  test("with zip file") {
    /* remove when datasets/Images is updated */
    creatZips

    val images = session.readImages(imagesDirectory, recursive = true)
    assert(isImage(images, "image"))
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
