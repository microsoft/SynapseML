// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.image

import java.io.File
import java.net.URL

import com.microsoft.ml.spark.core.env.FileUtilities
import com.microsoft.ml.spark.core.test.fuzzing.{TestObject, TransformerFuzzing}
import com.microsoft.ml.spark.opencv.{ImageTestUtils, ImageTransformer}
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.{DataFrame, Row}
import com.microsoft.ml.spark.io.IOImplicits._
import org.apache.commons.io.FileUtils

class ResizeImageTransformerSuite extends TransformerFuzzing[ResizeImageTransformer]
  with ImageTestUtils {

  lazy val images: DataFrame = spark.read.image
    .option("dropInvalid", true).load(FileUtilities.join(fileLocation, "**").toString)

  lazy val greyscaleImageLocation: String = {
    val loc = "/tmp/greyscale.jpg"
    val f = new File(loc)
    if (f.exists()) {
      f.delete()
    }
    FileUtils.copyURLToFile(new URL("https://mmlspark.blob.core.windows.net/datasets/LIME/greyscale.jpg"), f)
    loc
  }

  lazy val greyImages: DataFrame = spark.read.image
    .option("dropInvalid", true).load(greyscaleImageLocation)

  lazy val tr: ResizeImageTransformer = new ResizeImageTransformer()
    .setOutputCol("out")
    .setHeight(15)
    .setWidth(10)

  test("general workflow") {
    assert(images.count() == 30)
    val preprocessed = tr.transform(images)

    preprocessed
      .select(preprocessed("out.height"), preprocessed("out.width"))
      .collect
      .foreach { row: Row =>
        assert(row.getInt(0) == 15 && row.getInt(1) == 10, "output images have incorrect size")
      }

    val unroll = new UnrollImage()
      .setInputCol(tr.getOutputCol)
      .setOutputCol("final")

    val result = unroll.transform(preprocessed).select("final")
    result.collect().foreach { row: Row =>
      assert(row(0).asInstanceOf[DenseVector].toArray.length == 10 * 15 * 3, "unrolled image is incorrect")
    }
  }

  test("general workflow grey") {
    assert(greyImages.count() == 1)
    val preprocessed = tr.transform(greyImages)

    preprocessed
      .select(preprocessed("out.height"), preprocessed("out.width"))
      .collect
      .foreach { row: Row =>
        assert(row.getInt(0) == 15 && row.getInt(1) == 10, "output images have incorrect size")
      }

    val unroll = new UnrollImage()
      .setInputCol(tr.getOutputCol)
      .setOutputCol("final")

    unroll.transform(preprocessed).select("final").collect().foreach { row: Row =>
      assert(row(0).asInstanceOf[DenseVector].toArray.length == 10 * 15 * 1, "unrolled image is incorrect")
    }
  }

  test("Values match the old resize values") {
    val preprocessed = tr.transform(images)
    val unroll = new UnrollImage()
      .setInputCol(tr.getOutputCol)
      .setOutputCol("final")

    val unrolledScala = unroll.transform(preprocessed).select("final").collect
    val tr2 = new ImageTransformer()
      .setOutputCol("out")
      .resize(height = 15, width = 10)

    val preprocessed2 = tr.transform(images)
    val unroll2 = new UnrollImage()
      .setInputCol(tr2.getOutputCol)
      .setOutputCol("final")

    val unrolledOpenCV = unroll2.transform(preprocessed2).select("final").collect

    (unrolledScala, unrolledOpenCV).zipped.foreach{ (rowScala, rowOpenCV) =>
      val actual = rowScala(0).asInstanceOf[DenseVector].toArray
      val expected = rowOpenCV(0).asInstanceOf[DenseVector].toArray
      assert(actual === expected)
    }
  }

  override def reader: MLReadable[_] = ResizeImageTransformer

  override def testObjects(): Seq[TestObject[ResizeImageTransformer]] =
    Seq(new TestObject(tr, images))
}
