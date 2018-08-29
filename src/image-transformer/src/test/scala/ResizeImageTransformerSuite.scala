// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import com.microsoft.ml.spark.Readers.implicits._
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col

class ResizeImageTransformerSuite extends TransformerFuzzing[ResizeImageTransformer]
  with ImageTestUtils {

  test("general workflow") {
    val images = session.readImages(fileLocation, recursive = true)
    assert(images.count() == 30)

    val tr = new ResizeImageTransformer()
      .setOutputCol("out")
      .setHeight(15)
      .setWidth(10)

    val preprocessed = tr.transform(images)

    val out_sizes = preprocessed.select(preprocessed("out.height"), preprocessed("out.width")).collect

    out_sizes.foreach(
      (row:Row) => {
        assert(row.getInt(0) == 15 && row.getInt(1) == 10, "output images have incorrect size")
      }
    )

    val unroll = new UnrollImage()
      .setInputCol(tr.getOutputCol)
      .setOutputCol("final")

    val result = unroll.transform(preprocessed).select("final")
    result.collect().foreach(
      (row:Row) => {
        assert(row(0).asInstanceOf[DenseVector].toArray.length == 10 * 15 * 3, "unrolled image is incorrect")
      }
    )
  }

  test("Values match the old resize values"){

    val images = session.readImages(fileLocation, recursive = true)
    assert(images.count() == 30)

    val tr = new ResizeImageTransformer()
      .setOutputCol("out")
      .setHeight(15)
      .setWidth(10)

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

    (unrolledScala, unrolledOpenCV).zipped.foreach((rowScala, rowOpenCV) => {

      val actual = rowScala(0).asInstanceOf[DenseVector].toArray
      val expected = rowOpenCV(0).asInstanceOf[DenseVector].toArray
      if (!compareArrays(actual, expected)) {
        println("result:   " + actual.deep.toString)
        println("expected: " + expected.deep.toString)
        throw new Exception("incorrect numeric value for flattened image")
      }
    })

  }

  test("Scala resize performance") {
    val start = System.currentTimeMillis()

    // Remove files that are not an image. Like .csv before running this.
    val images = session.readBinaryFiles(fileLocation, recursive = true)
      .withColumn("image", col("value.bytes"))
    assert(images.count() == 30)

    val tr = new ResizeImageTransformer()
      .setOutputCol("out")
      .setHeight(15)
      .setWidth(10)

    tr.transform(images).foreach(_ => {})
    println((System.currentTimeMillis()-start)/1000.0)
  }

  override def reader: MLReadable[_] = ResizeImageTransformer

  override def testObjects(): Seq[TestObject[ResizeImageTransformer]] = Seq(new TestObject[ResizeImageTransformer](
    new ResizeImageTransformer().setOutputCol("out")
      .setHeight(15)
      .setWidth(10),
      session.readImages(fileLocation, recursive = true))
  )
}
