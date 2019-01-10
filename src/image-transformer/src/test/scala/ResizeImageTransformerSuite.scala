// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import com.microsoft.ml.spark.IOImplicits._
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.Row

class ResizeImageTransformerSuite extends TransformerFuzzing[ResizeImageTransformer]
  with ImageTestUtils {

  lazy val images = session.read.image
    .option("dropInvalid",true).load(fileLocation + "**")

  lazy val tr = new ResizeImageTransformer()
    .setOutputCol("out")
    .setHeight(15)
    .setWidth(10)

  test("general workflow") {
    assert(images.count() == 30)
    val preprocessed = tr.transform(images)

    val out_sizes = preprocessed
      .select(preprocessed("out.height"), preprocessed("out.width"))
      .collect

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
      assert(actual === expected)
    })

  }

  override def reader: MLReadable[_] = ResizeImageTransformer

  override def testObjects(): Seq[TestObject[ResizeImageTransformer]] =
    Seq(new TestObject(tr, images))
}
