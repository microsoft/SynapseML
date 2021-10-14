// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.lime

import java.awt.image.BufferedImage
import java.io.File
import java.net.URL
import com.microsoft.ml.spark.cntk.{ImageFeaturizer, TrainedCNTKModelUtils}
import com.microsoft.ml.spark.core.test.fuzzing.{TestObject, TransformerFuzzing}
import com.microsoft.ml.spark.io.IOImplicits._
import com.microsoft.ml.spark.io.image.ImageUtils
import com.microsoft.ml.spark.io.split1.FileReaderUtils
import com.microsoft.ml.spark.stages.UDFTransformer
import com.microsoft.ml.spark.stages.udfs.get_value_udf
import org.apache.commons.io.FileUtils
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.param.DataFrameEquality
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.ml.{NamespaceInjections, PipelineModel}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row}
import org.bytedeco.opencv.global.{opencv_core, opencv_ximgproc}

@deprecated("Please use 'com.microsoft.ml.spark.explainers.ImageLIME'.", since="1.0.0-RC3")
class ImageLIMESuite extends TransformerFuzzing[ImageLIME] with
  DataFrameEquality with TrainedCNTKModelUtils with FileReaderUtils {

  lazy val greyhoundImageLocation: String = {
    val loc = "/tmp/greyhound.jpg"
    val f = new File(loc)
    if (f.exists()) {
      f.delete()
    }
    FileUtils.copyURLToFile(new URL("https://mmlspark.blob.core.windows.net/datasets/LIME/greyhound.jpg"), f)
    loc
  }

  lazy val resNetTransformer: ImageFeaturizer = resNetModel().setCutOutputLayers(0)
  lazy val getGreyhoundClass: UDFTransformer = new UDFTransformer()
    .setInputCol(resNetTransformer.getOutputCol)
    .setOutputCol(resNetTransformer.getOutputCol)
    .setUDF(get_value_udf(172))

  lazy val pipeline: PipelineModel = NamespaceInjections.pipelineModel(
    Array(resNetTransformer, getGreyhoundClass))

  lazy val regionSize = 30
  lazy val ruler = 50.0f

  lazy val lime: ImageLIME = new ImageLIME()
    .setModel(pipeline)
    .setPredictionCol(resNetTransformer.getOutputCol)
    .setOutputCol("weights")
    .setInputCol(inputCol)
    .setRegionSize(regionSize)
    .setRuler(ruler)
    .setIterations(10)
    .setMinElementSize(25)
    .setNSamples(3)

  lazy val df: DataFrame = spark
    .read.binary.load(greyhoundImageLocation)
    .select(col("value.bytes").alias(inputCol))

  lazy val imageDf: DataFrame = spark
    .read.image.load(greyhoundImageLocation)
    .select(col("image").alias(inputCol))

  test("Resnet should output the correct class") {
    val resNetDF = resNetTransformer.transform(df)
    val resVec = resNetDF.select(outputCol).collect()(0).getAs[DenseVector](0)
    assert(resVec.argmax == 172)
  }

  test("LIME on Binary types") {
    val result: DataFrame = lime.setNSamples(20).transform(df)
    result.show(1)
  }

  test("basic functionality") {
    import spark.implicits._

    val df = Seq(
      (1, "foo", "foo1", 11),
      (1, "bar", "foo1", 12),
      (1, "r", "foo1", 13),
      (2, "bar", "foo2", 14),
      (2, "bar2", "foo2", 15),
      (3, "bar2", "foo2", 16),
      (4, "bar2", "foo2", 17))
      .toDF("id", "b", "c", "d").coalesce(1)

    val rdf = LIMEUtils.localAggregateBy(df, "id", Seq("b", "d"))
    rdf.printSchema()
    rdf.show()
  }

  test("LIME on image Types") {
    val result: DataFrame = lime.setNSamples(20).transform(imageDf)
    result.printSchema()

    // Gets first row from the LIME-transformed data frame
    val topRow: Row = result.take(1)(0)

    // Extracts the image, superpixels, and weights of importance from the first row of the data frame
    val imgRow: Row = topRow.getAs[Row](0)

    // Converts the row values to their appropriate types
    val superpixels1: SuperpixelData = SuperpixelData.fromRow(topRow.getAs[Row]("superpixels"))
    val states = topRow.getAs[DenseVector]("weights").toArray.map(_ >= 0.008)

    val superpixels2 = SuperpixelData.fromSuperpixel(
      new Superpixel(ImageUtils.toCVMat(imgRow), opencv_ximgproc.SLIC, regionSize, ruler, 10, Some(25))
    )

    //Make sure LIME outputs the correct superpixels
    assert(superpixels1.clusters.map(_.sorted) === superpixels2.clusters.map(_.sorted))

    // Creates the censored image, the explanation of the model
    val censoredImage1 = Superpixel.maskImage(imgRow, superpixels1, states)

    // Uncomment these two lines to view the image
    // Superpixel.displayImage(censoredImage1)
  }

  override def testObjects(): Seq[TestObject[ImageLIME]] = Seq(new TestObject(lime, df))

  override def reader: MLReadable[_] = ImageLIME
}
