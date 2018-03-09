// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.awt.image.BufferedImage

import com.microsoft.ml.spark.Readers.implicits._
import com.microsoft.ml.spark.schema.ImageSchema
import org.apache.spark.ml.NamespaceInjections
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, Row}

class ImageLIMESuite extends TransformerFuzzing[ImageLIME] with
  DataFrameEquality with NetworkUtils with FileReaderUtils {

  lazy val featurizer = new ImageFeaturizer()

  import sys.process._
  import java.net.URL
  import java.io.File

  def downloadFile(url: String, f: File): Unit = {
    val src = scala.io.Source.fromURL(url)
    val out = new java.io.FileWriter(f)
    try {out.write(src.mkString)} finally {out.close()}
  }

  // TODO add this image to the datasets directory
  lazy val greyhoundImageLocation = {
    val loc = "/tmp/greyhound.jpg"
    val f = new File(loc)
    if (f.exists()) {f.delete()}
    downloadFile("https://www.petfinder.com/images/breeds/dog/1290.jpg",f)
    loc
  }

  test("Image featurizer should work with ResNet50", TestBase.Extended) {
    val cellSize = 30.0
    val modifier = 50.0

    val testImages: DataFrame = session
      //.readImages(s"$filesRoot/Images/Grocery/testImages/WIN_20160803_11_28_42_Pro.jpg", false)
      //.readImages(s"$filesRoot/Images/CIFAR/00004.png", false)
      .readImages(greyhoundImageLocation, false)
      .withColumnRenamed("image", inputCol)

    val resNet = resNetModel().setCutOutputLayers(0)
    val resNetDF = resNet.transform(testImages)
    val resVec = resNetDF.select(outputCol).collect()(0).getAs[DenseVector](0)
    assert(resVec.argmax == 171)

    //val getEntryUdf = udf({vec: org.apache.spark.ml.linalg.Vector => vec(0)}, DoubleType)
    val getEntryUdf = udf({vec: org.apache.spark.ml.linalg.Vector => vec(171)}, DoubleType)
    val udfTransformer = new UDFTransformer()
      .setInputCol(resNet.getOutputCol)
      .setOutputCol(resNet.getOutputCol)
      .setUDF(getEntryUdf)
    val pipeline = NamespaceInjections.pipelineModel(Array(resNet, udfTransformer))

    // Pass pipeline through LIME
    lazy val lime = new ImageLIME()
      .setModel(pipeline)
      .setLabelCol(resNet.getOutputCol)
      .setOutputCol("weights")
      .setInputCol(inputCol)
      .setCellSize(cellSize)
      .setModifier(modifier)

    val result: DataFrame = lime.transform(testImages)
    result.printSchema()

    // Gets first row from the LIME-transformed data frame
    val topRow: Row = result.take(1)(0)

    // Extracts the image, superpixels, and weights of importances from the first row of the data frame
    val imgRow: Row = topRow.getAs[Row](0)

    // Converts the row values to their appropriate types
    val superpixels1: SuperpixelData = SuperpixelData.fromRow(topRow.getAs[Row](1))
    val states = topRow.getAs[DenseVector](2).toArray.map(_ >= 0.008)

    val superpixels2 = SuperpixelData.fromSuperpixel(
      new Superpixel(ImageSchema.toBufferedImage(imgRow), cellSize, modifier))

    assert(superpixels1.clusters.map(_.sorted) === superpixels2.clusters.map(_.sorted))

    // Creates the censored image, the explanation of the model
    val censoredImage1: BufferedImage = Superpixel.censorImage(imgRow, superpixels1, states)

    // Print censored output
    topRow.getAs[DenseVector](2).toArray.foreach(println)

    //Superpixel.displayImage(sp.getClusteredImage)
    //Superpixel.displayImage(censoredImage1)
    //Superpixel.displayImage(censoredImage2)
  }

  //override def testObjects(): Seq[TestObject[ImageLIME]] = Seq(new TestObject(t, df))

  //override def reader: MLReadable[_] = ImageLIME
  override def testObjects(): Seq[TestObject[ImageLIME]] = ???

  override def reader: MLReadable[_] = ???
}
