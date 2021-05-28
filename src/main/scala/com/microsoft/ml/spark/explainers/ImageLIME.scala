package com.microsoft.ml.spark.explainers

import breeze.linalg.DenseVector
import breeze.stats.distributions.RandBasis
import com.microsoft.ml.spark.lime.{HasCellSize, HasModifier, SuperpixelTransformer}
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.param.Param
import org.apache.spark.ml.param.shared.HasInputCol
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Row}

trait ImageLIMEParams extends LIMEParams with HasCellSize with HasModifier with HasSamplingFraction with HasInputCol {
  self: ImageLIME =>

  val superpixelCol = new Param[String](this, "superpixelCol", "The column holding the superpixel decompositions")

  def getSuperpixelCol: String = $(superpixelCol)

  def setSuperpixelCol(v: String): this.type = set(superpixelCol, v)

  def setInputCol(value: String): this.type = this.set(inputCol, value)

  setDefault(numSamples -> 900, cellSize -> 16, modifier -> 130, regularization -> 0.0,
    samplingFraction -> 0.3, superpixelCol -> "superpixels")
}

class ImageLIME(override val uid: String)
  extends LIMEBase(uid) with ImageLIMEParams {

  def this() = {
    this(Identifiable.randomUID("ImageLIME"))
  }



  override protected def createSamples(df: DataFrame,
                                       featureStats: Seq[FeatureStats],
                                       idCol: String,
                                       distanceCol: String
                                      ): DataFrame = {
???
//    // Data frame with new column containing superpixels (Array[Cluster]) for each row (image)
//    val spt = new SuperpixelTransformer()
//      .setCellSize(getCellSize)
//      .setModifier(getModifier)
//      .setInputCol(getInputCol)
//      .setOutputCol(getSuperpixelCol)
//
//    val spDF = spt.transform(df)


  }

  override protected def createFeatureStats(df: DataFrame): Seq[FeatureStats] = ???

  override protected def extractInputVector(row: Row): DenseVector[Double] = ???
}
