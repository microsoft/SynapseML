// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import com.microsoft.ml.spark.schema.DatasetExtensions
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.param.{DoubleParam, IntParam, ParamMap, TransformerParam}
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.ml.util.{ComplexParamsWritable, Identifiable}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{ArrayType, BooleanType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._


import scala.collection.mutable

/** Distributed implementation of
  * Local Interpretable Model-Agnostic Explanations (LIME)
  *
  * https://arxiv.org/pdf/1602.04938v1.pdf
  */
class ImageLIME(val uid: String) extends Transformer
  with HasInputCol with HasOutputCol with HasLabelCol
  with Wrappable with ComplexParamsWritable {
  def this() = this(Identifiable.randomUID("LIME"))

  val model = new TransformerParam(this, "model", "Model to try to locally approximate")

  def getModel: Transformer = $(model)

  def setModel(v: Transformer): this.type = set(model, v)

  val nSamples = new IntParam(this, "nSamples", "The number of samples to generate")

  def getNSamples: Int = $(nSamples)

  def setNSamples(v: Int): this.type = set(nSamples, v)

  val samplingFraction = new DoubleParam(this, "samplingFraction", "The fraction of superpixels to keep on")

  def getSamplingFraction: Double = $(samplingFraction)

  def setSamplingFraction(d: Double): this.type = set(samplingFraction, d)

  val cellSize = new DoubleParam(this, "cellSize", "Parameter relating to the size of the superpixel cells")

  def getCellSize: Double = $(cellSize)

  def setCellSize(d: Double): this.type = set(cellSize, d)

  //TODO figure out what the modifier does
  val modifier = new DoubleParam(this, "modifier", "Parameter relating to the superpixel creation")

  def getModifier: Double = $(modifier)

  def setModifier(d: Double): this.type = set(modifier, d)

  setDefault(nSamples -> 900, cellSize -> 16, modifier -> 130, samplingFraction -> 0.3)

  override def transform(dataset: Dataset[_]): DataFrame = {

    val df = dataset.toDF
    val model = getModel

    import df.sparkSession.implicits._

    val superpixelCol = DatasetExtensions
      .findUnusedColumnName("superpixels", df)

    val localFeaturesCol = DatasetExtensions
      .findUnusedColumnName("localFeatures", df)

    val localModel = new LinearRegression()
      .setLabelCol(getLabelCol)
      .setFeaturesCol(localFeaturesCol)

    // Data frame with new column containing superpixels (Array[Cluster]) for each row (image)
    val spt = new SuperpixelTransformer()
      .setCellSize(getCellSize)
      .setModifier(getModifier)
      .setInputCol(getInputCol)
      .setOutputCol(superpixelCol)

    val spDF = spt.transform(df)

    // Indices of the columns containing each image and image's superpixels
    val superpixelIndex = spDF.schema.fieldIndex(spt.getOutputCol)
    val spDFSchema = spDF.schema

    val indiciesToKeep = spDF.columns.indices//.filter(_ != superpixelIndex)

    // Collects to head node and creates a data frame from each row (image)
    val sampledIterator = spDF.toLocalIterator().map { row =>

      // Gets the superpixels from the row
      val superpixels = SuperpixelData.fromRow(row.getAs[Row](superpixelIndex))

      // Generate samples for the image
      val samples = Superpixel
        .clusterStateSampler(getSamplingFraction, superpixels.clusters.length)
        .take(getNSamples).toList

      // Creates a new data frame for each image, containing samples of cluster states
      val censoredDF = samples.toDF(localFeaturesCol)
        .map(stateRow => Row.merge(row, stateRow))(
          RowEncoder(spDFSchema.add(localFeaturesCol, ArrayType(BooleanType))))
        .withColumn(getInputCol, Superpixel.censorUDF(
          col(getInputCol), col(spt.getOutputCol), col(localFeaturesCol)))
        .withColumn(localFeaturesCol,
          udf(
            { barr: mutable.WrappedArray[Boolean] => new DenseVector(barr.map(b => if (b) 1.0 else 0.0).toArray) },
            VectorType)(col(localFeaturesCol)))

      // Maps the data frame through the deep model
      val mappedLocalDF = model.transform(censoredDF)
      println(mappedLocalDF.count())

      // Fits the data frame to the local model (regression), outputting the weights of importance
      val coefficients = localModel.fit(mappedLocalDF) match {
        case lr: LinearRegressionModel => lr.coefficients
      }

      Row(indiciesToKeep.map(row.get) ++ Seq(coefficients):_*)
    }

    val outputDF = df.sparkSession.createDataFrame(sampledIterator.toSeq, spDF.schema
      .add(getOutputCol, VectorType))

    outputDF
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  /** Add the features column to the schema
    *
    * @param schema
    * @return schema with features column
    */
  override def transformSchema(schema: StructType): StructType = {
    schema.add(getOutputCol, VectorType)
  }

}
