// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import com.microsoft.ml.spark.schema.{DatasetExtensions, ImageSchema}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.param._
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.ml.util.{ComplexParamsReadable, ComplexParamsWritable, Identifiable}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.collection.JavaConversions._

object ImageLIME extends ComplexParamsReadable[ImageLIME]

/** Distributed implementation of
  * Local Interpretable Model-Agnostic Explanations (LIME)
  *
  * https://arxiv.org/pdf/1602.04938v1.pdf
  */
class ImageLIME(val uid: String) extends Transformer
  with HasInputCol with HasOutputCol with HasLabelCol
  with Wrappable with ComplexParamsWritable
  with HasModifier with HasCellSize {

  def this() = this(Identifiable.randomUID("LIME"))

  val model = new TransformerParam(this, "model", "Model to try to locally approximate")

  def getModel: Transformer = $(model)

  def setModel(v: Transformer): this.type = set(model, v)

  val nSamples = new IntParam(this, "nSamples", "The number of samples to generate")

  def getNSamples: Int = $(nSamples)

  def setNSamples(v: Int): this.type = set(nSamples, v)

  val localModelPartitions = new IntParam(this, "localModelPartitions",
    "The number of partitions to coalesce to to fit the local model")

  def getLocalModelPartitions: Int = $(localModelPartitions)

  def setLocalModelPartitions(v: Int): this.type = set(localModelPartitions, v)

  val samplingFraction = new DoubleParam(this, "samplingFraction", "The fraction of superpixels to keep on")

  def getSamplingFraction: Double = $(samplingFraction)

  def setSamplingFraction(d: Double): this.type = set(samplingFraction, d)

  val superpixelCol = new Param[String](this, "superpixelCol", "The column holding the superpixel decompositions")

  def getSuperpixelCol: String = $(superpixelCol)

  def setSuperpixelCol(v: String): this.type = set(superpixelCol, v)

  setDefault(nSamples -> 900, cellSize -> 16, modifier -> 130,
    samplingFraction -> 0.3, superpixelCol -> "superpixels")

  override def transform(dataset: Dataset[_]): DataFrame = {

    val df = dataset.toDF
    val model = getModel
    val sc = df.sparkSession.sparkContext

    import df.sparkSession.implicits._

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
      .setOutputCol(getSuperpixelCol)

    val spDF = spt.transform(df)

    // Indices of the columns containing each image and image's superpixels
    val superpixelIndex = spDF.schema.fieldIndex(spt.getOutputCol)
    val spDFSchema = spDF.schema

    val indiciesToKeep = spDF.columns.indices

    val inputType = df.schema(getInputCol).dataType
    val censorUDF = inputType match {
      case BinaryType => Superpixel.censorBinaryUDF
      case t if t == ImageSchema.columnSchema => Superpixel.censorUDF
    }

    // Collects to head node and creates a data frame from each row (image)
    val sampledIterator = spDF.toLocalIterator().map { row =>

      // Gets the superpixels from the row
      val superpixels = SuperpixelData.fromRow(row.getAs[Row](superpixelIndex))

      // Generate samples for the image
      val samples = Superpixel
        .clusterStateSampler(getSamplingFraction, superpixels.clusters.length)
        .take(getNSamples).toList

      val broadcastedRow = sc.broadcast(row)

      // Creates a new data frame for each image, containing samples of cluster states
      val censoredDF = samples.toDF(localFeaturesCol)
        .map(stateRow => Row.merge(broadcastedRow.value, stateRow))(
          RowEncoder(spDFSchema.add(localFeaturesCol, ArrayType(BooleanType))))
        .withColumn(getInputCol,
          censorUDF(col(getInputCol), col(spt.getOutputCol), col(localFeaturesCol)))
        .withColumn(localFeaturesCol,
          udf(
            { barr: Seq[Boolean] => new DenseVector(barr.map(b => if (b) 1.0 else 0.0).toArray) },
            VectorType)(col(localFeaturesCol)))

      // Maps the data frame through the deep model
      val mappedLocalDF1 = model.transform(censoredDF)
        .select(localFeaturesCol, getLabelCol)

      val mappedLocalDF = get(localModelPartitions)
        .map(lmp => mappedLocalDF1.coalesce(lmp))
        .getOrElse(mappedLocalDF1).cache()

      // Fits the data frame to the local model (regression), outputting the weights of importance
      val coefficients = localModel.fit(mappedLocalDF) match {
        case lr: LinearRegressionModel => lr.coefficients
      }

      mappedLocalDF.unpersist()
      broadcastedRow.unpersist()
      // Creates the result row: original row + superpixels + coeffcients
      Row(indiciesToKeep.map(row.get) ++ Seq(coefficients):_*)
    }

    val outputDF = df.sparkSession.createDataFrame(sampledIterator.toSeq, spDF.schema
      .add(getOutputCol, VectorType))

    outputDF
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    schema.add(getSuperpixelCol, SuperpixelData.schema).add(getOutputCol, VectorType)
  }

}
