// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import breeze.linalg.{DenseMatrix => BDM}
import com.microsoft.ml.spark.FluentAPI._
import com.microsoft.ml.spark.schema.DatasetExtensions
import org.apache.spark.internal.{Logging => SLogging}
import org.apache.spark.ml.linalg.SQLDataTypes.{MatrixType, VectorType}
import org.apache.spark.ml.linalg.{DenseMatrix, DenseVector}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.{ComplexParamsReadable, ComplexParamsWritable, Identifiable}
import org.apache.spark.ml.{LimeNamespaceInjections, Transformer}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.collection.mutable

object LIMEUtils extends SLogging {

  def localAggregateBy(df: DataFrame, groupByCol: String, colsToSquish: Seq[String]): DataFrame = {
    val schema = new StructType(df.schema.fields.map {
      case field if colsToSquish.contains(field.name) => StructField(field.name, ArrayType(field.dataType))
      case f => f
    })
    val encoder = RowEncoder(schema)
    val indiciesToSquish = colsToSquish.map(df.schema.fieldIndex)
    df.mapPartitions { it =>
      val isEmpty = it.isEmpty
      if (isEmpty) {
        (Nil: Seq[Row]).toIterator
      } else {
        // Current Id, What we have accumulated, Previous Row
        val accumulator = mutable.ListBuffer[Seq[Any]]()
        var currentId: Option[Any] = None
        var prevRow: Option[Row] = None

        def returnState(accumulated: List[Seq[Any]], prevRow: Row): Row = {
          Row.fromSeq(prevRow.toSeq.zipWithIndex.map {
            case (v, i) if indiciesToSquish.contains(i) =>
              accumulated.map(_.apply(indiciesToSquish.indexOf(i)))
            case (v, i) => v
          })
        }

        def enqueueAndMaybeReturn(row: Row): Option[Row] = {
          val id = row.getAs[Any](groupByCol)
          if (currentId.isEmpty) {
            currentId = Some(id)
            prevRow = Some(row)
            accumulator += colsToSquish.map(row.getAs[Any])
            None
          } else if (id != currentId.get) {
            val accumulated = accumulator.toList
            accumulator.clear()
            accumulator += colsToSquish.map(row.getAs[Any])
            val modified = returnState(accumulated, prevRow.get)
            currentId = Some(id)
            prevRow = Some(row)
            Some(modified)
          } else {
            prevRow = Some(row)
            accumulator += colsToSquish.map(row.getAs[Any])
            None
          }
        }

        val it1 = it
          .flatMap(row => enqueueAndMaybeReturn(row))
          .map(r => Left(r): Either[Row, Null])
          .++(Seq(Right(null): Either[Row, Null]))

        val ret = it1.map {
          case Left(r) => r: Row
          case Right(_) => returnState(accumulator.toList, prevRow.getOrElse {
            logWarning("Could not get previous row in local aggregator, this is an error that should be fixed")
            null
          }): Row
        }
        ret
      }

    }(encoder)
  }
}

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

  val samplingFraction = new DoubleParam(this, "samplingFraction", "The fraction of superpixels to keep on")

  def getSamplingFraction: Double = $(samplingFraction)

  def setSamplingFraction(d: Double): this.type = set(samplingFraction, d)

  val superpixelCol = new Param[String](this, "superpixelCol", "The column holding the superpixel decompositions")

  def getSuperpixelCol: String = $(superpixelCol)

  def setSuperpixelCol(v: String): this.type = set(superpixelCol, v)

  val regularization = new DoubleParam(this, "regularization", "regularization param for the lasso")

  def getRegularization: Double = $(regularization)

  def setRegularization(v: Double): this.type = set(regularization, v)

  setDefault(nSamples -> 900, cellSize -> 16, modifier -> 130, regularization -> 0.0,
    samplingFraction -> 0.3, superpixelCol -> "superpixels")

  override def transform(dataset: Dataset[_]): DataFrame = {

    val df = dataset.toDF
    val model = getModel

    val idCol = DatasetExtensions.findUnusedColumnName("imageID", df)
    val statesCol = DatasetExtensions.findUnusedColumnName("states", df)
    val inputCol2 = DatasetExtensions.findUnusedColumnName("inputCol2", df)

    // Data frame with new column containing superpixels (Array[Cluster]) for each row (image)
    val spt = new SuperpixelTransformer()
      .setCellSize(getCellSize)
      .setModifier(getModifier)
      .setInputCol(getInputCol)
      .setOutputCol(getSuperpixelCol)

    val spDF = spt.transform(df)

    // Indices of the columns containing each image and image's superpixels
    val inputType = df.schema(getInputCol).dataType
    val censorUDF = inputType match {
      case BinaryType => Superpixel.censorBinaryUDF
      case t if ImageSchemaUtils.isImage(t) => Superpixel.censorUDF
    }

    def getSamples(n: Int): Seq[Seq[Boolean]] = {
      Superpixel
        .clusterStateSampler(getSamplingFraction, n)
        .take(getNSamples).map(_.toSeq).toSeq
    }

    val getSampleUDF = udf(getSamples _, ArrayType(ArrayType(BooleanType)))

    val fitLassoUDF = udf(LimeNamespaceInjections.fitLasso _, VectorType)

    def arrToMat(dvs: Seq[DenseVector]): DenseMatrix = {
      val mat = BDM(dvs.map(_.values): _*)
      new DenseMatrix(mat.rows, mat.cols, mat.data)
    }

    val arrToMatUDF = udf(arrToMat _, MatrixType)

    def arrToVect(ds: Seq[Double]): DenseVector = {
      new DenseVector(ds.toArray)
    }

    val arrToVectUDF = udf(arrToVect _, VectorType)

    val mapped = spDF.withColumn(idCol, monotonically_increasing_id())
      .withColumnRenamed(getInputCol, inputCol2)
      .withColumn(statesCol, explode_outer(getSampleUDF(size(col(getSuperpixelCol).getField("clusters")))))
      .withColumn(getInputCol, censorUDF(col(inputCol2), col(spt.getOutputCol), col(statesCol)))
      .withColumn(statesCol, udf(
        { barr: Seq[Boolean] => new DenseVector(barr.map(b => if (b) 1.0 else 0.0).toArray) },
        VectorType)(col(statesCol)))
      .mlTransform(model)
      .drop(getInputCol)

    LIMEUtils.localAggregateBy(mapped, idCol, Seq(statesCol, getLabelCol))
      .withColumn(statesCol, arrToMatUDF(col(statesCol)))
      .withColumn(getLabelCol, arrToVectUDF(col(getLabelCol)))
      .withColumn(getOutputCol, fitLassoUDF(col(statesCol), col(getLabelCol), lit(getRegularization)))
      .drop(statesCol, getLabelCol)
      .withColumnRenamed(inputCol2, getInputCol)
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    schema.add(getSuperpixelCol, SuperpixelData.schema).add(getOutputCol, VectorType)
  }

}
