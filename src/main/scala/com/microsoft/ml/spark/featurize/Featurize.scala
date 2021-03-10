// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.featurize

import java.sql.{Date, Timestamp}
import java.time.temporal.ChronoField

import com.microsoft.ml.spark.codegen.Wrappable
import com.microsoft.ml.spark.core.contracts.{HasInputCols, HasOutputCol}
import com.microsoft.ml.spark.core.schema.DatasetExtensions._
import com.microsoft.ml.spark.featurize.text.TextFeaturizer
import com.microsoft.ml.spark.stages.{DropColumns, Lambda, UDFTransformer}
import org.apache.spark.ml.feature.{Imputer, OneHotEncoder, SQLTransformer, VectorAssembler}
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Pipeline, PipelineModel}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._

import scala.collection.mutable

private[spark] object FeaturizeUtilities {
  // 2^18 features by default
  val NumFeaturesDefault = 262144
  // 2^12 features for tree-based or NN-based learners
  val NumFeaturesTreeOrNNBased = 4096
}

object Featurize extends DefaultParamsReadable[Featurize]

/** Featurizes a dataset. Converts the specified columns to feature columns. */
class Featurize(override val uid: String) extends Estimator[PipelineModel]
  with Wrappable with DefaultParamsWritable with HasOutputCol with HasInputCols {

  def this() = this(Identifiable.randomUID("Featurize"))

  /** One hot encode categorical columns when true; default is true
    *
    * @group param
    */
  val oneHotEncodeCategoricals: Param[Boolean] = new BooleanParam(this,
    "oneHotEncodeCategoricals",
    "One-hot encode categorical columns")

  setDefault(oneHotEncodeCategoricals -> true)

  /** @group getParam */
  final def getOneHotEncodeCategoricals: Boolean = $(oneHotEncodeCategoricals)

  /** @group setParam */
  def setOneHotEncodeCategoricals(value: Boolean): this.type = set(oneHotEncodeCategoricals, value)

  /** Number of features to hash string columns to
    *
    * @group param
    */
  val numFeatures: IntParam = new IntParam(this, "numFeatures",
    "Number of features to hash string columns to")
  setDefault(numFeatures -> FeaturizeUtilities.NumFeaturesDefault)

  /** @group getParam */
  final def getNumFeatures: Int = $(numFeatures)

  /** @group setParam */
  def setNumFeatures(value: Int): this.type = set(numFeatures, value)

  val imputeMissing: Param[Boolean] = new BooleanParam(this, "imputeMissing",
    "Whether to impute missing values")
  setDefault(imputeMissing -> true)

  /** @group getParam */
  final def getImputeMissing: Boolean = $(imputeMissing)

  /** @group setParam */
  def setImputeMissing(value: Boolean): this.type = set(imputeMissing, value)

  //noinspection ScalaStyle
  def featurizeSingleColumn(dataset: Dataset[_], col: String): (Pipeline, String) = {
    val usedColumns: mutable.HashSet[String] = new mutable.HashSet() ++ dataset.columns
    val colsToDrop: mutable.HashSet[String] = new mutable.HashSet()
    var newCol = col
    var oldCol = col
    var dType = dataset.schema(newCol).dataType

    def updateNewCol(prefix: String): Unit = {
      oldCol = newCol
      newCol = findUnusedColumnName(col + prefix)(usedColumns.toSet)
      usedColumns += newCol
      colsToDrop += newCol
    }

    val metadata = dataset.schema(oldCol).metadata
    val isCategorical = getOneHotEncodeCategoricals &&
      metadata.contains("ml_attr") &&
      metadata.getMetadata("ml_attr").contains("type") &&
      metadata.getMetadata("ml_attr").getString("type") == "nominal"

    val caster = dType match {
      case _ if isCategorical =>
        updateNewCol("_encoded")
        dType = VectorType
        Some(new OneHotEncoder().setInputCol(oldCol).setOutputCol(newCol))
      case _: FloatType | _: LongType | _: IntegerType =>
        updateNewCol("_cast")
        dType = DoubleType
        Some(new SQLTransformer().setStatement(s"SELECT *, cast(`$oldCol` as double) AS `$newCol` FROM __THIS__"))
      case _ =>
        None
    }

    val imputer = dType match {
      case _: DoubleType if getImputeMissing =>
        updateNewCol("_imputed")
        Some(new CleanMissingData().setInputCols(Array(oldCol)).setOutputCols(Array(newCol)))
      case _ =>
        None
    }

    val featurizer = {
      dType match {
        case _: StringType =>
          val oldCol2 = oldCol
          val m0 = new Lambda().setTransform(df => df.na.fill("", Seq(oldCol2))).setTransformSchema({ x => x })
          updateNewCol("_featurized")
          val m1 = new TextFeaturizer().setNumFeatures(getNumFeatures).setInputCol(oldCol).setOutputCol(newCol)
          updateNewCol("_selected")
          val m2 = new CountSelector().setInputCol(oldCol).setOutputCol(newCol)
          Some(new Pipeline().setStages(Array(m0, m1, m2)))
        case _: TimestampType =>
          updateNewCol("_featurized")
          val featurizeUdf = udf((ts: Timestamp) => {
            val localDate = ts.toLocalDateTime
            Vectors.dense(Array[Double](
              ts.getTime.toDouble,
              localDate.getYear.toDouble,
              localDate.getDayOfWeek.getValue.toDouble,
              localDate.getMonth.getValue.toDouble,
              localDate.getDayOfMonth.toDouble,
              localDate.get(ChronoField.HOUR_OF_DAY).toDouble,
              localDate.get(ChronoField.MINUTE_OF_HOUR).toDouble,
              localDate.get(ChronoField.SECOND_OF_MINUTE).toDouble))
          })
          Some(new UDFTransformer().setInputCol(oldCol).setOutputCol(newCol).setUDF(featurizeUdf))
        case _: DateType =>
          updateNewCol("_featurized")
          val featurizeUdf = udf((d: Date) => {
            val localDate = d.toLocalDate
            Vectors.dense(Array[Double](d.getTime.toDouble,
              localDate.getYear.toDouble,
              localDate.getDayOfWeek.getValue.toDouble,
              localDate.getMonth.getValue.toDouble,
              localDate.getDayOfMonth.toDouble))
          })
          Some(new UDFTransformer().setInputCol(oldCol).setOutputCol(newCol).setUDF(featurizeUdf))
        case _ =>
          None
      }
    }

    val dropper = Some(new DropColumns().setCols((colsToDrop - newCol).toArray))
    val pipe = new Pipeline().setStages(Seq(caster, imputer, featurizer, dropper).flatten.toArray)

    (pipe, newCol)
  }

  /** Featurizes the dataset.
    *
    * @param dataset The input dataset to train.
    * @return The featurized model.
    */
  override def fit(dataset: Dataset[_]): PipelineModel = {
    val (pipes, newCols) = getInputCols.map(featurizeSingleColumn(dataset, _)).unzip
    val va = new VectorAssembler().setInputCols(newCols).setOutputCol(getOutputCol).setHandleInvalid("skip")
    val dropper = new DropColumns().setCols(newCols.diff(getInputCols))
    val stages = pipes.toSeq ++ Seq(va) ++ Seq(dropper)
    val pipe = new Pipeline().setStages(stages.toArray)
    val fitModel = pipe.fit(dataset)
    fitModel
  }

  override def copy(extra: ParamMap): Estimator[PipelineModel] = {
    new Featurize()
  }

  override def transformSchema(schema: StructType): StructType =
    schema.add(getOutputCol, VectorType)

}
