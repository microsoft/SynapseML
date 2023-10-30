// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.featurize

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.contracts.{HasInputCols, HasOutputCol}
import com.microsoft.azure.synapse.ml.featurize.text.TextFeaturizer
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import com.microsoft.azure.synapse.ml.stages.{DropColumns, Lambda, UDFTransformer}
import org.apache.spark.ml.feature.{OneHotEncoder, SQLTransformer, VectorAssembler}
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._

import java.sql.{Date, Timestamp}
import java.time.temporal.ChronoField
import scala.collection.mutable

private[ml] object FeaturizeUtilities {
  // 2^18 features by default
  val NumFeaturesDefault = 262144
  // 2^12 features for tree-based or NN-based learners
  val NumFeaturesTreeOrNNBased = 4096
}

object Featurize extends DefaultParamsReadable[Featurize]

/** Featurizes a dataset. Converts the specified columns to feature columns. */
class Featurize(override val uid: String) extends Estimator[PipelineModel]
  with Wrappable with DefaultParamsWritable with HasOutputCol with HasInputCols with SynapseMLLogging {
  logClass(FeatureNames.Featurize)

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

  private case class ColumnInfo(originalName: String, dataType: DataType, version: Int = 0) {
    def currentName: String = {
      if (version == 0) {
        originalName
      } else {
        s"${originalName}_${uid}_$version"
      }
    }
  }

  private class ColumnState(df: Dataset[_]) {

    private val colsToDrop = mutable.Set[String]()

    private val columnInfoMap = mutable.Map(getInputCols.map(ic =>
      (ic, ColumnInfo(ic, df.schema(ic).dataType))): _*)

    def makeNewCol(baseCol: String, dataType: DataType): String = {
      val oldInfo = columnInfoMap(baseCol)
      val newInfo = ColumnInfo(oldInfo.originalName, dataType, oldInfo.version + 1)
      colsToDrop.add(newInfo.currentName)
      columnInfoMap.update(baseCol, newInfo)
      newInfo.currentName
    }

    def getCurrentInfo(baseCol: String): ColumnInfo = columnInfoMap(baseCol)

    def getCurrentCols: Seq[String] = columnInfoMap.values.map(_.currentName).toSeq

    def getColsToDrop: Seq[String] = colsToDrop.toSeq

  }

  /** Featurizes the dataset.
    *
    * @param dataset The input dataset to train.
    * @return The featurized model.
    */
  //scalastyle:off cyclomatic.complexity
  //scalastyle:off method.length
  override def fit(dataset: Dataset[_]): PipelineModel = {
    logFit({
      val columnState = new ColumnState(dataset)

      val (oldEncoderCols, newEncoderCols) = getInputCols.flatMap {
        baseCol =>
          val metadata = dataset.schema(baseCol).metadata
          val isCategorical = getOneHotEncodeCategoricals &&
            metadata.contains("ml_attr") &&
            metadata.getMetadata("ml_attr").contains("type") &&
            metadata.getMetadata("ml_attr").getString("type") == "nominal"
          columnState.getCurrentInfo(baseCol).dataType match {
            case _ if isCategorical =>
              Some(columnState.getCurrentInfo(baseCol).currentName, columnState.makeNewCol(baseCol, VectorType))
            case _ =>
              None
          }
      }.unzip

      val encoders: Array[PipelineStage] = Array(new OneHotEncoder()
        .setInputCols(oldEncoderCols).setOutputCols(newEncoderCols))

      val casters: Array[PipelineStage] = getInputCols.flatMap {
        baseCol =>
          val metadata = dataset.schema(baseCol).metadata
          val isCategorical = getOneHotEncodeCategoricals &&
            metadata.contains("ml_attr") &&
            metadata.getMetadata("ml_attr").contains("type") &&
            metadata.getMetadata("ml_attr").getString("type") == "nominal"
          columnState.getCurrentInfo(baseCol).dataType match {
            case _ if isCategorical =>
              None
            case _: FloatType | _: LongType | _: IntegerType =>
              val oldCol = columnState.getCurrentInfo(baseCol).currentName
              val newCol = columnState.makeNewCol(baseCol, DoubleType)
              Some(new SQLTransformer().setStatement(s"SELECT *, cast(`$oldCol` as double) AS `$newCol` FROM __THIS__"))
            case _ =>
              None
          }
      }

      val (oldImputerCols, newImputerCols) = getInputCols.flatMap {
        baseCol =>
          columnState.getCurrentInfo(baseCol).dataType match {
            case _: DoubleType if getImputeMissing =>
              Some(columnState.getCurrentInfo(baseCol).currentName, columnState.makeNewCol(baseCol, DoubleType))
            case _ =>
              None
          }
      }.unzip

      val imputers: Array[PipelineStage] = Array(new CleanMissingData()
        .setInputCols(oldImputerCols).setOutputCols(newImputerCols))

      val featurizers: Array[PipelineStage] = getInputCols.flatMap {
        baseCol =>
          val oldCol = columnState.getCurrentInfo(baseCol).currentName
          columnState.getCurrentInfo(baseCol).dataType match {
            case _: StringType =>
              val newCol = columnState.makeNewCol(baseCol, VectorType)
              val m0 = new Lambda().setTransform(df => df.na.fill("", Seq(oldCol))).setTransformSchema({
                x => x
              })
              val m1 = new TextFeaturizer().setNumFeatures(getNumFeatures).setInputCol(oldCol).setOutputCol(newCol)
              val newCol2 = columnState.makeNewCol(baseCol, VectorType)
              val m2 = new CountSelector().setInputCol(newCol).setOutputCol(newCol2)
              Some(new Pipeline().setStages(Array(m0, m1, m2)))
            case _: TimestampType =>
              val newCol = columnState.makeNewCol(baseCol, VectorType)
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
              val newCol = columnState.makeNewCol(baseCol, VectorType)
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

      val va: Array[PipelineStage] = Array(
        new VectorAssembler()
          .setInputCols(columnState.getCurrentCols.toArray)
          .setOutputCol(getOutputCol)
          .setHandleInvalid("skip"),
        new DropColumns().setCols(columnState.getColsToDrop.toArray)
      )

      new Pipeline().setStages(Seq(encoders, casters, imputers, featurizers, va).flatten.toArray).fit(dataset)
    }, dataset.columns.length)
  }
  //scalastyle:on cyclomatic.complexity
  //scalastyle:on method.length

  override def copy(extra: ParamMap): Estimator[PipelineModel] = {
    new Featurize()
  }

  override def transformSchema(schema: StructType): StructType =
    schema.add(getOutputCol, VectorType)

}
