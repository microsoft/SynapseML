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

/** Featurizes a dataset. Converts the specified columns to feature columns.
  *
  * Text inputs whose fitted text features are all zero contribute no output slots. Fitting fails
  * with a column-specific error when every requested input collapses this way.
  */
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

  /** How the final internal VectorAssembler stage handles null input values and Double.NaN
    * scalar numeric values when assembling the output feature vector.
    *
    * This only affects that last assembly step; it does not change text featurization,
    * categorical one-hot encoding, or the imputeMissing mean/median imputation performed earlier
    * in the pipeline. Supported values mirror the VectorAssembler handleInvalid parameter in
    * Spark 3.5:
    *  - "skip" (default): drop rows containing an invalid value, preserving the
    *    historical Featurize behavior.
    *  - "error": fail transform if an invalid value is encountered.
    *  - "keep": preserve every row and encode invalid values as Double.NaN in the assembled
    *    feature vector, e.g. so LightGBM native missing-value handling can use them. A null
    *    vector input becomes one Double.NaN per vector element.
    *    Set imputeMissing to false as well if raw missing values (rather than imputed ones)
    *    should reach the assembler.
    *
    * Note: input columns that are already vectors need size metadata (for example added via
    * VectorSizeHint) to use "keep"; otherwise Spark cannot infer their length and will throw.
    *
    * @group param
    */
  val vectorAssemblerHandleInvalid: Param[String] = new Param[String](this,
    "vectorAssemblerHandleInvalid",
    "How the final VectorAssembler stage handles null inputs and scalar numeric NaN values: " +
      "skip (default, drops rows, matches prior behavior), error (fails on invalid values), " +
      "or keep (preserves rows and encodes invalid values as Double.NaN, e.g. " +
      "for LightGBM missing-value handling). Only affects the final VectorAssembler; does not " +
      "change text, categorical, or imputeMissing behavior. Vector-typed input columns without " +
      "size metadata (see VectorSizeHint) can fail to infer lengths when set to keep.",
    ParamValidators.inArray(Array("skip", "error", "keep")))

  setDefault(vectorAssemblerHandleInvalid -> "skip")

  /** @group getParam */
  final def getVectorAssemblerHandleInvalid: String = $(vectorAssemblerHandleInvalid)

  /** @group setParam */
  def setVectorAssemblerHandleInvalid(value: String): this.type = set(vectorAssemblerHandleInvalid, value)

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

  private def emptyCountSelectorModels(stage: PipelineStage): Seq[CountSelectorModel] = {
    stage match {
      case model: CountSelectorModel if model.getIndices.isEmpty =>
        Seq(model)
      case model: PipelineModel =>
        model.stages.flatMap(emptyCountSelectorModels)
      case _ =>
        Seq.empty
    }
  }

  private def validateUsableFeatures(featureColumns: Seq[String],
                                     emptyTextColumns: Seq[String]): Unit = {
    val distinctEmptyTextColumns = emptyTextColumns.distinct
    val emptyTextColumnSet = distinctEmptyTextColumns.toSet
    if (featureColumns.nonEmpty && featureColumns.forall(emptyTextColumnSet.contains)) {
      val columnLabel = if (distinctEmptyTextColumns.length == 1) "column" else "columns"
      val formattedColumns = distinctEmptyTextColumns.sorted.map(col => s"'$col'").mkString(", ")
      throw new IllegalArgumentException(
        s"No usable featurized features were produced. Text $columnLabel $formattedColumns " +
          "produced zero-width feature vectors after text featurization. This usually means " +
          "the input values were constant, empty, or otherwise contained no information.")
    }
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
      val textSelectorOutputs = mutable.Map[String, String]()

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
              textSelectorOutputs.update(newCol2, baseCol)
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
          .setHandleInvalid(getVectorAssemblerHandleInvalid),
        new DropColumns().setCols(columnState.getColsToDrop.toArray)
      )

      val featurizedModel =
        new Pipeline().setStages(Seq(encoders, casters, imputers, featurizers, va).flatten.toArray).fit(dataset)
      val emptyTextColumns = featurizedModel.stages
        .flatMap(emptyCountSelectorModels)
        .flatMap(model => textSelectorOutputs.get(model.getOutputCol))
      validateUsableFeatures(getInputCols.toSeq, emptyTextColumns)
      featurizedModel
    }, dataset.columns.length)
  }
  //scalastyle:on cyclomatic.complexity
  //scalastyle:on method.length

  override def copy(extra: ParamMap): Estimator[PipelineModel] = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType =
    schema.add(getOutputCol, VectorType)

}
