// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.causal

import com.microsoft.azure.synapse.ml.causal.linalg.{DVector, VectorEntry}
import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.schema.DatasetExtensions
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import com.microsoft.azure.synapse.ml.param.DataFrameParam
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkException
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.util.{Identifiable, MLReader, MLWriter}
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, Estimator, Model}
import org.apache.spark.sql.types.{BooleanType, NumericType, StructField, StructType}
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset, SparkSession}

import java.util

abstract class BaseDiffInDiffEstimator(override val uid: String)
  extends Estimator[DiffInDiffModel]
    with DiffInDiffEstimatorParams {

  private def validateFieldNumericOrBooleanType(field: StructField): Unit = {
    val dataType = field.dataType
    require(dataType.isInstanceOf[NumericType] || dataType == BooleanType,
      s"Column ${field.name} must be numeric type or boolean type, but got $dataType instead.")
  }

  protected def validateFieldNumericType(field: StructField): Unit = {
    val dataType = field.dataType
    require(dataType.isInstanceOf[NumericType],
      s"Column ${field.name} must be numeric type, but got $dataType instead.")
  }

  override def transformSchema(schema: StructType): StructType = {
    validateFieldNumericOrBooleanType(schema(getPostTreatmentCol))
    validateFieldNumericOrBooleanType(schema(getTreatmentCol))
    validateFieldNumericType(schema(getOutcomeCol))
    schema
  }

  override def copy(extra: ParamMap): Estimator[DiffInDiffModel] = defaultCopy(extra)

  private[causal] val findInteractionCol = DatasetExtensions.findUnusedColumnName("interaction") _

  private[causal] def fitLinearModel(df: DataFrame,
                                     featureCols: Array[String],
                                     fitIntercept: Boolean,
                                     weightCol: Option[String] = None) = {

    val featuresCol = DatasetExtensions.findUnusedColumnName("features", df)
    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol(featuresCol)

    val regression = weightCol
      .map(new LinearRegression().setWeightCol)
      .getOrElse(new LinearRegression())

    regression
      .setFeaturesCol(featuresCol)
      .setLabelCol(getOutcomeCol)
      .setFitIntercept(fitIntercept)
      .setLoss("squaredError")
      .setRegParam(1E-10)

    assembler.transform _ andThen regression.fit apply df
  }
}

case class DiffInDiffSummary(treatmentEffect: Double, standardError: Double,
                             timeWeights: Option[DVector] = None,
                             timeIntercept: Option[Double] = None,
                             timeRMSE: Option[Double] = None,
                             unitWeights: Option[DVector] = None,
                             unitIntercept: Option[Double] = None,
                             unitRMSE: Option[Double] = None,
                             zeta: Option[Double] = None,
                             lossHistoryTimeWeights: Option[List[Double]] = None,
                             lossHistoryUnitWeights: Option[List[Double]] = None) {
  import scala.collection.JavaConverters._

  def getLossHistoryTimeWeightsJava: Option[util.List[Double]] = {
    lossHistoryTimeWeights.map(_.asJava)
  }

  def getLossHistoryUnitWeightsJava: Option[util.List[Double]] = {
    lossHistoryUnitWeights.map(_.asJava)
  }
}

private[causal] case class DiffInDiffSummaryData(treatmentEffect: Double,
                                                standardError: Double,
                                                hasTimeWeights: Boolean,
                                                timeIntercept: Option[Double],
                                                timeRMSE: Option[Double],
                                                hasUnitWeights: Boolean,
                                                unitIntercept: Option[Double],
                                                unitRMSE: Option[Double],
                                                zeta: Option[Double],
                                                lossHistoryTimeWeights: Option[Seq[Double]],
                                                lossHistoryUnitWeights: Option[Seq[Double]])

private object DiffInDiffSummaryPersistence {
  private val SummaryDir = "summary"
  private val DataDir = "data"
  private val TimeWeightsDir = "timeWeights"
  private val UnitWeightsDir = "unitWeights"

  def save(summary: DiffInDiffSummary, path: String, spark: SparkSession): Unit = {
    import spark.implicits._

    val summaryPath = new Path(path, SummaryDir)
    val data = DiffInDiffSummaryData(
      summary.treatmentEffect,
      summary.standardError,
      summary.timeWeights.isDefined,
      summary.timeIntercept,
      summary.timeRMSE,
      summary.unitWeights.isDefined,
      summary.unitIntercept,
      summary.unitRMSE,
      summary.zeta,
      summary.lossHistoryTimeWeights,
      summary.lossHistoryUnitWeights
    )
    spark.createDataset(Seq(data)).write.parquet(new Path(summaryPath, DataDir).toString)
    summary.timeWeights.foreach(_.write.parquet(new Path(summaryPath, TimeWeightsDir).toString))
    summary.unitWeights.foreach(_.write.parquet(new Path(summaryPath, UnitWeightsDir).toString))
  }

  def load(path: String, spark: SparkSession): Option[DiffInDiffSummary] = {
    val summaryPath = new Path(path, SummaryDir)
    import spark.implicits._

    val data = try {
      Some(spark.read.parquet(new Path(summaryPath, DataDir).toString).as[DiffInDiffSummaryData].head())
    } catch {
      case error: AnalysisException if error.getErrorClass == "PATH_NOT_FOUND" => None
    }
    data.map { data =>
      val timeWeights = if (data.hasTimeWeights) {
        Some(spark.read.parquet(new Path(summaryPath, TimeWeightsDir).toString).as[VectorEntry])
      } else {
        None
      }
      val unitWeights = if (data.hasUnitWeights) {
        Some(spark.read.parquet(new Path(summaryPath, UnitWeightsDir).toString).as[VectorEntry])
      } else {
        None
      }
      DiffInDiffSummary(
        data.treatmentEffect,
        data.standardError,
        timeWeights,
        data.timeIntercept,
        data.timeRMSE,
        unitWeights,
        data.unitIntercept,
        data.unitRMSE,
        data.zeta,
        data.lossHistoryTimeWeights.map(_.toList),
        data.lossHistoryUnitWeights.map(_.toList)
      )
    }
  }
}

class DiffInDiffModel(override val uid: String)
  extends Model[DiffInDiffModel]
    with HasUnitCol
    with HasTimeCol
    with Wrappable
    with ComplexParamsWritable
    with SynapseMLLogging {

  logClass(FeatureNames.Causal)

  final val timeIndex = new DataFrameParam(this, "timeIndex", "time index")
  def getTimeIndex: DataFrame = $(timeIndex)
  def setTimeIndex(value: DataFrame): this.type = set(timeIndex, value)

  final val timeIndexCol = new Param[String](this, "timeIndexCol", "time index column")
  def getTimeIndexCol: String = $(timeIndexCol)
  def setTimeIndexCol(value: String): this.type = set(timeIndexCol, value)

  final val unitIndex = new DataFrameParam(this, "unitIndex", "unit index")
  def getUnitIndex: DataFrame = $(unitIndex)
  def setUnitIndex(value: DataFrame): this.type = set(unitIndex, value)

  final val unitIndexCol = new Param[String](this, "unitIndexCol", "unit index column")
  def getUnitIndexCol: String = $(unitIndexCol)
  def setUnitIndexCol(value: String): this.type = set(unitIndexCol, value)

  override protected lazy val pyInternalWrapper = true

  def this() = this(Identifiable.randomUID("DiffInDiffModel"))

  private final var summary: Option[DiffInDiffSummary] = None

  override def write: MLWriter = {
    val paramsWriter = super.write
    new MLWriter {
      override protected def saveImpl(path: String): Unit = {
        val writer = paramsWriter.session(sparkSession)
        if (shouldOverwrite) {
          writer.overwrite()
        }
        writer.save(path)
        summary.foreach(DiffInDiffSummaryPersistence.save(_, path, sparkSession))
      }
    }
  }

  def getSummary: DiffInDiffSummary = summary.getOrElse {
    throw new SparkException(
      s"No summary available for this ${this.getClass.getSimpleName}")
  }

  private[causal] def setSummary(summary: Option[DiffInDiffSummary]): this.type = {
    this.summary = summary
    this
  }

  override def copy(extra: ParamMap): DiffInDiffModel = {
    copyValues(new DiffInDiffModel(uid), extra)
      .setSummary(this.summary)
      .setParent(parent)
  }

  override def transform(dataset: Dataset[_]): DataFrame = dataset.toDF

  override def transformSchema(schema: StructType): StructType = schema

  def getTimeWeights: Option[DataFrame] = {
    (get(timeIndex), getSummary.timeWeights) match {
      case (Some(idxDf), Some(timeWeights)) =>
        Some(
          idxDf.join(timeWeights, idxDf(getTimeIndexCol) === timeWeights("i"), "left_outer")
            .select(
              idxDf(getTimeCol),
              timeWeights("value")
            )
        )
      case _ =>
        None
    }
  }

  def getUnitWeights: Option[DataFrame] = {
    (get(unitIndex), getSummary.unitWeights) match {
      case (Some(idxDf), Some(unitWeights)) =>
        Some(
          idxDf.join(unitWeights, idxDf(getUnitIndexCol) === unitWeights("i"), "left_outer")
            .select(
              idxDf(getUnitCol),
              unitWeights("value")
            )
        )
      case _ =>
        None
    }
  }
}

object DiffInDiffModel extends ComplexParamsReadable[DiffInDiffModel] {
  private def complexParamsReader: MLReader[DiffInDiffModel] = super.read

  override def read: MLReader[DiffInDiffModel] = new MLReader[DiffInDiffModel] {
    override def load(path: String): DiffInDiffModel = {
      val model = complexParamsReader.session(sparkSession).load(path)
      model.setSummary(DiffInDiffSummaryPersistence.load(path, sparkSession))
    }
  }
}

trait DiffInDiffEstimatorParams extends Params
  with HasTreatmentCol
  with HasOutcomeCol
  with HasPostTreatmentCol
