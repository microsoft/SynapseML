package com.microsoft.azure.synapse.ml.causal

import com.microsoft.azure.synapse.ml.causal.linalg.DVector
import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.param.DataFrameParam
import org.apache.spark.SparkException
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, Estimator, Model}
import org.apache.spark.sql.types.{BooleanType, NumericType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

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

  private[causal] val interactionCol = "interaction"

  private[causal] def fitLinearModel(df: DataFrame, weightCol: Option[String] = None) = {
    val assembler = new VectorAssembler()
      .setInputCols(Array(getPostTreatmentCol, getTreatmentCol, interactionCol))
      .setOutputCol("features")

    val regression = weightCol
      .map(new LinearRegression().setWeightCol)
      .getOrElse(new LinearRegression())

    regression
      .setFeaturesCol("features")
      .setLabelCol(getOutcomeCol)
      .setFitIntercept(true)
      .setLoss("squaredError")
      .setRegParam(0.0)

    assembler.transform _ andThen regression.fit apply df
  }
}

case class DiffInDiffSummary(treatmentEffect: Double, standardError: Double,
                             timeWeights: Option[DVector] = None,
                             timeIntercept: Option[Double] = None,
                             unitWeights: Option[DVector] = None,
                             unitIntercept: Option[Double] = None)

class DiffInDiffModel(override val uid: String)
  extends Model[DiffInDiffModel]
    with HasUnitCol
    with HasTimeCol
    with Wrappable
    with ComplexParamsWritable {

  final val timeIndex = new DataFrameParam(this, "timeIndex", "time index")
  def getTimeIndex: DataFrame = $(timeIndex)
  def setTimeIndex(value: DataFrame): this.type = set(timeIndex, value)

  final val unitIndex = new DataFrameParam(this, "unitIndex", "unit index")
  def getUnitIndex: DataFrame = $(unitIndex)
  def setUnitIndex(value: DataFrame): this.type = set(unitIndex, value)

  override protected lazy val pyInternalWrapper = true

  def this() = this(Identifiable.randomUID("did"))

  private final var summary: Option[DiffInDiffSummary] = None

  def getSummary: DiffInDiffSummary = summary.getOrElse {
    throw new SparkException(
      s"No summary available for this ${this.getClass.getSimpleName}")
  }

  private[causal] def setSummary(summary: Option[DiffInDiffSummary]): this.type = {
    this.summary = summary
    this
  }

  override def copy(extra: ParamMap): DiffInDiffModel = {
    copyValues(new DiffInDiffModel(uid))
      .setSummary(this.summary)
      .setParent(parent)
  }

  override def transform(dataset: Dataset[_]): DataFrame = dataset.toDF

  override def transformSchema(schema: StructType): StructType = schema

  def getTimeWeights: Option[DataFrame] = {
    (get(timeIndex), getSummary.timeWeights) match {
      case (Some(idxDf), Some(timeWeights)) =>
        Some(
          idxDf.join(timeWeights, idxDf(SyntheticEstimator.TimeIdxCol) === timeWeights("i"), "left_outer")
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
          idxDf.join(unitWeights, idxDf(SyntheticEstimator.UnitIdxCol) === unitWeights("i"), "left_outer")
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

object DiffInDiffModel extends ComplexParamsReadable[DiffInDiffModel]