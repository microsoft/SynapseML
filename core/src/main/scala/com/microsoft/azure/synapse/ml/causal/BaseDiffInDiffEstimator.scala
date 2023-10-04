package com.microsoft.azure.synapse.ml.causal

import com.microsoft.azure.synapse.ml.causal.linalg.DVector
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

abstract class BaseDiffInDiffEstimator(override val uid: String)
  extends Estimator[DiffInDiffModel]
    with DiffInDiffEstimatorParams {

  override def transformSchema(schema: StructType): StructType = throw new NotImplementedError

  override def copy(extra: ParamMap): Estimator[DiffInDiffModel] = defaultCopy(extra)

  private[causal] val interactionCol = "interaction"

  private[causal] def fitLinearModel(did_data: DataFrame, weightCol: Option[String] = None) = {
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

    assembler.transform _ andThen regression.fit apply did_data
  }
}

case class DiffInDiffSummary(treatmentEffect: Double, standardError: Double,
                             timeWeights: Option[DVector] = None, timeIntercept: Option[Double] = None,
                             unitWeights: Option[DVector] = None, unitIntercept: Option[Double] = None)

class DiffInDiffModel(override val uid: String, val summary: DiffInDiffSummary) extends Model[DiffInDiffModel] {
  override def copy(extra: ParamMap): DiffInDiffModel = defaultCopy(extra)

  override def transform(dataset: Dataset[_]): DataFrame = throw new NotImplementedError

  override def transformSchema(schema: StructType): StructType = throw new NotImplementedError
}