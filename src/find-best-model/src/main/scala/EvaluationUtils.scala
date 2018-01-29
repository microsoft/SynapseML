// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import com.microsoft.ml.spark.schema.SchemaConstants
import org.apache.spark.ml.classification.{ClassificationModel, Classifier, ProbabilisticClassificationModel}
import org.apache.spark.ml.{Estimator, PipelineStage, RegressionUtils, Transformer}
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.regression._

trait HasEvaluationMetric extends Wrappable {

  /** Metric to evaluate the models with. Default is "all"
    *
    * The metrics that can be chosen are:
    *
    *   For Binary Classifiers:
    *     - AreaUnderROC
    *     - AUC
    *     - accuracy
    *     - precision
    *     - recall
    *
    *   For Regression Classifiers:
    *     - mse
    *     - rmse
    *     - r2
    *     - mae
    *
    *   Or, for either type of classifier:
    *     - all - This will report all the relevant metrics
    *
    * @group param
    */
  val evaluationMetric: Param[String] = StringParam(this, "evaluationMetric", "Metric to evaluate models with",
    (s: String) => Set(ComputeModelStatistics.MseSparkMetric,
      ComputeModelStatistics.RmseSparkMetric,
      ComputeModelStatistics.R2SparkMetric,
      ComputeModelStatistics.MaeSparkMetric,
      ComputeModelStatistics.AccuracySparkMetric,
      ComputeModelStatistics.PrecisionSparkMetric,
      ComputeModelStatistics.RecallSparkMetric,
      ComputeModelStatistics.AucSparkMetric) contains s)
  /** @group getParam */
  def getEvaluationMetric: String = $(evaluationMetric)
  /** @group setParam */
  def setEvaluationMetric(value: String): this.type = set(evaluationMetric, value)

  // Set default evaluation metric to accuracy
  setDefault(evaluationMetric -> ComputeModelStatistics.AccuracySparkMetric)

}

object EvaluationUtils {
  val modelTypeUnsupportedErr = "Model type not supported for evaluation"
  // Find type of trained models
  def getModelType(model: PipelineStage):String = {
    model match {
      case _: TrainRegressor => SchemaConstants.RegressionKind
      case _: TrainClassifier => SchemaConstants.ClassificationKind
      case _: Classifier[_, _, _] => SchemaConstants.ClassificationKind
      case regressor: PipelineStage if RegressionUtils.isRegressor(regressor) => SchemaConstants.RegressionKind
      case _: DecisionTreeRegressor => SchemaConstants.RegressionKind
      case _: GBTRegressor => SchemaConstants.RegressionKind
      case _: RandomForestRegressor => SchemaConstants.RegressionKind
      case _: TrainedRegressorModel => SchemaConstants.RegressionKind
      case _: TrainedClassifierModel => SchemaConstants.ClassificationKind
      case evm: BestModel => getModelType(evm.getBestModel)
      case _: ClassificationModel[_, _] => SchemaConstants.ClassificationKind
      case _: RegressionModel[_, _] => SchemaConstants.RegressionKind
      case _ => throw new Exception(modelTypeUnsupportedErr)
    }
  }

  def getMetricWithOperator(model: PipelineStage, evaluationMetric: String): (String, Ordering[Double]) = {
    val modelType = getModelType(model)
    getMetricWithOperator(modelType, evaluationMetric)
  }

  def getMetricWithOperator(modelType: String, evaluationMetric: String): (String, Ordering[Double]) = {
    val chooseHighest = Ordering.Double
    val chooseLowest = Ordering.Double.reverse
    val (evaluationMetricColumnName, operator): (String, Ordering[Double]) = modelType match {
      case SchemaConstants.RegressionKind => evaluationMetric match {
        case ComputeModelStatistics.MseSparkMetric  => (ComputeModelStatistics.MseColumnName,  chooseLowest)
        case ComputeModelStatistics.RmseSparkMetric => (ComputeModelStatistics.RmseColumnName, chooseLowest)
        case ComputeModelStatistics.R2SparkMetric   => (ComputeModelStatistics.R2ColumnName,   chooseHighest)
        case ComputeModelStatistics.MaeSparkMetric  => (ComputeModelStatistics.MaeColumnName,  chooseLowest)
        case _ => throw new Exception("Metric is not supported for regressors")
      }
      case SchemaConstants.ClassificationKind => evaluationMetric match {
        case ComputeModelStatistics.AucSparkMetric       => (ComputeModelStatistics.AucColumnName, chooseHighest)
        case ComputeModelStatistics.PrecisionSparkMetric => (ComputeModelStatistics.PrecisionColumnName, chooseHighest)
        case ComputeModelStatistics.RecallSparkMetric    => (ComputeModelStatistics.RecallColumnName, chooseHighest)
        case ComputeModelStatistics.AccuracySparkMetric  => (ComputeModelStatistics.AccuracyColumnName, chooseHighest)
        case _ => throw new Exception("Metric is not supported for classifiers")
      }
      case _ => throw new Exception("Model type not supported for evaluation")
    }
    (evaluationMetricColumnName, operator)
  }

  def getModelParams(model: Transformer): ParamMap = {
    model match {
      case reg: TrainedRegressorModel => reg.getParamMap
      case cls: TrainedClassifierModel => cls.getParamMap
      case evm: BestModel => getModelParams(evm.getBestModel)
      case _ => throw new Exception("Model type not supported for evaluation")
    }
  }

  /** Returns a string representation of the model.
    * @param model The model output by TrainClassifier or TrainRegressor
    * @return A comma delimited representation of the model parameter names and values
    */
  def modelParamsToString(model: Transformer): String =
    getModelParams(model).toSeq.map(pv => s"${pv.param.name}: ${pv.value}").sorted.mkString(", ")

}
