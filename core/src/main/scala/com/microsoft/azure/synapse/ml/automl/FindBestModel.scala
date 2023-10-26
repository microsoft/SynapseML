// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.automl

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.contracts.HasEvaluationMetric
import com.microsoft.azure.synapse.ml.core.metrics.MetricConstants
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import com.microsoft.azure.synapse.ml.param.{DataFrameParam, TransformerArrayParam, TransformerParam}
import com.microsoft.azure.synapse.ml.train.ComputeModelStatistics
import org.apache.spark.ml._
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.collection.JavaConverters._

object FindBestModel extends ComplexParamsReadable[FindBestModel] {
  val ModelNameCol = "model_name"
  val MetricsCol = "metric"
  val ParamsCol = "parameters"
}

trait FindBestModelParams extends Wrappable with ComplexParamsWritable with HasEvaluationMetric {
  /** Metric to evaluate the models with. Default is "accuracy"
    *
    * The metrics that can be chosen are:
    *
    * For Binary Classifiers:
    *     - AreaUnderROC
    *     - AUC
    *     - accuracy
    *     - precision
    *     - recall
    *
    * For Regression Classifiers:
    *     - mse
    *     - rmse
    *     - r2
    *     - mae
    *
    * Or, for either type of classifier:
    *     - all - This will report all the relevant metrics
    *
    * @group param
    */
  setDefault(evaluationMetric -> MetricConstants.AccuracySparkMetric)
}

/** Evaluates and chooses the best model from a list of models. */
class FindBestModel(override val uid: String) extends Estimator[BestModel]
  with FindBestModelParams with SynapseMLLogging {
  logClass(FeatureNames.AutoML)

  def this() = this(Identifiable.randomUID("FindBestModel"))

  /** List of models to be evaluated. The list is an Array of models
    *
    * @group param
    */
  val models: TransformerArrayParam = new TransformerArrayParam(this, "models", "List of models to be evaluated")

  /** @group getParam */
  def getModels: Array[Transformer] = $(models)

  /** @group setParam */
  def setModels(value: Array[Transformer]): this.type = set(models, value)

  def setModels(value: java.util.ArrayList[Transformer]): this.type = set(models, value.asScala.toArray)

  /** @param dataset - The input dataset, to be fitted
    * @return The Model that results from the fitting
    */
  override def fit(dataset: Dataset[_]): BestModel = {
    logFit({
      import FindBestModel._
      import dataset.sparkSession.implicits._

      // Staging
      val trainedModels = getModels
      if (trainedModels.isEmpty) {
        throw new Exception("No trained models to evaluate.")
      }
      if (!(MetricConstants.FindBestModelMetrics contains getEvaluationMetric)) {
        throw new Exception("Invalid evaluation metric")
      }
      val evaluator = new ComputeModelStatistics()
      evaluator.set(evaluator.evaluationMetric, getEvaluationMetric)

      val firstModel = trainedModels(0)
      val (metricColName, operator) = EvaluationUtils.getMetricWithOperator(firstModel, getEvaluationMetric)
      val models = trainedModels.map(_.uid)
      val parameters = trainedModels.map(EvaluationUtils.modelParamsToString)
      val tdfs = trainedModels.map(_.transform(dataset))
      val metrics = tdfs.map(evaluator.transform(_))
      val simpleMetrics = metrics.map(_.select(metricColName).first()(0).toString.toDouble)

      val bestIndex = simpleMetrics.zipWithIndex.foldLeft(Double.NaN, -1) {
        case (best, curr) =>
          if (best._1.isNaN || operator.lt(best._1, curr._1))
            curr
          else
            best
      }._2
      val bestScoredDf = tdfs(bestIndex)
      evaluator.set(evaluator.evaluationMetric, MetricConstants.AllSparkMetrics)
      val allModelMetrics = (models, simpleMetrics, parameters).zipped.toSeq.toDF(ModelNameCol, MetricsCol, ParamsCol)

      new BestModel(uid)
        .setBestModel(trainedModels(bestIndex))
        .setScoredDataset(bestScoredDf)
        .setBestModelMetrics(evaluator.transform(bestScoredDf))
        .setRocCurve(evaluator.rocCurve)
        .setAllModelMetrics(allModelMetrics)
    }, dataset.columns.length)
  }

  // Choose a random model as we don't know which one will be chosen yet - all will transform schema in same way
  def transformSchema(schema: StructType): StructType = getModels(0).transformSchema(schema)

  def copy(extra: ParamMap): FindBestModel = defaultCopy(extra)

}

trait HasBestModel extends Params {
  val bestModel = new TransformerParam(this, "bestModel", "the best model found")

  /** The best model found during evaluation.
    *
    * @return The best model.
    */
  def getBestModel: Transformer = $(bestModel)

  def setBestModel(v: Transformer): this.type = set(bestModel, v)
}

/** Model produced by [[FindBestModel]]. */
class BestModel(val uid: String) extends Model[BestModel]
  with ComplexParamsWritable with Wrappable with HasBestModel with SynapseMLLogging {
  logClass(FeatureNames.AutoML)

  def this() = this(Identifiable.randomUID("BestModel"))

  val scoredDataset = new DataFrameParam(this, "scoredDataset", "dataset scored by best model")

  /** Gets the scored dataset.
    *
    * @return The scored dataset for the best model.
    */
  def getScoredDataset: DataFrame = $(scoredDataset)

  def setScoredDataset(v: DataFrame): this.type = set(scoredDataset, v)

  val rocCurve = new DataFrameParam(this, "rocCurve", "the roc curve of the best model")

  /** Gets the ROC curve with TPR, FPR.
    *
    * @return The evaluation results.
    */
  def getRocCurve: Dataset[_] = $(rocCurve)

  def setRocCurve(v: DataFrame): this.type = set(rocCurve, v)

  val bestModelMetrics = new DataFrameParam(this, "bestModelMetrics", "the metrics from the best model")

  /** Gets all of the best model metrics results from the evaluator.
    *
    * @return All of the best model metrics results.
    */
  def getBestModelMetrics: Dataset[_] = $(bestModelMetrics)

  def setBestModelMetrics(v: DataFrame): this.type = set(bestModelMetrics, v)

  val allModelMetrics = new DataFrameParam(this, "allModelMetrics", "all model metrics")

  /** Gets a table of metrics from all models compared from the evaluation comparison.
    *
    * @return The model metrics results from all models.
    */
  def getAllModelMetrics: Dataset[_] = $(allModelMetrics)

  def setAllModelMetrics(v: DataFrame): this.type = set(allModelMetrics, v)

  override protected lazy val pyInternalWrapper = true

  override def copy(extra: ParamMap): BestModel = defaultCopy(extra)

  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame](
      getBestModel.transform(dataset), dataset.columns.length
    )
  }

  override def transformSchema(schema: StructType): StructType = getBestModel.transformSchema(schema)

}

object BestModel extends ComplexParamsReadable[BestModel]
