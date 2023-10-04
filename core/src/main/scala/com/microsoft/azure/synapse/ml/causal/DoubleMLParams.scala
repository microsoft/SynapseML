// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.causal

import com.microsoft.azure.synapse.ml.core.contracts.{HasFeaturesCol, HasWeightCol}
import com.microsoft.azure.synapse.ml.param.EstimatorParam
import org.apache.spark.ml.ParamInjections.HasParallelismInjected
import org.apache.spark.ml.classification.{LogisticRegression, ProbabilisticClassifier}
import org.apache.spark.ml.param.shared.HasMaxIter
import org.apache.spark.ml.param.{DoubleArrayParam, DoubleParam, Params}
import org.apache.spark.ml.regression.Regressor
import org.apache.spark.ml.{Estimator, Model}

trait DoubleMLParams extends Params
  with HasTreatmentCol with HasOutcomeCol with HasFeaturesCol
  with HasMaxIter with HasWeightCol with HasParallelismInjected {

  val treatmentModel = new EstimatorParam(this, "treatmentModel", "treatment model to run")
  def getTreatmentModel: Estimator[_ <: Model[_]]  = $(treatmentModel)

  /**
   * Set treatment model, it could be any model derived from
   * 'org.apache.spark.ml.regression.Regressor' or
   * 'org.apache.spark.ml.classification.ProbabilisticClassifier'
   *
   * @group setParam
   */
  def setTreatmentModel(value: Estimator[_ <: Model[_]]): this.type = {
    ensureSupportedEstimator(value)
    set(treatmentModel, value)
  }

  val outcomeModel = new EstimatorParam(this, "outcomeModel", "outcome model to run")
  def getOutcomeModel: Estimator[_ <: Model[_]] = $(outcomeModel)

  /**
   * Set outcome model, it could be any model derived from
   * 'org.apache.spark.ml.regression.Regressor' or
   * 'org.apache.spark.ml.classification.ProbabilisticClassifier'
   *
   * @group setParam
   */
  def setOutcomeModel(value: Estimator[_ <: Model[_]]): this.type = {
    ensureSupportedEstimator(value)
    set(outcomeModel, value)
  }

  val sampleSplitRatio = new DoubleArrayParam(
    this,
    "sampleSplitRatio",
    "Sample split ratio for cross-fitting. Default: [0.5, 0.5].",
    split => split.length == 2 && split.forall(_ >= 0)
  )
  def getSampleSplitRatio: Array[Double] = $(sampleSplitRatio).map(v => v / $(sampleSplitRatio).sum)

  /**
   * Set the sample split ratio, default is Array(0.5, 0.5)
   *
   * @group setParam
   */
  def setSampleSplitRatio(value: Array[Double]): this.type = set(sampleSplitRatio, value)

  private[causal] object DoubleMLModelTypes extends Enumeration {
    type DoubleMLModelTypes = Value
    val Binary, Continuous = Value
  }

  private[causal] def getDoubleMLModelType(model: Any): DoubleMLModelTypes.Value = {
    model match {
      case _: ProbabilisticClassifier[_, _, _] =>
        DoubleMLModelTypes.Binary
      case _: Regressor[_, _, _] =>
        DoubleMLModelTypes.Continuous
      case _ =>
        throw new IllegalArgumentException(s"Invalid model type: ${model.getClass.getName}")
    }
  }

  val confidenceLevel = new DoubleParam(
    this,
    "confidenceLevel",
    "confidence level, default value is 0.975",
    value => value > 0 && value < 1)

  def getConfidenceLevel: Double = $(confidenceLevel)

  /**
   * Set the higher bound percentile of ATE distribution. Default is 0.975.
   * lower bound value will be automatically calculated as 100*(1-confidenceLevel)
   * That means by default we compute 95% confidence interval, it is [2.5%, 97.5%] percentile of ATE distribution
   *
   * @group setParam
   */
  def setConfidenceLevel(value: Double): this.type = set(confidenceLevel, value)

  /**
   * Set the maximum number of confidence interval bootstrapping iterations.
   * Default is 1, which means it does not calculate confidence interval.
   * To get Ci values please set a meaningful value
   *
   * @group setParam
   */
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  def setParallelism(value: Int): this.type = set(parallelism, value)

  setDefault(
    treatmentModel -> new LogisticRegression(),
    outcomeModel -> new LogisticRegression(),
    sampleSplitRatio -> Array(0.5, 0.5),
    confidenceLevel -> 0.975,
    maxIter -> 1,
    parallelism -> 10 // Best practice, a value up to 10 should be sufficient for most clusters.
  )

  private def ensureSupportedEstimator(value: Estimator[_ <: Model[_]]): Unit = {
    value match {
      case _: Regressor[_, _, _] => // for continuous treatment or outcome
      case _: ProbabilisticClassifier[_, _, _]  =>
      case _ => throw new Exception(
        s"DoubleMLEstimator only supports Regressor and ProbabilisticClassifier as treatment or outcome model types, " +
          s"but got type ${value.getClass.getName}"
      )
    }
  }
}
