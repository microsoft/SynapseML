// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.recommendation

import com.github.fommil.netlib.{BLAS => NetlibBLAS}
import com.microsoft.ml.spark.Wrappable
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasLabelCol, HasPredictionCol, HasSeed}
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.types._
import org.apache.spark.util.ThreadUtils

trait RecommendationParams extends Wrappable with ALSParams

trait TrainValidRecommendSplitParams extends Wrappable with HasSeed {
  /**
    * Param for ratio between train and validation data. Must be between 0 and 1.
    * Default: 0.75
    *
    * @group param
    */

  val trainRatio: DoubleParam = new DoubleParam(this, "trainRatio",
    "ratio between training set and validation set (>= 0 && <= 1)", ParamValidators.inRange(0, 1))

  /** @group getParam */
  def getTrainRatio: Double = $(trainRatio)

  /** @group getParam */
  def getEstimatorParamMaps: Array[ParamMap] = $(estimatorParamMaps)

  val estimatorParamMaps: ArrayParamMapParam =
    new ArrayParamMapParam(this, "estimatorParamMaps", "param maps for the estimator")

  /** @group getParam */
  def getEvaluator: Evaluator = $(evaluator)

  val evaluator: EvaluatorParam = new EvaluatorParam(this, "evaluator",
    "evaluator used to select hyper-parameters that maximize the validated metric")

  /** @group getParam */
  def getEstimator: Estimator[_ <: Model[_]] = $(estimator)

  val estimator = new EstimatorParam(this, "estimator", "estimator for selection")

  setDefault(trainRatio -> 0.75)

  val validationMetrics = new ArrayParam(
    this, "validationMetrics", "Which metrics to validate the models with")

  /** @group getParam */
  def getValidationMetrics: Array[String] = $(validationMetrics).map(_.toString)

  protected def transformSchemaImpl(schema: StructType): StructType = {
    require($(estimatorParamMaps).nonEmpty, s"Validator requires non-empty estimatorParamMaps")
    val firstEstimatorParamMap = $(estimatorParamMaps).head
    val est = $(estimator)
    for (paramMap <- $(estimatorParamMaps).tail) {
      est.copy(paramMap).transformSchema(schema)
    }
    est.copy(firstEstimatorParamMap).transformSchema(schema)
  }

  /**
    * Instrumentation logging for tuning params including the inner estimator and evaluator info.
    */
  protected def logTuningParams(instrumentation: Instrumentation[_]): Unit = {
    instrumentation.logNamedValue("estimator", $(estimator).getClass.getCanonicalName)
    instrumentation.logNamedValue("evaluator", $(evaluator).getClass.getCanonicalName)
    instrumentation.logNamedValue("estimatorParamMapsLength", Int.int2long($(estimatorParamMaps).length))
  }
}

private[ml] object TrainValidRecommendSplitParams {
  /**
    * Check that [[TrainValidRecommendSplitParams.evaluator]] and
    * [[TrainValidRecommendSplitParams.estimator]] are Writable.
    * This does not check [[TrainValidRecommendSplitParams.estimatorParamMaps]].
    */
  def validateParams(instance: TrainValidRecommendSplitParams): Unit = {
    def checkElement(elem: Params, name: String): Unit = elem match {
      case stage: MLWritable => // good
      case other =>
        throw new UnsupportedOperationException(instance.getClass.getName + " write will fail " +
          s" because it contains $name which does not implement Writable." +
          s" Non-Writable $name: ${other.uid} of type ${other.getClass}")
    }

    checkElement(instance.getEvaluator, "evaluator")
    checkElement(instance.getEstimator, "estimator")
    // Check to make sure all Params apply to this estimator.  Throw an error if any do not.
    // Extraneous Params would cause problems when loading the estimatorParamMaps.
    val uidToInstance: Map[String, Params] = MetaAlgorithmReadWrite.getUidMap(instance)
    instance.getEstimatorParamMaps.foreach { case pMap: ParamMap =>
      pMap.toSeq.foreach { case ParamPair(p, v) =>
        require(uidToInstance.contains(p.parent), s"ValidatorParams save requires all Params in" +
          s" estimatorParamMaps to apply to this ValidatorParams, its Estimator, or its" +
          s" Evaluator. An extraneous Param was found: $p")
      }
    }
  }

}

trait RecEvaluatorParams extends Wrappable
  with HasPredictionCol with HasLabelCol with ComplexParamsWritable

object SparkHelpers {

  def getThreadUtils(): ThreadUtils.type = {
    ThreadUtils
  }
}

