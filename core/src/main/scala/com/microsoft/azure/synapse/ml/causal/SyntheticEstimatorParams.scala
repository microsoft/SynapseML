package com.microsoft.azure.synapse.ml.causal

import org.apache.spark.ml.param.{IntParam, Param, ParamValidators, Params}
import org.apache.spark.ml.param.shared.{HasMaxIter, HasStepSize, HasTol}

trait SyntheticEstimatorParams extends Params
  with HasUnitCol
  with HasTimeCol
  with HasMaxIter
  with HasStepSize
  with HasTol {

  protected val supportedMissingOutcomes: Array[String] = Array("skip", "zero", "impute")
  final val handleMissingOutcome = new Param[String](this, "handleMissingOutcome",
    "How to handle missing outcomes. Options are skip (which will filter out units with missing outcomes), " +
      "zero (fill in missing outcomes with zero), or impute (impute with nearest available outcomes, " +
      "or mean if two nearest outcomes are available)",
    ParamValidators.inArray(supportedMissingOutcomes))

  def getHandleMissingOutcome: String = $(handleMissingOutcome)

  def setHandleMissingOutcome(value: String): this.type = set(handleMissingOutcome, value)

  final val numIterNoChange = new IntParam(this, "numIterNoChange",
    "Early termination when number of iterations without change reached.", ParamValidators.gt(0))

  def getNumIterNoChange: Option[Int] = get(numIterNoChange)

  def setNumIterNoChange(value: Int): this.type = set(numIterNoChange, value)

  def setMaxIter(value: Int): this.type = set(maxIter, value)

  def setStepSize(value: Double): this.type = set(stepSize, value)

  def setTol(value: Double): this.type = set(tol, value)

  setDefault(
    stepSize -> 0.5,
    tol -> 1E-3,
    maxIter -> 100,
    handleMissingOutcome -> "zero"
  )
}
