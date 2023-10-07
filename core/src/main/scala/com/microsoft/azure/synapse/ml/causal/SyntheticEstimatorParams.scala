package com.microsoft.azure.synapse.ml.causal

import org.apache.spark.ml.param.{IntParam, LongParam, Param, ParamValidators, Params}
import org.apache.spark.ml.param.shared.{HasMaxIter, HasStepSize, HasTol}

import scala.util.Random

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

  def getNumIterNoChange: Int = $(numIterNoChange)

  def setNumIterNoChange(value: Int): this.type = set(numIterNoChange, value)

  /**
    * Param for deciding whether to collect part of data on driver node and solve the constrained least square problems
    * locally on driver.
    * @group expertParam
    */
  final val localSolverThreshold = new LongParam(this, "localSolverThreshold",
    "threshold for using local solver on driver node. Local solver is faster but relies on part of data " +
      "being collected on driver node.", ParamValidators.gt(0))

  /** @group expertGetParam */
  def getLocalSolverThreshold: Long = $(localSolverThreshold)

  /** @group expertGetParam */
  def setLocalSolverThreshold(value: Long): this.type = set(localSolverThreshold, value)

  def setMaxIter(value: Int): this.type = set(maxIter, value)

  def setStepSize(value: Double): this.type = set(stepSize, value)

  def setTol(value: Double): this.type = set(tol, value)

  setDefault(
    stepSize -> 0.5,
    tol -> 1E-3,
    maxIter -> 100,
    handleMissingOutcome -> "zero",
    localSolverThreshold -> 1000 * 1000
  )
}
