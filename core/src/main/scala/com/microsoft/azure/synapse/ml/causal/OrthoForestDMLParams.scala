// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.causal

import com.microsoft.azure.synapse.ml.core.contracts.HasOutputCol
import org.apache.spark.ml.param.{IntParam, Param, Params}
import org.apache.spark.ml.regression.GBTRegressor

trait HasNumTrees extends Params {
  val numTrees: IntParam = new IntParam(this, "numTrees", "Number of trees")

  def getNumTrees: Int = $(numTrees)

  /**
    * Set number of trees to be used in the forest
    *
    * @group setParam
    */
  def setNumTrees(value: Int): this.type = set(numTrees, value)
}

trait HasMaxDepth extends Params {

  val maxDepth: IntParam = new IntParam(this,
    "maxDepth",
    "Max Depth of Tree")

  def getMaxDepth: Int = $(maxDepth)

  /**
    * Set max depth of the trees to be used in the forest
    *
    * @group setParam
    */
  def setMaxDepth(value: Int): this.type = set(maxDepth, value)
}

trait HasMinSampleLeaf extends Params {
  val minSamplesLeaf: IntParam = new IntParam(this,
    "minSamplesLeaf",
    "Max Depth of Tree")

  def getMinSamplesLeaf: Int = $(minSamplesLeaf)

  /**
    * Set number of samples in the leaf node of trees to be used in the forest
    *
    * @group setParam
    */
  def setMinSamplesLeaf(value: Int): this.type = set(minSamplesLeaf, value)
}

trait OrthoForestDMLParams extends DoubleMLParams
  with HasNumTrees with HasMaxDepth with HasMinSampleLeaf with HasOutputCol {
  val treatmentResidualCol: Param[String] = new Param[String](this,
    "treatmentResidualCol",
    "Treatment Residual Column")

  def getTreatmentResidualCol: String = $(treatmentResidualCol)

  /**
    * Set treatment residual column
    *
    * @group setParam
    */
  def setTreatmentResidualCol(value: String): this.type = set(treatmentResidualCol, value)

  val outcomeResidualCol: Param[String] = new Param[String](this,
    "outcomeResidualCol",
    "Outcome Residual Column")

  def getOutcomeResidualCol: String = $(outcomeResidualCol)

  /**
    * Set outcome residual column
    *
    * @group setParam
    */
  def setOutcomeResidualCol(value: String): this.type = set(outcomeResidualCol, value)


  val heterogeneityVecCol: Param[String] = new Param[String](this,
    "heterogeneityVecCol",
    "Vector to divide the treatment by")

  def getHeterogeneityVecCol: String = $(heterogeneityVecCol)

  /**
    * Set heterogeneity vector column
    *
    * @group setParam
    */
  def setHeterogeneityVecCol(value: String): this.type = set(heterogeneityVecCol, value)


  val confounderVecCol: Param[String] = new Param[String](this,
    "confounderVecCol",
    "Confounders to control for")

  def getConfounderVecCol: String = $(confounderVecCol)

  /**
    * Set confounder vector column
    *
    * @group setParam
    */
  def setConfounderVecCol(value: String): this.type = set(confounderVecCol, value)

  val outputLowCol: Param[String] = new Param[String](this,
    "outputLowCol",
    "Output Confidence Interval Low")

  def getOutputLowCol: String = $(outputLowCol)

  /**
    * Set output column for effect lower bound
    *
    * @group setParam
    */
  def setOutputLowCol(value: String): this.type = set(outputLowCol, value)


  val outputHighCol: Param[String] = new Param[String](this,
    "outputHighCol",
    "Output Confidence Interval Low")

  def getOutputHighCol: String = $(outputHighCol)

  /**
    * Set output column for effect upper bound
    *
    * @group setParam
    */
  def setOutputHighCol(value: String): this.type = set(outputHighCol, value)


  setDefault(
    treatmentModel -> new GBTRegressor().setSeed(0),
    outcomeModel -> new GBTRegressor().setSeed(0),
    treatmentResidualCol -> "TreatmentResidual",
    outcomeResidualCol -> "OutcomeResidual",
    heterogeneityVecCol -> "X",
    confounderVecCol -> "XW",
    outputLowCol -> "EffectLowerBound",
    outputCol -> "EffectAverage",
    outputHighCol -> "EffectUpperBound",
    sampleSplitRatio -> Array(0.5, 0.5),
    confidenceLevel -> 0.975,
    maxDepth -> 5,
    minSamplesLeaf -> 10,
    numTrees -> 20 // Good enough for most CI estimates
  )

}
