// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.causal

import org.apache.commons.math3.distribution.NormalDistribution
import org.apache.commons.math3.stat.descriptive.rank.Percentile
import org.apache.spark.ml.param.{DoubleArrayParam, IntParam, StringArrayParam}

/** Additive persisted plural statistics; singular rawTreatmentEffects is unchanged. */
trait DoubleMLMultipleTreatmentResults extends DoubleMLParams with HasTreatmentCols {
  val rawTreatmentEffectsByIteration = new DoubleArrayParam(this, "rawTreatmentEffectsByIteration",
    "row-major treatment effects, one ordered treatment vector per successful iteration")
  val rawTreatmentEffectCount = new IntParam(this, "rawTreatmentEffectCount", "fitted treatment vector width")
  val fittedTreatmentCols = new StringArrayParam(this, "fittedTreatmentCols", "fitted treatment order snapshot")

  def setRawTreatmentEffectsByIteration(value: Array[Double]): this.type =
    set(rawTreatmentEffectsByIteration, value.clone())

  def setRawTreatmentEffectCount(value: Int): this.type = set(rawTreatmentEffectCount, value)

  def setFittedTreatmentCols(value: Array[String]): this.type = set(fittedTreatmentCols, value.clone())

  private def requirePluralStatistics(): Unit = {
    if (isDefined(treatmentCol) || !isDefined(rawTreatmentEffectsByIteration) || !isDefined(rawTreatmentEffectCount) ||
      !isDefined(fittedTreatmentCols) || !isDefined(treatmentCols)) {
      throw new IllegalStateException("Plural treatment statistics are not available on this model")
    }
  }

  private def checkedRows: Array[Array[Double]] = {
    requirePluralStatistics()
    val width = $(rawTreatmentEffectCount)
    val names = $(fittedTreatmentCols)
    val flat = $(rawTreatmentEffectsByIteration)
    if (width < 2 || names.length != width || !$(treatmentCols).sameElements(names) ||
      flat.isEmpty || flat.length % width != 0 || !flat.forall(java.lang.Double.isFinite)) {
      throw new IllegalStateException("Plural treatment statistics do not match fitted treatment columns and width")
    }
    flat.grouped(width).map(_.clone()).toArray
  }

  def getRawTreatmentEffectCount: Int = {
    checkedRows
    $(rawTreatmentEffectCount)
  }

  def getRawTreatmentEffectsByIteration: Array[Double] = checkedRows.flatten

  def getRawTreatmentEffectRows: Array[Array[Double]] = checkedRows

  def getFittedTreatmentCols: Array[String] = {
    checkedRows
    $(fittedTreatmentCols).clone()
  }

  /** Bootstrap mean in fitted treatment order, not a separate full-sample estimate. */
  def getAvgTreatmentEffects: Array[Double] = {
    val rows = checkedRows
    rows.transpose.map(values => values.sum / values.length)
  }

  /** Per-coordinate normal-bootstrap Wald p-values, without multiplicity adjustment.
    * Uses sample SD of draws, not SD/sqrt(B). Zero or nonfinite SD yields NaN.
    */
  def getPValues: Array[Double] = {
    val rows = checkedRows
    require(rows.length >= 2, "Plural p-values require at least two bootstrap iterations")
    val normal = new NormalDistribution()
    rows.transpose.map { values =>
      val mean = values.sum / values.length
      val variance = values.map(v => (v - mean) * (v - mean)).sum / (values.length - 1)
      val se = math.sqrt(variance)
      if (se == 0 || !java.lang.Double.isFinite(se)) Double.NaN
      else 2 * normal.cumulativeProbability(-math.abs(mean / se))
    }
  }

  /** Coordinate-wise percentile intervals; no simultaneous coverage claim. */
  def getConfidenceIntervals: Array[Array[Double]] = checkedRows.transpose.map { values =>
    val percentile = new Percentile()
    percentile.setData(values)
    Array(percentile.evaluate(100 * (1 - getConfidenceLevel)),
      percentile.evaluate(100 * getConfidenceLevel))
  }
}
