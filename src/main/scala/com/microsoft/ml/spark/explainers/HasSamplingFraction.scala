package com.microsoft.ml.spark.explainers

import org.apache.spark.ml.param.{DoubleParam, ParamValidators, Params}

trait HasSamplingFraction extends Params {
  val samplingFraction = new DoubleParam(
    this,
    "samplingFraction",
    "The fraction of superpixels to keep on",
    ParamValidators.inRange(0, 1)
  )

  def getSamplingFraction: Double = $(samplingFraction)

  def setSamplingFraction(d: Double): this.type = set(samplingFraction, d)
}
