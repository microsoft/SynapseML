package com.microsoft.ml.spark.explainers

import org.apache.spark.ml.param.{IntParam, ParamValidators, Params}

trait HasNumSamples extends Params {
  final val numSamples: IntParam = new IntParam(
    this,
    "numSamples",
    "Number of samples to generate.",
    ParamValidators.gt(0)
  )

  final def getNumSamples: Int = $(numSamples)

  final def setNumSamples(value: Int): this.type = this.set(numSamples, value)
}
