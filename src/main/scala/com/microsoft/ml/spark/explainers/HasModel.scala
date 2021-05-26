package com.microsoft.ml.spark.explainers

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Params, TransformerParam}

trait HasModel extends Params {
  val model = new TransformerParam(this, "model", "The model to be interpreted.")

  def getModel: Transformer = $(model)

  def setModel(v: Transformer): this.type = set(model, v)
}
