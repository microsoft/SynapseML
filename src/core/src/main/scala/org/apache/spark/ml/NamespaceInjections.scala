package org.apache.spark.ml

import org.apache.spark.ml.util.Identifiable

object NamespaceInjections {

  def pipelineModel[T <: Transformer](stages: Array[T]): PipelineModel = {
    new PipelineModel(Identifiable.randomUID("PipelineModel"), stages.asInstanceOf[Array[Transformer]])
  }

}
