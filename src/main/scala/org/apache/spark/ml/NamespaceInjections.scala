package org.apache.spark.ml

import org.apache.spark.ml.image.ImageSchema
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Row

object NamespaceInjections {

  def pipelineModel[T <: Transformer](stages: Array[T]): PipelineModel = {
    new PipelineModel(Identifiable.randomUID("PipelineModel"), stages.asInstanceOf[Array[Transformer]])
  }

}

object ImageInjections {

  def decode(origin: String, bytes: Array[Byte]): Option[Row] = ImageSchema.decode(origin, bytes)

}

