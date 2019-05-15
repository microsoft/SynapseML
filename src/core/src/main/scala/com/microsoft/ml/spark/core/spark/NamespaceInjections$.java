object NamespaceInjections {

  def pipelineModel[T <: Transformer](stages: Array[T]): PipelineModel = {
    new PipelineModel(Identifiable.randomUID("PipelineModel"), stages.asInstanceOf[Array[Transformer]])
  }

}
