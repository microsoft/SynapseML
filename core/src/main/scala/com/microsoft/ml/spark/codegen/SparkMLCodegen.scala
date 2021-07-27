package com.microsoft.ml.spark.codegen

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.param.{ParamMap, Params}

import java.io.Serializable
import scala.language.implicitConversions

object WrappableExtensions {
  implicit def toWrappable(in: PipelineStage): WrappablePipelineStage = new WrappablePipelineStage(in)

  implicit def fromWrappable(in: WrappablePipelineStage): PipelineStage = in.stage
}

class WrappablePipelineStage(val stage: PipelineStage) extends Wrappable {
  import stage._

  override def copy(extra: ParamMap): Params = stage.copy(extra)

  override val uid: String = stage.uid
}

object Foo extends App {

  import WrappableExtensions._

  new Word2Vec().makePyFile()


}
