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

  override protected val thisStage: Params = stage

  override def copy(extra: ParamMap): Params = stage.copy(extra)

  override val uid: String = stage.uid
}

object Foo extends App {

  import WrappableExtensions._

  val Config = CodegenConfig(
    rVersion = "1.0.0",
    name = "mmlspark-core",
    packageName = "mmlspark",
    pythonizedVersion = "1.0.0.dev1",
    version = "1.0.0-43-ca7deac7-SNAPSHOT",
    jarName = None,
    topDir = "C:\\code\\mmlspark\\core",
    targetDir = "C:\\code\\mmlspark\\core\\target\\scala-2.12\\sbt-1.0\\")

  new Word2Vec().makePyFile(Config)


}
