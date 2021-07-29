package com.microsoft.ml.spark.codegen

import com.microsoft.ml.spark.io.http.JSONInputParser
import com.microsoft.ml.spark.stages.EnsembleByKey
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.feature.{FeatureHasher, Word2Vec}
import org.apache.spark.ml.param.{ParamMap, Params}

import java.io.Serializable
import scala.language.implicitConversions

object WrappableExtensions {
  implicit def toWrappable(in: PipelineStage): WrappablePipelineStage = new WrappablePipelineStage(in)

  implicit def fromWrappable(in: WrappablePipelineStage): PipelineStage = in.stage
}

class WrappablePipelineStage(val stage: PipelineStage) extends Wrappable with DotnetWrappable {

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
    targetDir = "C:\\code\\mmlspark\\core\\")

  new Word2Vec().makePyFile(Config)


}

object DotNetTest extends App {
  import WrappableExtensions._

  val Config = CodegenConfig(
    rVersion = "1.0.0",
    name = "mmlspark-cognitive",
    packageName = "mmlspark",
    pythonizedVersion = "1.0.0.dev1",
    version = "1.0.0-rc3-154-20479925-SNAPSHOT",
    jarName = None,
    topDir = "D:\\repos\\mmlspark\\cognitive",
    targetDir = "D:\\repos\\mmlspark\\cognitive\\")

  new Word2Vec().makeDotnetFile(Config)
  new FeatureHasher().makeDotnetFile(Config)
//  new Word2Vec().makePyFile(Config)
  new EnsembleByKey().makeDotnetFile(Config)
  new JSONInputParser().makeDotnetFile(Config)
}
