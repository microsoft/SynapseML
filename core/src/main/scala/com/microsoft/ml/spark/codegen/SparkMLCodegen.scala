package com.microsoft.ml.spark.codegen

import com.microsoft.ml.spark.io.http.JSONInputParser
import com.microsoft.ml.spark.stages.EnsembleByKey
import org.apache.spark.SparkContext
import org.apache.spark.ml.{PipelineStage, feature}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.mllib.feature.{Word2VecModel => Word2VecModel2}
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.streaming.StreamingContext

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

object DotnetTest extends App {
  import WrappableExtensions._

  val Config = CodegenConfig(
    rVersion = "1.0.0",
    name = "mmlspark-cognitive",
    packageName = "mmlspark",
    pythonizedVersion = "1.0.0.dev1",
    version = "1.0.0-rc3-154-20479925-SNAPSHOT",
    jarName = None,
    topDir = "D:\\repos\\mmlspark\\core",
    targetDir = "D:\\repos\\mmlspark\\core\\")

  // All dotnet/spark feature samples test
  // Transformers or Estimators
//  new Bucketizer().makeDotnetFile(Config)
//  new CountVectorizer().makeDotnetFile(Config)
//  new FeatureHasher().makeDotnetFile(Config)
//  new HashingTF().makeDotnetFile(Config)
//  new IDF().makeDotnetFile(Config)
//  new NGram().makeDotnetFile(Config)
//  new SQLTransformer().makeDotnetFile(Config)
//  new StopWordsRemover().makeDotnetFile(Config)
//  new Tokenizer().makeDotnetFile(Config)
//  new Word2Vec().makeDotnetFile(Config)
//  // Models
//  new CountVectorizerModel(Array("hello", "I", "AM", "TO", "TOKENIZE")).makeDotnetFile(Config)
//  new Word2VecModel().makeDotnetFile(Config)
//  new IDFModel().makeDotnetFile(Config)

  new VectorAssembler().makeDotnetFile(Config)

  // Estimator example
//  new Word2Vec().makeDotnetFile(Config)
//  // Transformer example
//  new FeatureHasher().makeDotnetFile(Config)
//  // UnaryTransformer example
//  new Tokenizer().makeDotnetFile(Config)
//  // Predictor example
//  new LinearRegression().makeDotnetFile(Config)
  // Model example
//  new LinearRegressionModel().makeDotnetFile(Config)
//  new Word2Vec().makePyFile(Config)
//  new EnsembleByKey().makeDotnetFile(Config)
//  new JSONInputParser().makeDotnetFile(Config)
}
