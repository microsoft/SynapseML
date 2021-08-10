package com.microsoft.ml.spark.codegen

import com.microsoft.ml.spark.featurize.text.MultiNGram
import com.microsoft.ml.spark.io.http.JSONInputParser
import com.microsoft.ml.spark.recommendation.RankingTrainValidationSplitModel
import com.microsoft.ml.spark.stages.{EnsembleByKey, TextPreprocessor}
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

//  // Test missing types fix
//  // MapParam[String, Int]
//  new EnsembleByKey().makeDotnetFile(Config)
//  // MapParam[String, String]
//  new JSONInputParser().makeDotnetFile(Config)
//  new TextPreprocessor().makeDotnetFile(Config)
//  // TypedArrayParam
//  new MultiNGram().makeDotnetFile(Config)
//  // ModelParam
//  new RankingTrainValidationSplitModel().makeDotnetFile(Config)

  // All dotnet/spark feature samples test
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
  //  new CountVectorizerModel(Array("hello", "I", "AM", "TO", "TOKENIZE")).makeDotnetFile(Config)
  //  new VectorAssembler().makeDotnetFile(Config)
  //  new LinearRegression().makeDotnetFile(Config)
}
