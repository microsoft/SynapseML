package com.microsoft.ml.spark.explainers

import com.microsoft.ml.spark.core.test.base.TestBase
import com.microsoft.ml.spark.core.test.fuzzing.{ExperimentFuzzing, PyTestFuzzing, TestObject}
import com.microsoft.ml.spark.image.NetworkUtils
import org.apache.spark.ml.linalg.{Vector => SV}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.DataFrame
import com.microsoft.ml.spark.explainers.BreezeUtils._

abstract class TextExplainersSuite extends TestBase with NetworkUtils {

  import spark.implicits._

  val model: PipelineModel = {
    val df: DataFrame = Seq(
      ("hi this is example 1", 1.0),
      ("hi this is cat 1", 0.0),
      ("hi this is example 1", 1.0),
      ("foo this is example 1", 1.0),
      ("hi this is example 1", 1.0),
      ("hi this is cat 1", 0.0),
      ("hi this is example 1", 1.0),
      ("hi this is example 1", 1.0),
      ("hi this is example 1", 1.0),
      ("hi bar is cat 1", 0.0),
      ("hi this is example 1", 1.0)
    ) toDF("text", "label")

    val tok: Tokenizer = new Tokenizer().setInputCol("text").setOutputCol("tokens")
    val si: HashingTF = new HashingTF().setInputCol("tokens").setOutputCol("features")
    val lr: LogisticRegression = new LogisticRegression()
      .setFeaturesCol("features").setLabelCol("label").setProbabilityCol("prob")

    val textClassifier: Pipeline = new Pipeline().setStages(Array(tok, si, lr))

    textClassifier.fit(df)
  }

  val shap: TextSHAP = LocalExplainer.KernelSHAP.text
    .setModel(model)
    .setInputCol("text")
    .setTargetCol("prob")
    .setTargetClasses(Array(1))
    .setOutputCol("weights")
    .setTokensCol("tokens")
    .setNumSamples(100)

  val lime: TextLIME = LocalExplainer.LIME.text
    .setModel(model)
    .setInputCol("text")
    .setTargetCol("prob")
    .setTargetClasses(Array(1))
    .setOutputCol("weights")
    .setTokensCol("tokens")
    .setSamplingFraction(0.7)
    .setNumSamples(1000)

  val infer: DataFrame = Seq(
    ("hi this is example 1", 1.0),
    ("hi bar is cat 1", 0.0)
  ) toDF("text", "label")
}

class TextSHAPSuite extends TextExplainersSuite
  with ExperimentFuzzing[TextSHAP]
  with PyTestFuzzing[TextSHAP] {

  import spark.implicits._

  test("TextSHAP can explain a model locally") {
    val results = shap.transform(infer).select("tokens", "weights", "r2")
      .as[(Seq[String], Seq[SV], SV)]
      .collect()
      .map {
        case (tokens, shapValues, r2) => (tokens(3), shapValues.head.toBreeze, r2(0))
      }

    results.foreach {
      case (_, _, r2) =>
        assert(math.abs(1 - r2) < 1e-5)
    }

    // Sum of shap values should match predicted value
    results.foreach {
      case (token, shapValues, _) if token == "example" =>
        assert(math.abs(1 - breeze.linalg.sum(shapValues)) < 1e-5)
        assert(shapValues(4) > 0.29)
      case (token, shapValues, _) if token == "cat" =>
        assert(math.abs(breeze.linalg.sum(shapValues)) < 1e-5)
        assert(shapValues(4) < -0.29)
    }
  }

  private lazy val testObjects: Seq[TestObject[TextSHAP]] = Seq(new TestObject(shap, infer))

  override def experimentTestObjects(): Seq[TestObject[TextSHAP]] = testObjects

  override def pyTestObjects(): Seq[TestObject[TextSHAP]] = testObjects
}

class TextLIMESuite extends TextExplainersSuite
  with ExperimentFuzzing[TextLIME]
  with PyTestFuzzing[TextLIME] {

  import spark.implicits._

  test("TextLIME can explain a model locally") {
    val results = lime.transform(infer).select("tokens", "weights", "r2")
      .as[(Seq[String], Seq[SV], SV)]
      .collect()
      .map {
        case (tokens, weights, r2) => (tokens(3), weights.head(3), r2(0))
      }

    results.foreach {
      case (token, weight, r2) if token == "example" =>
        assert(weight > 0.2)
        assert(r2 > 0.6)
      case (token, weight, r2) if token == "cat" =>
        assert(weight < 0)
        assert(r2 > 0.3)
    }
  }

  private lazy val testObjects: Seq[TestObject[TextLIME]] = Seq(new TestObject(lime, infer))

  override def experimentTestObjects(): Seq[TestObject[TextLIME]] = testObjects

  override def pyTestObjects(): Seq[TestObject[TextLIME]] = testObjects
}