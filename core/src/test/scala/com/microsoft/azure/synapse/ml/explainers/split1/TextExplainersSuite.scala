// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.explainers.split1

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import com.microsoft.azure.synapse.ml.core.utils.BreezeUtils._
import com.microsoft.azure.synapse.ml.explainers.LocalExplainer.{KernelSHAP, LIME}
import com.microsoft.azure.synapse.ml.explainers.{TextLIME, TextSHAP}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.DataFrame

abstract class TextExplainersSuite extends TestBase {

  import spark.implicits._

  lazy val model: PipelineModel = {
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

  lazy val shap: TextSHAP = KernelSHAP.text
    .setModel(model)
    .setInputCol("text")
    .setTargetCol("prob")
    .setTargetClasses(Array(1))
    .setOutputCol("weights")
    .setTokensCol("tokens")
    .setNumSamples(1000)

  lazy val lime: TextLIME = LIME.text
    .setModel(model)
    .setInputCol("text")
    .setTargetCol("prob")
    .setTargetClasses(Array(1))
    .setOutputCol("weights")
    .setTokensCol("tokens")
    .setSamplingFraction(0.7)
    .setNumSamples(1000)

  lazy val infer: DataFrame = Seq(
    ("hi this is example 1", 1.0),
    ("hi bar is cat 1", 0.0)
  ) toDF("text", "label")
}

class TextSHAPExplainerSuite extends TextExplainersSuite
  with TransformerFuzzing[TextSHAP] {

  import spark.implicits._

  test("TextSHAP can explain a model locally") {
    val results = shap.transform(infer).select("tokens", "weights", "r2")
      .as[(Seq[String], Seq[Vector], Vector)]
      .collect()
      .map {
        case (tokens, shapValues, r2) => (tokens(3), shapValues.head.toBreeze, r2(0))
      }

    results.foreach {
      case (_, _, r2) =>
        assert(r2 === 1d)
    }

    // Sum of shap values should match predicted value
    results.foreach {
      case (token, shapValues, _) if token == "example" =>
        assert(breeze.linalg.sum(shapValues) === 1d)
        assert(shapValues(4) > 0.27)
      case (token, shapValues, _) if token == "cat" =>
        assert(breeze.linalg.sum(shapValues) === 0d)
        assert(shapValues(4) < -0.0)
    }
  }

  override def testObjects(): Seq[TestObject[TextSHAP]] = Seq(new TestObject(shap, infer))

  override def reader: MLReadable[_] = TextSHAP
}

class TextLIMEExplainerSuite extends TextExplainersSuite
  with TransformerFuzzing[TextLIME] {

  import spark.implicits._

  test("TextLIME can explain a model locally") {
    val results = lime.transform(infer).select("tokens", "weights", "r2")
      .as[(Seq[String], Seq[Vector], Vector)]
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


  override def testObjects(): Seq[TestObject[TextLIME]] = Seq(new TestObject(lime, infer))

  override def reader: MLReadable[_] = TextLIME
}
