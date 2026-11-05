// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.services.ai.{AILiveTestEnvironment, AIPrompt, NativeRequestFixtures}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructType}
import spray.json._

import scala.collection.JavaConverters._

/** Opt-in inference against an existing deployment; never provisions or changes resources. */
class OpenAIDecisionsLiveSuite extends TestBase {
  test("live Decisions supports typed questions through direct and facade APIs") {
    val endpoint = AILiveTestEnvironment.get("SYNAPSEML_DECISIONS_ENDPOINT")
    val model = AILiveTestEnvironment.get("SYNAPSEML_DECISIONS_MODEL")
    val token = AILiveTestEnvironment.get("SYNAPSEML_DECISIONS_TOKEN")
    val key = AILiveTestEnvironment.get("SYNAPSEML_DECISIONS_KEY")
    assume(endpoint.isDefined && model.isDefined && (token.isDefined || key.isDefined),
      "Live Decisions endpoint, model and credentials are not configured")
    val input = spark.createDataFrame(Seq(
      Row("Checkout is down again for every customer. This is my third request.")).asJava,
      new StructType().add("ticket", StringType))
    Seq(new OpenAIDecisions(), new AIPrompt().setApiType("decisions")).foreach { stage =>
      stage.setUrl(endpoint.get).setModel(model.get).setInputCol("ticket")
        .setQuestions(NativeRequestFixtures.DecisionQuestions).setOutputCol("decision")
        .setErrorCol("error").setUsageCol("usage").setRawResponseCol("raw")
      token match {
        case Some(value) => stage.setAADToken(value)
        case None => key.foreach(stage.setSubscriptionKey)
      }
      val result = stage.transform(input)
      assert(result.schema == stage.transformSchema(input.schema))
      val row = result.head()
      assert(row.isNullAt(row.fieldIndex("error")),
        "Live Decisions request failed; inspect the protected error channel")
      val decision = row.getAs[Row]("decision")
      val answers = decision.getAs[Seq[Row]]("answers")
      assert(answers.map(_.getAs[String]("name")) == Seq("repeat_contact", "team", "severity"))
      assert(answers.map(_.getAs[String]("type")) == Seq("predicate", "choice", "score"))
      val probability = answers.head.getAs[Double]("probability")
      assert(probability >= 0 && probability <= 1)
      val choice = answers(1).getAs[Row]("choice")
      assert(choice.getAs[String]("type") == "string")
      assert(Set("billing", "engineering")(choice.getAs[String]("stringValue")))
      val score = answers(2).getAs[Double]("score")
      assert(score >= 0 && score <= 2)
      val raw = row.getAs[String]("raw").parseJson.asJsObject
      assert(raw.fields("answers").asInstanceOf[JsArray].elements.size == answers.size)
      assert(decision.getAs[String]("model").nonEmpty)
      assert(!row.isNullAt(row.fieldIndex("usage")))
    }
  }
}
