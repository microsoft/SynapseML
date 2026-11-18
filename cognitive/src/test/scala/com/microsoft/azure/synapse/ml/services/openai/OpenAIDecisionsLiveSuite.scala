// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.services.ai.{AILiveTestEnvironment, AIPrompt, NativeRequestFixtures}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructType}
import spray.json._

import java.awt.Color
import java.awt.image.BufferedImage
import java.io.ByteArrayOutputStream
import java.util.Base64
import javax.imageio.ImageIO
import scala.collection.JavaConverters._
import scala.util.Using

/** Opt-in inference against an existing deployment; never provisions or changes resources. */
class OpenAIDecisionsLiveSuite extends TestBase {
  Seq(false, true).foreach { hosted =>
    test(s"live Decisions accepts image inputs and image instructions with hosted=$hosted") {
      val endpoint = AILiveTestEnvironment.get("SYNAPSEML_DECISIONS_ENDPOINT")
      val model = AILiveTestEnvironment.get("SYNAPSEML_DECISIONS_MODEL")
      val token = AILiveTestEnvironment.get("SYNAPSEML_DECISIONS_TOKEN")
      val key = AILiveTestEnvironment.get("SYNAPSEML_DECISIONS_KEY")
      assume(endpoint.isDefined && model.isDefined && (token.isDefined || key.isDefined),
        "Live Decisions endpoint, model and credentials are not configured")
      val hostedUrl = AILiveTestEnvironment.get("SYNAPSEML_DECISIONS_IMAGE_URL")
      if (hosted) assume(hostedUrl.exists(_.startsWith("https://")),
        "Configure SYNAPSEML_DECISIONS_IMAGE_URL with a stable publicly accessible HTTPS test image")
      val inline = Using.resource(new ByteArrayOutputStream()) { output =>
        val image = new BufferedImage(64, 64, BufferedImage.TYPE_INT_RGB)
        for (x <- 0 until 64; y <- 0 until 64) image.setRGB(x, y, Color.BLUE.getRGB)
        assert(ImageIO.write(image, "png", output))
        "data:image/png;base64," + Base64.getEncoder.encodeToString(output.toByteArray)
      }
      def image(url: String): JsValue = JsObject("type" -> JsString("input_image"), "image_url" -> JsString(url))
      val questions = JsArray(JsObject("type" -> JsString("predicate"), "name" -> JsString("same_color"),
        "instructions" -> JsArray(
          JsObject("type" -> JsString("input_text"),
            "text" -> JsString("Does the input share this image's main color?")),
          image(inline)))).compactPrint
      val value = if (hosted) JsArray(JsObject("role" -> JsString("user"),
        "content" -> JsArray(image(hostedUrl.get)))).compactPrint else inline
      val input = spark.createDataFrame(Seq(Row(value)).asJava, new StructType().add("input", StringType))
      Seq(new OpenAIDecisions(), new AIPrompt().setApiType("decisions")).foreach { stage =>
        stage.setUrl(endpoint.get).setModel(model.get).setQuestions(questions)
          .setOutputCol("decision").setErrorCol("error")
        if (hosted) stage.setInputCol("input").setInputMode("json")
        else stage.setPromptTemplate("Inspect {input}").setColumnTypes(Map("input" -> "path")).setFileSizeLimitMB(1)
        token match {
          case Some(value) => stage.setAADToken(value)
          case None => key.foreach(stage.setSubscriptionKey)
        }
        val result = stage.transform(input)
        assert(result.schema == stage.transformSchema(input.schema))
        val row = result.head()
        assert(row.getAs[Row]("error") == null,
          "Live Decisions image request failed; inspect the protected error channel")
        val answers = row.getAs[Row]("decision").getAs[Seq[Row]]("answers")
        assert(answers.size == 1 && answers.head.getAs[String]("name") == "same_color")
        assert(answers.head.getAs[String]("type") == "predicate")
        val probability = answers.head.getAs[Double]("probability")
        assert(probability >= 0 && probability <= 1)
      }
    }
  }

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
