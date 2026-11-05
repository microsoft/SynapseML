// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import com.microsoft.azure.synapse.ml.io.http.{HTTPRequestData, HTTPSchema, URLKey}
import com.microsoft.azure.synapse.ml.param.GlobalParams
import com.microsoft.azure.synapse.ml.services.OpenAISubscriptionKey
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.StructType
import spray.json.{JsArray, JsString}

import java.util.Collections

/** Test-jar bridge for managed-runtime routing without external inference or storage. */
object AIPromptFabricProbe {
  def verifyImplicitEndpoints(spark: SparkSession): String = verifyImplicitEndpoints(spark, () => new AIPrompt())

  private[ai] def verifyImplicitEndpoints(spark: SparkSession,
                                          create: () => AIPrompt): String = AIPrompt.withSession(spark) {
    withoutGlobalDefaults {
      val data = spark.createDataFrame(Collections.singletonList(Row("synthetic-model")),
        new StructType().add("text", "string"))
      val apis = Vector("chat_completions", "responses", "embeddings")
      apis.foreach { api =>
        val reply = api match {
          case "chat_completions" =>
            """{"choices":[{"index":0,"message":{"role":"assistant","content":"ok"}}]}"""
          case "responses" =>
            """{"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}]}"""
          case "embeddings" => """{"data":[{"index":0,"embedding":[0.5]}]}"""
        }
        val stage = create().setApiType(api).setModelCol("text").setInputCol("text")
          .setOutputCol("answer").setErrorCol("error")
        val effective = stage.effectiveCopy()
        val root = effective.getUrl.stripSuffix("/") + "/"
        require(effective.usingImplicitFabricEndpoint(root), "Expected an implicit Fabric route")
        stage.setHandler { (_, request) =>
          require(request.requestLine.uri.startsWith(root), "Request did not use the implicit Fabric route")
          val headers = request.headers.map(h => h.name.toLowerCase(java.util.Locale.ROOT) -> h.value).toMap
          require(headers.get("authorization").exists(_.trim.nonEmpty), "Expected runtime authorization")
          require(request.headers.exists(_.name == HTTPRequestData.FabricAuthMarkerHeader),
            "Expected Fabric fallback authentication")
          require(headers.get("x-taxonomy-traffictype").contains("Background") &&
            headers.get("x-llm-service-tier").contains("flex"), "Expected Fabric workload classification")
          HTTPSchema.stringToResponse(reply, 200, "OK")
        }
        val output = stage.transform(data)
        require(output.schema == stage.transformSchema(data.schema), "Implicit route schema changed")
        val row = output.head()
        require(row.getAs[Row]("error") == null && !row.isNullAt(row.fieldIndex("answer")),
          "Implicit route did not preserve the synthetic answer")
        require(!stage.isDefined(stage.url), "Execution must not persist the implicit URL")
      }
      require(GlobalParams.getGlobalParam(URLKey).isEmpty, "Execution must not change the global URL")
      require(GlobalParams.getGlobalParam(OpenAISubscriptionKey).isEmpty,
        "Execution must not change the global subscription key")
      JsArray(apis.map(JsString(_))).compactPrint
    }
  }

  private def withoutGlobalDefaults[T](action: => T): T = {
    val previousUrl = GlobalParams.getGlobalParam(URLKey)
    val previousKey = GlobalParams.getGlobalParam(OpenAISubscriptionKey)
    try {
      GlobalParams.resetGlobalParam(URLKey)
      GlobalParams.resetGlobalParam(OpenAISubscriptionKey)
      action
    } finally {
      previousUrl match {
        case Some(value) => GlobalParams.setGlobalParam(URLKey, value)
        case None => GlobalParams.resetGlobalParam(URLKey)
      }
      previousKey match {
        case Some(value) => GlobalParams.setGlobalParam(OpenAISubscriptionKey, value)
        case None => GlobalParams.resetGlobalParam(OpenAISubscriptionKey)
      }
    }
  }
}
