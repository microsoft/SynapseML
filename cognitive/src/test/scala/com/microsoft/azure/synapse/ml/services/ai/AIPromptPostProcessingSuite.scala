// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructType}

import scala.collection.JavaConverters._
import scala.util.Using

class AIPromptPostProcessingSuite extends AIPromptTestBase {
  Seq("csv", "regex").foreach { mode =>
    test(s"lazy $mode postprocessing keeps planned schemas and values") {
      Seq("chat_completions", "responses").foreach { api =>
        val reply = if (api == "chat_completions") {
          """{"choices":[{"index":0,"message":{"role":"assistant","content":"one;two"},"finish_reason":"stop"}]}"""
        } else {
          """{"status":"completed","output":[{"type":"message","role":"assistant",
            |"content":[{"type":"output_text","text":"one;two"}]}]}""".stripMargin
        }
        Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
          val options = if (mode == "csv") Map("delimiter" -> ";") else {
            Map("regex" -> "one;(two)", "regexGroup" -> "1")
          }
          val stage = new AIPrompt().setApiType(api).setUrl(server.url + "/v1").setModel("model")
            .setSubscriptionKey("synthetic-key").setInputCol("text").setOutputCol("answer")
            .setErrorCol("error").setRawResponseCol("raw").setPostProcessingOptions(options)
          val data = spark.createDataFrame(Seq(Row("hello"), Row(Option.empty[String].orNull)).asJava,
            new StructType().add("text", StringType))
          val result = stage.transform(data)
          assert(result.schema == stage.transformSchema(data.schema))
          val rows = result.collect()
          val answered = rows.find(_.getAs[String]("text") == "hello").get
          if (mode == "csv") assert(answered.getAs[Seq[String]]("answer") == Seq("one", "two"))
          else assert(answered.getAs[String]("answer") == "two")
          assert(answered.getAs[Row]("error") == null)
          val missing = rows.find(_.getAs[String]("text") == null).get
          assert(missing.isNullAt(missing.fieldIndex("answer")))
          assert(missing.getAs[Row]("error").getString(0).contains("missing_input"))
          assert(server.requests.size() == 1)
        }
      }
    }
  }
}
