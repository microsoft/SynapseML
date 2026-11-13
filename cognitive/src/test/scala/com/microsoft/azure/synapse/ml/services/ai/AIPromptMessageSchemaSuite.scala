// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.services.openai.OpenAIDecisions
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._
import scala.util.Using

class AIPromptMessageSchemaSuite extends TestBase {
  private val chatReply =
    """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"}}]}"""

  test("duplicate protocol message fields fail schema planning even for empty inputs") {
    def message(content: DataType): StructType =
      new StructType().add("role", StringType).add("content", content)
    val duplicateText = new StructType().add("type", StringType)
      .add("text", StringType).add("text", StringType)
    val duplicateUrl = new StructType().add("url", StringType).add("url", StringType)
    val schemas = Seq(
      message(StringType).add("content", StringType),
      message(StringType).add("role", StringType),
      message(ArrayType(duplicateText)),
      message(ArrayType(new StructType().add("type", StringType).add("image_url", duplicateUrl))),
      message(MapType(StringType, ArrayType(duplicateText))))
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
      val stages = Seq(new AIPrompt(), new AIPrompt().setApiType("responses"),
        new AIPrompt().setApiType("decisions").setQuestions(NativeRequestFixtures.DecisionQuestions),
        new OpenAIDecisions().setQuestions(NativeRequestFixtures.DecisionQuestions))
      stages.foreach { stage =>
        stage.setUrl(server.url + "/v1").setModel("model").setMessagesCol("messages")
        schemas.foreach { fields =>
          val schema = new StructType().add("messages", ArrayType(fields))
          val data = spark.createDataFrame(java.util.Collections.emptyList[Row](), schema)
          assert(intercept[IllegalArgumentException](stage.transformSchema(schema))
            .getMessage.contains("duplicate"))
          assert(intercept[IllegalArgumentException](stage.transform(data)).getMessage.contains("duplicate"))
        }
      }
      assert(server.requests.isEmpty)
    }
  }

  test("message validation leaves duplicate fields in unrelated retained structs unchanged") {
    val message = new StructType().add("role", StringType).add("content", StringType)
    val retained = new StructType().add("text", StringType).add("text", StringType)
    val data = spark.createDataFrame(Seq(Row(Seq(Row("user", "hello")), Row("one", "two"))).asJava,
      new StructType().add("messages", ArrayType(message)).add("retained", retained))
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
      val stage = new AIPrompt().setUrl(server.url + "/v1").setModel("model").setMessagesCol("messages")
        .setOutputCol("answer").setErrorCol("error")
      val result = stage.transform(data)
      assert(server.requests.isEmpty && result.schema == stage.transformSchema(data.schema))
      val row = result.head()
      assert(row.getAs[String]("answer") == "answer" && row.getAs[Row]("retained") == Row("one", "two"))
      assert(server.requests.size() == 1)
    }
  }
}
