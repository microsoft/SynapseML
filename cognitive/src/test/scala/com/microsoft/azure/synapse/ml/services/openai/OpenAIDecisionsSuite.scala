// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.test.fuzzing.TestObject
import com.microsoft.azure.synapse.ml.services.ai.{AIPromptFuzzing, NativeRequestFixtures}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructType}

import scala.collection.JavaConverters._

class OpenAIDecisionsSuite extends AIPromptFuzzing[OpenAIDecisions] {
  override def reader: MLReadable[_] = OpenAIDecisions

  override def testObjects(): Seq[TestObject[OpenAIDecisions]] = Seq(new TestObject(
    new OpenAIDecisions("decisions-fuzzing").setUrl("https://example.invalid/v1").setModel("synthetic-model")
      .setSubscriptionKey("synthetic-key").setInputCol("input").setQuestions(NativeRequestFixtures.DecisionQuestions)
      .setOutputCol("decision").setErrorCol("error"),
    spark.createDataFrame(Seq(Row(Option.empty[String].orNull)).asJava, new StructType().add("input", StringType))))
}
