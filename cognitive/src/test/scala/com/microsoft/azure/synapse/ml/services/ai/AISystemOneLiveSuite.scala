// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructType}

import scala.collection.JavaConverters._

/** Opt-in inference only: this suite never provisions, changes, or deletes Azure resources. */
class AISystemOneLiveSuite extends TestBase {
  test("existing Decision-1 deployment supports all three question types through direct and facade APIs") {
    val endpoint = AILiveTestEnvironment.get("SYNAPSEML_SYSTEMONE_ENDPOINT")
    val deployment = AILiveTestEnvironment.get("SYNAPSEML_SYSTEMONE_DEPLOYMENT")
    val token = AILiveTestEnvironment.get("SYNAPSEML_SYSTEMONE_TOKEN")
    val key = AILiveTestEnvironment.get("SYNAPSEML_SYSTEMONE_KEY")
    assume(endpoint.isDefined && deployment.isDefined && (token.isDefined || key.isDefined),
      "Live System One endpoint, deployment and credentials are not configured")
    val input = spark.createDataFrame(Seq(
      Row("Checkout is down again for every customer. This is my third request.")).asJava,
      new StructType().add("ticket", StringType))
    Seq(new AISystemOne(), new AIPrompt().setApiType("systemone")).foreach { stage =>
      stage.setUrl(endpoint.get).setDeploymentName(deployment.get).setStateCol("ticket")
        .setQuestions(NativeRequestFixtures.SystemQuestions).setOutputCol("decision")
        .setErrorCol("error").setUsageCol("usage")
      token match {
        case Some(value) => stage.setAADToken(value)
        case None => key.foreach(stage.setSubscriptionKey)
      }
      val result = stage.transform(input)
      assert(result.schema == stage.transformSchema(input.schema))
      val row = result.head()
      assert(row.getAs[Row]("error") == null, "Live System One request failed; inspect the protected error channel")
      val decision = row.getAs[Row]("decision")
      val answers = decision.getAs[Map[String, String]]("answers")
      assert(answers.keySet == Set("repeat_contact", "team", "severity"))
      answers.values.foreach(AIJson.parse)
      assert(decision.getAs[String]("model").nonEmpty)
      assert(row.getAs[Row]("usage") != null)
    }
  }
}
