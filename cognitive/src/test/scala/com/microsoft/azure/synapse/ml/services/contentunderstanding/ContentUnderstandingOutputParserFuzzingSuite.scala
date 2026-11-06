// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.contentunderstanding

import com.microsoft.azure.synapse.ml.codegen.CodegenConfig
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import com.microsoft.azure.synapse.ml.io.http.HTTPSchema
import com.microsoft.azure.synapse.ml.logging.FeatureNames
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.functions.col

import scala.collection.mutable.ArrayBuffer

class ContentUnderstandingOutputParserFuzzingSuite
  extends TransformerFuzzing[ContentUnderstandingOutputParser] {
  import spark.implicits._

  override def testObjects(): Seq[TestObject[ContentUnderstandingOutputParser]] = {
    val response = ContentUnderstandingResponse(None, Some("op-1"), "Succeeded", 200,
      ContentUnderstandingFixtures.Succeeded, None)
    val body = ContentUnderstandingResponse.JsonFormat.write(response).compactPrint
    val input = Seq(body).toDF("body")
      .withColumn("response", HTTPSchema.string_to_response(col("body")))
    val parser = new ContentUnderstandingOutputParser().setInputCol("response").setOutputCol("analysis")
    Seq(new TestObject(parser, input))
  }

  override def reader: MLReadable[_] = ContentUnderstandingOutputParser

  test("internal parser telemetry does not emit public CU feature events") {
    val events = ArrayBuffer.empty[(String, Option[String])]
    val parser = new ContentUnderstandingOutputParser("telemetry-parser") {
      override protected def logBase(info: Map[String, String], featureName: Option[String]): Unit = {
        events += info("method") -> featureName
      }
    }
    parser.setInputCol("response").setOutputCol("analysis")
    val response = ContentUnderstandingResponse(None, Some("op-1"), "Succeeded", 200,
      ContentUnderstandingFixtures.Succeeded, None)
    val body = ContentUnderstandingResponse.JsonFormat.write(response).compactPrint
    val input = Seq(body).toDF("body").withColumn("response", HTTPSchema.string_to_response(col("body")))
    assert(parser.transform(input).select("analysis.status").head().getString(0) == "Succeeded")
    assert(events.contains("constructor" -> Some(FeatureNames.Core)))
    assert(events.contains("transform" -> None))
    assert(!events.exists(_._2.contains(FeatureNames.AiServices.ContentUnderstanding)))
  }

  // Generic language tests assume public wrappers; the internal parser is covered by Scala fuzzing.
  override def makePyTestFile(conf: CodegenConfig): Unit = ()

  override def makeRTestFile(conf: CodegenConfig): Unit = ()

}
