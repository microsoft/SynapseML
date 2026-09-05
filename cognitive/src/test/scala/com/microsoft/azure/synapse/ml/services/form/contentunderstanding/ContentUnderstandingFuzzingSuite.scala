// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.form.contentunderstanding

import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import com.microsoft.azure.synapse.ml.services.contentunderstanding.ContentUnderstanding
import org.apache.http.HttpStatus
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.Row

class ContentUnderstandingFuzzingSuite extends TransformerFuzzing[ContentUnderstanding] {
  import spark.implicits._

  private var endpoint = "https://example.invalid"

  override def testObjects(): Seq[TestObject[ContentUnderstanding]] = {
    val stage = new ContentUnderstanding().setEndpoint(endpoint).setDocumentUrlCol("documentUrl")
      .setDocumentName("synthetic.pdf").setMimeType("application/pdf").setRange("1")
      .setOutputCol("analysis").setErrorCol("error").setMaxPollAttempts(1).setPollingDelay(0)
    val input = Seq("https://example.invalid/synthetic.pdf").toDF("documentUrl")
    Seq(new TestObject(stage, input))
  }

  override def reader: MLReadable[_] = ContentUnderstanding

  private def withService(testCode: => Unit): Unit = {
    val reply = ContentUnderstandingStubReply(HttpStatus.SC_OK, ContentUnderstandingFixtures.Succeeded)
    ContentUnderstandingStub.withReplies(Seq(reply)) { service =>
      val original = endpoint
      endpoint = service.endpoint
      try {
        testCode
        assert(service.requests.nonEmpty)
      } finally {
        endpoint = original
      }
    }
  }

  override def testExperiments(): Unit = withService {
    experimentTestObjects().foreach { testObject =>
      val result = runExperiment(testObject.stage, testObject.fitDF, testObject.transDF).head()
      assert(result.getAs[Row]("analysis").getAs[String]("status") == "Succeeded")
    }
  }

  override def testSerialization(): Unit = withService {
    super.testSerialization()
  }
}
