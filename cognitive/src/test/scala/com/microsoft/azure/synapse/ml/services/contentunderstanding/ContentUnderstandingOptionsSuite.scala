// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.contentunderstanding

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructType}

import java.util.Locale

class ContentUnderstandingOptionsSuite extends TestBase {
  import ContentUnderstandingFixtures.{Accepted => accepted, DefaultApiVersion, LocationPath, Succeeded}
  import ContentUnderstandingStub.withReplies

  private def stage(server: ContentUnderstandingStub): ContentUnderstanding =
    new ContentUnderstanding().setEndpoint(server.endpoint).setOutputCol("result").setErrorCol("error")
      .setOperationMode("submit").setDocumentBytes(Array[Byte](1))

  private def response(row: Row): ContentUnderstandingResponse =
    ContentUnderstandingResponse.makeFromRowConverter(row.getAs[Row]("result"))

  private def submit(transformer: ContentUnderstanding): ContentUnderstandingResponse =
    response(transformer.transform(spark.range(0L, 1L, 1L, 1)).head())

  test("temporary column names use indexed lookup under case-insensitive collisions") {
    for (suffix <- Seq("Input", "Result", "Error", "Validation")) {
      val prefix = s"contentUnderstanding$suffix"
      var visited = 0
      val occupied = new scala.collection.mutable.HashSet[String] {
        override def iterator: Iterator[String] = super.iterator.map { name =>
          visited += 1
          name
        }
        override def foreach[U](f: String => U): Unit = super.foreach { name =>
          visited += 1
          f(name)
        }
      }
      val count = 2000
      val upper = prefix.toUpperCase(Locale.ROOT)
      occupied ++= (0 until count).map(i => if (i == 0) upper else s"${upper}_$i")
      assert(ContentUnderstanding.internalColumnName(prefix, occupied) == s"${prefix}_$count")
      assert(visited <= count * 2, s"Repeatedly scanned occupied names: $visited visits")
    }
  }

  test("allowInputTruncation preserves unset versus explicit false on GA and preview") {
    Seq(DefaultApiVersion, "2026-06-01-preview").foreach { version =>
      withReplies(Seq(accepted)) { server =>
        assert(submit(stage(server).setApiVersion(version)).status == "Running")
        assert(server.requests.head.query == s"api-version=$version")
      }
    }
    for (version <- Seq(DefaultApiVersion, "2026-06-01-preview"); value <- Seq(true, false)) {
      withReplies(Seq(accepted)) { server =>
        val transformer = stage(server).setApiVersion(version).setAllowInputTruncation(value)
        assert(submit(transformer).status == "Running")
        assert(server.requests.head.query == s"allowInputTruncation=$value&api-version=$version")
        assert(!server.requests.head.body.contains("allowInputTruncation"))
        transformer.clear(transformer.allowInputTruncation)
        submit(transformer)
        assert(server.requests.last.query == s"api-version=$version")
      }
    }
  }

  test("allowInputTruncation columns send booleans and omit null rows") {
    import spark.implicits._

    withReplies(Seq(accepted)) { server =>
      val frame = Seq(Some(true), Some(false), None).toDF("truncate").coalesce(1)
      val transformer = stage(server).setApiVersion("2026-06-01-preview").setAllowInputTruncationCol("truncate")
      assert(transformer.transform(frame).collect().forall(row => response(row).status == "Running"))
      assert(server.requests.map(_.query) == Seq(
        "allowInputTruncation=true&api-version=2026-06-01-preview",
        "allowInputTruncation=false&api-version=2026-06-01-preview", "api-version=2026-06-01-preview"))
      val error = intercept[IllegalArgumentException] {
        transformer.transformSchema(new StructType().add("truncate", StringType))
      }
      assert(error.getMessage.contains("The column configured for allowInputTruncation must have type boolean"))
      assert(server.requests.size == 3)
    }
  }

  test("poll mode ignores a truncation column absent from the input") {
    withReplies(Seq(ContentUnderstandingStubReply(200, Succeeded))) { server =>
      val transformer = stage(server).setOperationMode("poll")
        .setOperationLocation(server.endpoint + LocationPath).setAllowInputTruncationCol("missing")
      assert(submit(transformer).status == "Succeeded")
      assert(server.requests.map(_.method) == Seq("GET"))
      assert(!server.requests.head.query.contains("allowInputTruncation"))
    }
  }
}
