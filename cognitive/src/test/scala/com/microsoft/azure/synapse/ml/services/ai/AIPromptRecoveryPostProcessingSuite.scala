// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructType}

import scala.collection.JavaConverters._
import scala.util.Using

class AIPromptRecoveryPostProcessingSuite extends AIPromptOutputTestBase {
  Seq("csv", "regex").foreach { mode =>
    test(s"durable $mode postprocessing keeps planned schemas and values") {
      delta()
      withServer { server =>
        val session = spark
        import session.implicits._
        val root = path()
        val options = if (mode == "csv") Map("delimiter" -> "n") else {
          Map("regex" -> "a(nswer)", "regexGroup" -> "1")
        }
        val stage = prompt(root, server).setPostProcessingOptions(options)
        val input = Seq("hello", "ROW_ERROR").toDF("text")
        val result = stage.transform(input)
        Seq(result, AIPrompt.loadOutput(spark, root), stage.resume(spark)).foreach { output =>
          assert(output.schema == stage.transformSchema(input.schema))
          val rows = output.collect()
          val answered = rows.find(_.getAs[String]("text") == "hello").get
          if (mode == "csv") assert(answered.getAs[Seq[String]]("answer") == Seq("a", "swer"))
          else assert(answered.getAs[String]("answer") == "nswer")
          assert(answered.getAs[Row]("error") == null)
          val failed = rows.find(_.getAs[String]("text") == "ROW_ERROR").get
          assert(failed.isNullAt(failed.fieldIndex("answer")) && failed.getAs[Row]("error") != null)
        }
        assert(server.requests.get() == 2)
        assert(AIPrompt.getOutputStatus(spark, root).rowErrorCount == 1)
      }
    }
  }
}
