// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.services.openai.OpenAIPrompt
import org.apache.spark.sql.functions.{col, expr}

import scala.util.Using

class AIPromptBenchmarkSuite extends TestBase {
  test("compare public distributed chat paths at fixed payload, latency, partitions and concurrency") {
    assume(sys.env.get("SYNAPSEML_AI_BENCHMARK").contains("true"), "Opt-in bounded local benchmark")
    val count = 4096
    val reply = """{"id":"bench","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},
                  |"finish_reason":"stop"}]}""".stripMargin
    val data = spark.range(count).withColumn("text", expr("repeat('x', 128 + pmod(id, 896))")).cache()
    assert(data.count() == count)
    try {
      Using.resource(new AIRecordingServer((_, _) => {
        Thread.sleep(5)
        200 -> reply
      })) { server =>
        def configured(stage: OpenAIPrompt, concurrency: Int): OpenAIPrompt =
          stage.setUrl(server.url + "/v1").setDeploymentName("local-benchmark")
            .setPromptTemplate("{text}").setOutputCol("answer").setErrorCol("error")
            .setConcurrency(concurrency)
        Seq(new OpenAIPrompt(), new AIPrompt()).foreach { stage =>
          configured(stage, 1).transform(data.limit(64).coalesce(1)).select("answer").collect()
        }
        Seq(4 -> 1, 16 -> 4).foreach { case (partitions, concurrency) =>
          Seq(new OpenAIPrompt(), new AIPrompt()).foreach { stage =>
            val before = server.requests.size()
            val started = System.nanoTime()
            val rows = configured(stage, concurrency).transform(data.repartition(partitions))
              .select(col("answer"), col("error")).collect()
            val seconds = (System.nanoTime() - started).toDouble / 1e9
            val sends = server.requests.size() - before
            assert(rows.length == count && sends == count)
            assert(rows.forall(row => row.getString(0) == "ok" && row.isNullAt(1)))
            info(s"AI_BENCHMARK stage=${stage.getClass.getSimpleName} rows=$count " +
              s"partitions=$partitions concurrency=$concurrency providerLatencyMs=5 sends=$sends " +
              s"seconds=$seconds rowsPerSecond=${count / seconds}")
          }
        }
      }
    } finally data.unpersist()
  }
}
