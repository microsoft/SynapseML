// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.core.test.benchmarks.Benchmarks
import com.microsoft.azure.synapse.ml.policyeval._
import org.apache.spark.sql.{functions => F}

class VerifyPolicyEval extends Benchmarks {
  lazy val moduleName = "vw"
  val numPartitions = 2

  import spark.implicits._

  test("Verify BanditSnips") {
    val dataset = Seq(
      (0.2, 1, 0.3, 1, "A", 1),
      (0.2, 2, 0.1, 2, "A", 2),
      (0.2, 3, 0.4, 1, "A", 3),
      (0.2, 2, 0.1, 2, "C", 1),
      (0.2, 3, 0.4, 1, "C", 2)
    ).toDF("probLog", "reward", "probPred", "count", "key", "t")

    PolicyEvalUDAFUtil.registerUdafs()

    val actual = dataset
      .groupBy("key")
      .agg(
        F.round(F.expr("snips(probLog, reward, probPred, count)"), 1).as("snips"),
        F.round(F.expr("ips(probLog, reward, probPred, count)"), 1).as("ips"),
        F.round(F.expr("cressieRead(probLog, reward, probPred, count, -100, 100)"), 1)
          .as("cressieRead"),
        F.expr("cressieReadInterval(probLog, reward, probPred, count, -100, 100, 0, 100)")
          .as("cressieReadInterval"),
        F.expr("cressieReadIntervalEmpirical(probLog, reward, probPred, count, -100, 100)")
          .as("cressieReadIntervalEmpirical")
      )
      .withColumn("cressieReadInterval_lower", F.round(F.expr("cressieReadInterval.lower"), 2))
      .withColumn("cressieReadInterval_upper", F.round(F.expr("cressieReadInterval.upper"), 2))
      .withColumn("cressieReadIntervalEmpirical_lower",
        F.round(F.expr("cressieReadIntervalEmpirical.lower"), 2))
      .withColumn("cressieReadIntervalEmpirical_upper",
        F.round(F.expr("cressieReadIntervalEmpirical.upper"), 2))
      .drop("cressieReadInterval", "cressieReadIntervalEmpirical")

    val expected = Seq(
      ("A", 2.1, 2.4, 2.4, 1.03, 33.82, 1.03, 2.73),
      ("C", 2.7, 2.7, 2.7, 0.98, 56.18, 0.98, 2.98),
    ).toDF("key", "snips", "ips", "cressieRead", "cressieReadInterval_lower", "cressieReadInterval_upper",
        "cressieReadIntervalEmpirical_lower", "cressieReadIntervalEmpirical_upper")

    verifyResult(expected, actual)
  }
}
