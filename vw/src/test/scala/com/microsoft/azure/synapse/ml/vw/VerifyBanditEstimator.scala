package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.core.test.benchmarks.Benchmarks
import org.apache.spark.sql.{functions => F, types => T}

class VerifyBanditEstimator extends Benchmarks  {
  lazy val moduleName = "vw"
  val numPartitions = 2

  test ("Verify BanditSnips") {
    import spark.implicits._

    val dataset = Seq(
      (0.2, 1, 0.3, 1, "A"),
      (0.2, 2, 0.1, 2, "A"),
      (0.2, 3, 0.4, 1, "C"),
    ).toDF("probLog", "reward", "probPred", "count", "key")

    import org.apache.spark.sql.Encoders

    spark.udf.register("snips", F.udaf(new BanditEstimatorSnips(), Encoders.product[BanditEstimatorSnipsInput]))
    spark.udf.register("ips", F.udaf(new BanditEstimatorIps(), Encoders.product[BanditEstimatorIpsInput]))
    spark.udf.register("cressieRead",
      F.udaf(new BanditEstimatorCressieRead(), Encoders.product[BanditEstimatorCressieReadInput]))
    spark.udf.register("cressieReadInterval",
      F.udaf(new BanditEstimatorCressieReadInterval(), Encoders.product[BanditEstimatorCressieReadIntervalInput]))

    val actual = dataset
      .groupBy("key")
      .agg(
        F.round(F.expr("snips(probLog, reward, probPred, count)"), 1).as("snips"),
        F.round(F.expr("ips(probLog, reward, probPred, count)"), 1).as("ips"),
        F.round(F.expr("cressieRead(probLog, reward, probPred, count, -100, 100)"), 1)
          .as("cressieRead"),
        F.expr("cressieReadInterval(probLog, reward, probPred, count, -100, 100)").as("cressieReadInterval")
      )

    actual.show()

    //    val expected = Seq(
    //      ("A", 1.4f, 1.2f),
    //      ("C", 3.0f, 6.0f)
    //    ).toDF("key", "snips", "ips")
    //
    //    assertDFEq(actual, expected)
  }
}
