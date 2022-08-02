// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.policyeval

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udaf

object PolicyEvalUDAFUtil {
  def registerUdafs(): Unit = {
    val spark = SparkSession.active

    import org.apache.spark.sql.Encoders

    spark.udf.register("snips", udaf(new Snips(), Encoders.product[SnipsInput]))
    spark.udf.register("ips", udaf(new Ips(), Encoders.product[IpsInput]))
    spark.udf.register("cressieRead",
      udaf(new CressieRead(), Encoders.product[CressieReadInput]))
    spark.udf.register("cressieReadInterval",
      udaf(new CressieReadInterval(false), Encoders.product[CressieReadIntervalInput]))
    spark.udf.register("cressieReadIntervalEmpirical",
      udaf(new CressieReadInterval(true), Encoders.product[CressieReadIntervalInput]))
    spark.udf.register("bernstein",
      udaf(new EmpiricalBernsteinCS(), Encoders.product[EmpiricalBernsteinCSInput]))
  }
}
