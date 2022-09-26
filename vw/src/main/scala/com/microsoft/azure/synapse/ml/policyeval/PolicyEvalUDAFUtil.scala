// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.policyeval

import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions.udaf

object PolicyEvalUDAFUtil {
  def registerUdafs(): Unit = {
    val spark = SparkSession.active

    spark.udf.register("snips", Snips)
    spark.udf.register("ips", Ips)
    spark.udf.register("cressieRead", CressieRead)
    spark.udf.register("cressieReadInterval", CressieReadInterval)
    spark.udf.register("cressieReadIntervalEmpirical", CressieReadIntervalEmpirical)
  }

  val Snips = udaf(new Snips(), Encoders.product[SnipsInput])

  val Ips = udaf(new Ips(), Encoders.product[IpsInput])

  val CressieRead = udaf(new CressieRead(), Encoders.product[CressieReadInput])

  val CressieReadInterval = udaf(new CressieReadInterval(false),
    Encoders.product[CressieReadIntervalInput])

  val CressieReadIntervalEmpirical =
    udaf(new CressieReadInterval(true), Encoders.product[CressieReadIntervalInput])
}
