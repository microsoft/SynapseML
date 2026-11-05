// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.stages

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.ml.Serializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import java.io.File

private object UDFPersistenceTestHelpers {
  val StringToIntegerUDF: UserDefinedFunction = udf((_: String) => 1)
}

class VerifyUDFTransformerPersistence extends TestBase {

  Seq(false, true).foreach { allowed =>
    test(s"complex readers use their assigned Spark session deserialization policy with allowed=$allowed") {
      import spark.implicits._

      val config = Serializer.LegacyObjectDeserializationConfig
      val active = spark.newSession()
      active.conf.set(config, (!allowed).toString)
      val readerSession = spark.newSession()
      readerSession.conf.set(config, allowed.toString)
      val previous = SparkSession.getActiveSession
      SparkSession.setActiveSession(active)
      try {
        val path = tmpDir.resolve(s"reader-session-$allowed.model").toString
        new UDFTransformer().setUDF(UDFPersistenceTestHelpers.StringToIntegerUDF)
          .setInputCol("words").setOutputCol("out").write.session(readerSession).save(path)
        if (allowed) {
          val loaded = UDFTransformer.read.session(readerSession).load(path)
          assert(loaded.transform(Seq("safe").toDF("words")).select("out").as[Int].collect().sameElements(Array(1)))
        } else {
          val error = intercept[SecurityException](UDFTransformer.read.session(readerSession).load(path))
          assert(error.getMessage.contains(config))
        }
      } finally previous.fold(SparkSession.clearActiveSession())(SparkSession.setActiveSession)
    }
  }

  test("Persisted UDFs require explicit trusted legacy opt-in") {
    import spark.implicits._

    val path = new File(tmpDir.toFile, "trusted-udf.model")
    val transformer = new UDFTransformer().setUDF(UDFPersistenceTestHelpers.StringToIntegerUDF)
      .setInputCol("words").setOutputCol("out")
    val config = Serializer.LegacyObjectDeserializationConfig
    val previous = spark.conf.getOption(config)
    val input = Seq("safe").toDF("words")
    transformer.write.overwrite().save(path.toString)
    spark.conf.unset(config)

    try {
      val error = intercept[SecurityException] {
        UDFTransformer.load(path.toString)
      }
      assert(error.getMessage.contains(config))

      spark.conf.set(config, "true")
      val loaded = UDFTransformer.load(path.toString)
      assert(loaded.transform(input).select("out").as[Int].collect().sameElements(Array(1)))
    } finally {
      previous.fold(spark.conf.unset(config))(spark.conf.set(config, _))
    }
  }
}
