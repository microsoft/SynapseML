// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.stages

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.ml.Serializer
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import java.io.File

private object UDFPersistenceTestHelpers {
  val StringToIntegerUDF: UserDefinedFunction = udf((_: String) => 1)
}

class VerifyUDFTransformerPersistence extends TestBase {

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
