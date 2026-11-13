// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.stages

import com.microsoft.azure.synapse.ml.core.serialize.ComplexParamTestFixtures.withActiveSession
import com.microsoft.azure.synapse.ml.core.serialize.TestableNestedParamTest
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.io.http.SimpleHTTPTransformer
import org.apache.spark.ml.Serializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import java.io.File

private object UDFPersistenceTestHelpers {
  val StringToIntegerUDF: UserDefinedFunction = udf((_: String) => 1)
}

class VerifyUDFTransformerPersistence extends TestBase {

  test("complex defaults without constructor-reconstruction opt-in remain persisted and guarded") {
    val session = spark.newSession()
    session.conf.set(Serializer.LegacyObjectDeserializationConfig, "false")
    val path = tmpDir.resolve("unregistered-complex-defaults").toString
    new SimpleHTTPTransformer().write.session(session).save(path)
    val error = intercept[SecurityException] {
      SimpleHTTPTransformer.read.session(session).load(path)
    }
    assert(error.getMessage.contains("UserDefinedFunction"))
  }

  Seq(false, true).foreach { allowed =>
    test(s"complex readers use their assigned Spark session deserialization policy with allowed=$allowed") {
      import spark.implicits._

      val config = Serializer.LegacyObjectDeserializationConfig
      val active = spark.newSession()
      active.conf.set(config, (!allowed).toString)
      val readerSession = spark.newSession()
      readerSession.conf.set(config, allowed.toString)
      withActiveSession(Some(active)) {
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
      }
    }
  }

  for {
    allowed <- Seq(false, true)
    array <- Seq(false, true)
    hasActiveSession <- Seq(false, true)
  } {
    test(s"nested complex readers use their assigned Spark session policy with " +
      s"allowed=$allowed, array=$array, active=$hasActiveSession") {
      import spark.implicits._

      val input = Seq("safe").toDF("words")
      val config = Serializer.LegacyObjectDeserializationConfig
      val active = spark.newSession()
      active.conf.set(config, (!allowed).toString)
      val assigned = spark.newSession()
      assigned.conf.set(config, allowed.toString)
      val previousDefault = SparkSession.getDefaultSession
      val expectedActive = if (hasActiveSession) Some(active) else None
      withActiveSession(expectedActive) {
        val path = tmpDir.resolve(s"nested-reader-$allowed-$array-$hasActiveSession.model").toString
        val child = new UDFTransformer().setUDF(UDFPersistenceTestHelpers.StringToIntegerUDF)
          .setInputCol("words").setOutputCol("out")
        new TestableNestedParamTest(s"parent-$allowed-$array").setNested(child, array)
          .write.session(assigned).save(path)
        assert(SparkSession.getActiveSession == expectedActive)
        assert(SparkSession.getDefaultSession == previousDefault)
        if (allowed) {
          val loaded = TestableNestedParamTest.read.session(assigned).load(path).getNested(array)
            .asInstanceOf[UDFTransformer]
          assert(SparkSession.getActiveSession == expectedActive)
          assert(loaded.transform(input).select("out").as[Int].collect().sameElements(Array(1)))
        } else {
          val error = intercept[SecurityException](TestableNestedParamTest.read.session(assigned).load(path))
          assert(error.getMessage.contains(config))
        }
        assert(SparkSession.getActiveSession == expectedActive)
        assert(SparkSession.getDefaultSession == previousDefault)
      }
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
