// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import com.microsoft.azure.synapse.ml.build.BuildInfo
import com.microsoft.azure.synapse.ml.param.GlobalParams
import com.microsoft.azure.synapse.ml.services.OpenAIApiVersion
import com.microsoft.azure.synapse.ml.services.openai.{OpenAIDecisions, OpenAIPrompt}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.internal.SQLConfInjections
import org.apache.spark.sql.types._
import spray.json.{JsArray, JsNumber, JsObject, JsString}

import java.io.File
import java.util.Collections
import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap
import scala.util.Using

class AIPromptStoredIdentitySuite extends AIPromptOutputTestBase {
  test("rendered JSON number kinds define public requests and durable identities") {
    val reply = """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"}}]}"""
    Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
      val data = spark.range(1).select(lit("hello").as("text"))
      def configured(value: Either[String, Double]): AIPrompt = {
        val stage = new AIPrompt("rendered-number").setUrl(server.url + "/v1").setModel("model")
          .setInputCol("text").setOutputCol("answer").setErrorCol("error")
        value.fold(text => stage.setExtraBody(s"""{"x":$text}"""),
          number => stage.setExtraBody(Map("x" -> number)))
      }
      def fingerprint(stage: AIPrompt): String =
        AIPromptOutput.fingerprint(stage, data.schema, stage.transformSchema(data.schema), spark)
      val cases: Seq[(Either[String, Double], String)] =
        Seq(Left("1e0") -> "1", Left("3.2e1") -> "32", Right(12345678.0) -> "12345678")
      cases.foreach { case (value, rendered) =>
        val stage = configured(value)
        assert(fingerprint(stage) == fingerprint(configured(Left(rendered))))
        assert(fingerprint(stage) != fingerprint(configured(Left(rendered + ".0"))))
        assert(stage.transform(data).head().getAs[String]("answer") == "answer")
        val request = server.requests.poll()._2
        assert(AIJson.objectString(request).fields("x").compactPrint == rendered)
      }
      assert(server.requests.isEmpty)
    }
  }

  test("equivalent JSON numeric spellings retain exact durable fingerprints") {
    val data = spark.range(1).select(lit("hello").as("text"))
    def configured(name: String, number: String): AIPrompt = {
      val value = ListMap[String, Any]("number" -> BigDecimal(number),
        "nested" -> Seq(ListMap("number" -> BigDecimal(number))))
      val stage = new AIPrompt("numeric-identity").setUrl("http://localhost/v1").setModel("model")
        .setInputCol("text").setOutputCol("answer").setErrorCol("error")
      name match {
        case "extraBody" => stage.setExtraBody(value)
        case "responseFormat" => stage.setResponseSchema(ListMap("type" -> "object",
          "properties" -> ListMap("value" -> ListMap("type" -> "number")), "values" -> value))
        case "questions" => stage.setApiType("decisions").setQuestions(Seq(ListMap(
          "type" -> "predicate", "name" -> "check", "instructions" -> "Check", "values" -> value)))
      }
    }
    def fingerprint(stage: AIPrompt): String =
      AIPromptOutput.fingerprint(stage, data.schema, stage.transformSchema(data.schema), spark)
    val equivalent = Seq("2.50" -> "2.5", "1.00" -> "1.0", "1e2" -> "100.0", "-0.00" -> "0.0",
      "-2.50" -> "-2.5", "0.1234567890123456789012345678900" -> "0.12345678901234567890123456789",
      "1.00e400" -> "1e400", "1.00e-400" -> "1e-400", "100e2147483645" -> "1000e2147483644")
    Seq("extraBody", "responseFormat", "questions").foreach { name =>
      equivalent.foreach { case (original, rebound) =>
        assert(fingerprint(configured(name, original)) == fingerprint(configured(name, rebound)),
          s"$name: $original and $rebound must have the same recovery identity")
      }
      assert(fingerprint(configured(name, "0.1")) !=
        fingerprint(configured(name, "0.10000000000000000000000000001")))
      Seq("0", "1", "32", "-32").foreach { integer =>
        assert(fingerprint(configured(name, integer)) != fingerprint(configured(name, integer + ".0")),
          s"$name: integer $integer and decimal $integer.0 must have distinct recovery identities")
      }
    }
  }

  test("durable base64 chunking identity rejects changes between batches and across recovery sessions") {
    delta()
    val session = spark.newSession()
    val setting = "spark.sql.chunkBase64String.enabled"
    session.conf.set(setting, "true")
    val data = session.range(3).withColumn("text", lit("long input " * 12))
    val expression = "base64(encode(text, 'UTF-8'))"
    val original = data.selectExpr(expression).head().getString(0)
    session.conf.set(setting, "false")
    val changed = data.selectExpr(expression).head().getString(0)
    assert(original.contains("\r\n") && original.replace("\r\n", "") == changed.replace("\r\n", ""))
    // Earlier Spark patches always wrap; newer runtimes consult this registered setting.
    if (SQLConfInjections.registeredKeys.contains(setting)) assert(original != changed)
    session.conf.set(setting, "true")
    val reply = """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"}}]}"""
    Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
      val root = path()
      val stage = AIPromptStorageTestConfig.configure(new AIPrompt(), root, server.url + "/v1")
        .setPromptTemplate("{" + expression + "}").setOutputBatchSize(1)
      def contents: Vector[String] = server.requests.asScala.toVector.map { case (_, body) =>
        AIJson.objectString(body).fields("messages").asInstanceOf[JsArray].elements.last
          .asJsObject.fields("content").asInstanceOf[JsString].value
      }
      val mismatch = intercept[IllegalArgumentException] {
        AIPromptOutput.transformObserved(stage, data, event =>
          if (event == "batch_committed") session.conf.set(setting, "false"))
      }
      assert(mismatch.getMessage.contains("Spark SQL semantics"))
      val status = AIPrompt.getOutputStatus(session, root)
      assert(status.committedCount == 1 && status.pendingCount == 2 && status.ownerToken.isEmpty)
      assert(contents == Vector(original))
      val resumed = spark.newSession()
      resumed.conf.set(setting, "false")
      val version = AIPromptDelta.version(resumed, root, "_control")
      assert(intercept[IllegalArgumentException](stage.resume(resumed)).getMessage.contains("Spark SQL semantics"))
      assert(contents == Vector(original) && AIPrompt.getOutputStatus(resumed, root).ownerToken.isEmpty)
      assert(AIPromptDelta.version(resumed, root, "_control") == version)
      resumed.conf.set(setting, "true")
      assert(stage.resume(resumed).count() == 3 && contents == Vector.fill(3)(original))
      assert(AIPrompt.getOutputStatus(resumed, root).allSucceeded)
    }
  }

  test("corrupt required control identities reject before public durable reads or owner changes") {
    delta()
    import spark.implicits._
    withServer { server =>
      val root = path()
      val stage = prompt(root, server)
      intercept[IllegalStateException] {
        AIPromptOutput.transformObserved(stage, Seq("hello").toDF("text"), event =>
          if (event == "ready") throw new IllegalStateException("stop before requests"))
      }
      val owner = AIPrompt.getOutputStatus(spark, root).ownerToken.get
      val table = AIPromptDelta.table(root, "_control")
      val actions: Seq[() => Any] = Seq(
        () => AIPrompt.getOutputStatus(spark, root),
        () => AIPrompt.loadOutput(spark, root),
        () => stage.resume(spark),
        () => AIPrompt.releaseOutputOwner(spark, root, owner, confirmStopped = true))
      Seq("runId", "keyColumn", "fingerprint", "libraryVersion", "sparkVersion", "bindings",
        "inputSchema", "outputSchema").foreach { column =>
        val original = spark.sql(s"SELECT $column FROM $table").head().getString(0)
        Seq("NULL", "''").foreach { invalid =>
          spark.sql(s"UPDATE $table SET $column = $invalid")
          val version = AIPromptDelta.version(spark, root, "_control")
          val checks = if (column == "runId") actions else Seq(() => AIPromptDelta.control(spark, root))
          checks.foreach { check =>
            val error = intercept[IllegalArgumentException](check())
            assert(error.getMessage.contains("Corrupt AIPrompt control table") && error.getCause == null)
          }
          assert(AIPromptDelta.version(spark, root, "_control") == version && server.requests.get() == 0)
        }
        spark.sql(s"UPDATE $table SET $column = ${AIPromptDelta.literal(original)}")
      }
      Seq("", " ", "\t").foreach { invalid =>
        spark.sql(s"UPDATE $table SET owner = ${AIPromptDelta.literal(invalid)}")
        val version = AIPromptDelta.version(spark, root, "_control")
        actions.foreach { check =>
          val error = intercept[IllegalArgumentException](check())
          assert(error.getMessage.contains("Corrupt AIPrompt control table") && error.getCause == null)
        }
        assert(AIPromptDelta.version(spark, root, "_control") == version && server.requests.get() == 0)
      }
      spark.sql(s"UPDATE $table SET owner = ${AIPromptDelta.literal(owner)}")
      assert(AIPrompt.getOutputStatus(spark, root).ownerToken.contains(owner))
      AIPrompt.releaseOutputOwner(spark, root, owner, confirmStopped = true)
      assert(AIPrompt.getOutputStatus(spark, root).ownerToken.isEmpty)
      assert(stage.resume(spark).count() == 1 && server.requests.get() == 1)
    }
  }

  test("durable recovery rejects missing scalar authentication before ownership and permits credential rotation") {
    delta()
    import spark.implicits._
    withServer { server =>
      val root = path()
      val stage = prompt(root, server).setTelemHeaders(Map.empty[String, String])
      intercept[IllegalStateException] {
        AIPromptOutput.transformObserved(stage, Seq("one", "two", "three").toDF("text"), event =>
          if (event == "batch_committed") throw new IllegalStateException("stop after first batch"))
      }
      val status = AIPrompt.getOutputStatus(spark, root)
      assert(status.committedCount == 2 && server.requests.get() == 2)
      AIPrompt.releaseOutputOwner(spark, root, status.ownerToken.get, confirmStopped = true)
      val version = AIPromptDelta.version(spark, root, "_control")
      val missing = prompt(root, server).setTelemHeaders(Map.empty[String, String])
      missing.clear(missing.subscriptionKey)
      Seq(false, true).foreach { blank =>
        if (blank) missing.setSubscriptionKey(" ").setAADToken(" ").setCustomAuthHeader(" ")
          .setCustomHeaders(Map("Authorization" -> " ")).setTelemHeaders(Map("api-key" -> "ignored"))
        val error = intercept[IllegalArgumentException] {
          AIPromptOutput.resumeObserved(missing, spark, event => fail(s"Missing auth acquired ownership: $event"))
        }
        assert(error.getMessage.contains("configuration"))
        assert(AIPromptDelta.version(spark, root, "_control") == version && server.requests.get() == 2)
        assert(AIPrompt.getOutputStatus(spark, root).ownerToken.isEmpty)
      }
      missing.setAADToken("rotated-synthetic-token")
      assert(missing.resume(spark).count() == 3 && server.requests.get() == 3)
      assert(AIPrompt.getOutputStatus(spark, root).allSucceeded)
    }
  }

  test("durable authentication presence follows scalar precedence inherited headers and implicit Fabric routing") {
    val schema = new StructType().add("text", StringType)
    val stages = Seq(
      new AIPrompt("auth-presence") -> "api-key",
      new TestableAIHeaderPrompt("auth-presence") -> "x-model-key",
      new TestableAISharedHeaderPrompt("auth-presence") -> "authorization",
      new TestableAIFabricAuthPrompt("auth-presence") -> "api-key")
    stages.foreach { case (source, authName) =>
      val base = source.setPromptTemplate("{text}").setTelemHeaders(Map.empty[String, String]).effectiveCopy()
      def fingerprint(stage: AIPrompt): String = AIPromptOutput.fingerprint(stage, schema, schema, spark)
      val configured = fingerprint(base.copy(org.apache.spark.ml.param.ParamMap.empty)
        .setSubscriptionKey("synthetic-key"))
      val setters: Seq[AIPrompt => Unit] = Seq(
        _.setAADToken("synthetic-token"), _.setCustomAuthHeader("Custom synthetic-token"),
        _.setCustomHeaders(Map(authName.toUpperCase(java.util.Locale.ROOT) -> "synthetic-key")),
        _.setSubscriptionKey(" ").setAADToken("synthetic-token"))
      setters.foreach { set =>
        val rotated = base.copy(org.apache.spark.ml.param.ParamMap.empty)
        set(rotated)
        assert(fingerprint(rotated) == configured)
      }
      val implicitFabric = source.isInstanceOf[TestableAIFabricAuthPrompt]
      assert((fingerprint(base) == configured) == implicitFabric)
      val blank = base.copy(org.apache.spark.ml.param.ParamMap.empty).setSubscriptionKey(" ")
        .setAADToken(" ").setCustomAuthHeader(" ").setCustomHeaders(Map(authName -> " "))
        .setTelemHeaders(Map(authName -> "ignored"))
      assert(fingerprint(blank) == fingerprint(base))
    }
  }

  Seq("systemone", "decisions").foreach { api =>
    Seq(Map.empty[String, String], Map("same" -> "telemetry")).foreach { telemetry =>
      test(s"$api class identity is independent of telemetry $telemetry for pending and completed recovery") {
        delta()
        import spark.implicits._
        val reply = if (api == "systemone") NativeRequestFixtures.SystemResponse
          else NativeRequestFixtures.DecisionResponse
        val questions = if (api == "systemone") NativeRequestFixtures.SystemQuestions
          else NativeRequestFixtures.DecisionQuestions
        scala.util.Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
          val root = path()
          def configure(stage: AIPrompt): AIPrompt = stage.setApiType(api).setTelemHeaders(telemetry)
            .setUrl(server.url).setModel("model").setQuestions(questions).setInputCol("text")
            .setOutputCol("answer").setErrorCol("error").setOutputPath(root).setOutputBatchSize(1)
          def direct(): AIPrompt = configure(if (api == "systemone") new AISystemOne() else new OpenAIDecisions())
          intercept[IllegalStateException] {
            AIPromptOutput.transformObserved(direct(), Seq("one", "two").toDF("text"), event =>
              if (event == "batch_committed") throw new IllegalStateException("stop after first outcome"))
          }
          val owner = AIPrompt.getOutputStatus(spark, root).ownerToken.get
          AIPrompt.releaseOutputOwner(spark, root, owner, confirmStopped = true)
          Seq(false, true).foreach { complete =>
            val version = AIPromptDelta.version(spark, root, "_control")
            val mismatch = intercept[IllegalArgumentException](configure(new AIPrompt()).resume(spark))
            assert(mismatch.getMessage.contains("stage class"))
            assert(AIPromptDelta.version(spark, root, "_control") == version)
            assert(AIPrompt.getOutputStatus(spark, root).ownerToken.isEmpty)
            assert(server.requests.size() == (if (complete) 2 else 1))
            assert(direct().resume(spark).count() == 2 && server.requests.size() == 2)
          }
        }
      }
    }
  }

  Seq(false, true).foreach { complete =>
    test(s"resume rejects changed stored build or Spark identity before ownership when complete is $complete") {
      delta()
      import spark.implicits._
      withServer { server =>
        val root = path()
        val stage = prompt(root, server)
        val data = Seq("hello").toDF("text")
        if (complete) stage.transform(data) else {
          intercept[IllegalStateException] {
            AIPromptOutput.transformObserved(stage, data, event =>
              if (event == "ready") throw new IllegalStateException("stop before requests"))
          }
          val owner = AIPrompt.getOutputStatus(spark, root).ownerToken.get
          AIPrompt.releaseOutputOwner(spark, root, owner, confirmStopped = true)
        }
        val table = AIPromptDelta.table(root, "_control")
        val original = spark.sql(s"SELECT * FROM $table").head()
        val versions = Seq("libraryVersion" -> BuildInfo.version, "sparkVersion" -> spark.version)
        versions.foreach { case (column, expected) =>
          assert(original.getAs[String](column) == expected)
          spark.sql(s"UPDATE $table SET $column = 'incompatible-version'")
          val version = AIPromptDelta.version(spark, root, "_control")
          val error = intercept[IllegalArgumentException](stage.resume(spark))
          assert(error.getMessage.contains("build or Spark runtime") && error.getCause == null)
          assert(AIPromptDelta.version(spark, root, "_control") == version)
          assert(AIPrompt.getOutputStatus(spark, root).ownerToken.isEmpty)
          assert(AIPrompt.loadOutput(spark, root).count() == (if (complete) 1 else 0))
          assert(server.requests.get() == (if (complete) 1 else 0))
          spark.sql(s"UPDATE $table SET $column = ${AIPromptDelta.literal(expected)}")
        }
        assert(stage.resume(spark).count() == 1 && server.requests.get() == 1)
      }
    }
  }

  private val malformedSchemas = Seq(
    "non-struct" -> "\"string\"",
    "field-name" -> """{"struct":[{"name":5,"type":"string","metadata":{}}]}""",
    "field-value" -> """{"struct":[null]}""",
    "metadata" -> """{"struct":[{"name":"text","type":"string","metadata":{"secret":[1,"private"]}}]}""",
    "metadata-array" -> """{"struct":[{"name":"text","type":"string","metadata":{"secret":[null]}}]}""",
    "metadata-number" -> """{"struct":[{"name":"text","type":"string","metadata":{"n":9223372036854775808}}]}""",
    "decimal" -> """{"struct":[{"name":"text","type":{"precision":1.5,"scale":0},"metadata":{}}]}""",
    "decimal-precision" -> """{"struct":[{"name":"text","type":{"precision":99,"scale":0},"metadata":{}}]}""",
    "decimal-scale" -> """{"struct":[{"name":"text","type":{"precision":1,"scale":2},"metadata":{}}]}""",
    "deep" -> ("{\"array\":" * 110 + "\"string\"" + "}" * 110))

  malformedSchemas.foreach { case (name, json) =>
    test(s"malformed stored $name schema fails with a controlled argument error") {
      val error = intercept[IllegalArgumentException](AIPromptStorageCodec.readSchema(json))
      assert(error.getCause == null && !error.getMessage.contains("private"))
    }
  }

  test("corrupt schema rejects all public durable reads and owner release without writes or sends") {
    delta()
    import spark.implicits._
    withServer { server =>
      val root = path()
      val stage = prompt(root, server)
      intercept[IllegalStateException] {
        AIPromptOutput.transformObserved(stage, Seq("hello").toDF("text"), event =>
          if (event == "ready") throw new IllegalStateException("stop before requests"))
      }
      val owner = AIPrompt.getOutputStatus(spark, root).ownerToken.get
      val table = AIPromptDelta.table(root, "_control")
      Seq("inputSchema", "outputSchema").foreach { column =>
        val original = spark.sql(s"SELECT $column FROM $table").head().getString(0)
        malformedSchemas.foreach { case (_, json) =>
          spark.sql(s"UPDATE $table SET $column = ${AIPromptDelta.literal(json)}")
          val version = AIPromptDelta.version(spark, root, "_control")
          val actions: Seq[() => Any] = Seq(
            () => AIPrompt.getOutputStatus(spark, root),
            () => AIPrompt.loadOutput(spark, root),
            () => stage.resume(spark),
            () => AIPrompt.releaseOutputOwner(spark, root, owner, confirmStopped = true))
          actions.foreach { action =>
            val error = intercept[IllegalArgumentException](action())
            assert(error.getCause == null && !error.getMessage.contains("private"))
          }
          assert(AIPromptDelta.version(spark, root, "_control") == version && server.requests.get() == 0)
        }
        spark.sql(s"UPDATE $table SET $column = ${AIPromptDelta.literal(original)}")
      }
      assert(AIPrompt.getOutputStatus(spark, root).ownerToken.contains(owner))
      AIPrompt.releaseOutputOwner(spark, root, owner, confirmStopped = true)
      assert(stage.resume(spark).count() == 1 && server.requests.get() == 1)
    }
  }

  test("schema depth limits reject before durable initialization and supported metadata round trips") {
    delta()
    withServer { server =>
      def nestedStruct(levels: Int): DataType =
        (1 to levels).foldLeft[DataType](StringType)((value, _) => new StructType().add("child", value))
      val accepted = new StructType().add("text", StringType).add("nested", nestedStruct(32))
      assert(AIPromptStorageCodec.readSchema(AIPromptStorageCodec.schemaJson(accepted)) ==
        AIPromptOutput.storageSchema(accepted))
      val metadata = new MetadataBuilder().putLongArray("values", Array(1L, 2L))
        .putMetadata("nested", new MetadataBuilder().putString("label", "value").build()).build()
      val leaf = StructType(Seq(StructField("child", StringType, nullable = false, metadata)))
      val withMetadata = (1 until 32).foldLeft[DataType](leaf)((value, _) =>
        new StructType().add("child", value))
      val arrays = (1 to 110).foldLeft[DataType](StringType)((value, _) => ArrayType(value))
      Seq(arrays, nestedStruct(40), withMetadata).foreach { nested =>
        val root = path()
        val schema = new StructType().add("text", StringType).add("nested", nested)
        val data = spark.createDataFrame(Collections.emptyList[Row](), schema)
        val error = intercept[IllegalArgumentException](prompt(root, server).transform(data))
        assert(error.getMessage.contains("AIPrompt durable schema") && error.getCause == null)
        assert(!new File(root).exists() && server.requests.get() == 0)
      }
      val supported = StructType(Seq(StructField("text", StringType, nullable = false, metadata)))
      assert(AIPromptStorageCodec.readSchema(AIPromptStorageCodec.schemaJson(supported)) ==
        AIPromptOutput.storageSchema(supported))
    }
  }
}
