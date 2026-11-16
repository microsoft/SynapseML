// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import org.apache.spark.ml.linalg.{DenseVector, Matrices, SQLDataTypes, SparseVector, Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spray.json._

import java.nio.file.{Files, Paths}
import java.util.Arrays
import scala.util.Using

class AIPromptStorageCodecSuite extends AIPromptOutputTestBase {
  test("durable schema planning rejects negative decimal scales at every nesting position") {
    val setting = "spark.sql.legacy.allowNegativeScaleOfDecimal"
    val previous = spark.conf.getOption(setting)
    try {
      spark.conf.set(setting, "true")
      val decimal = DecimalType(8, -1)
      val types = Seq(decimal, new StructType().add("amount", decimal), ArrayType(decimal),
        MapType(StringType, decimal), MapType(decimal, StringType))
      types.foreach { dataType =>
        val root = path()
        val stage = new AIPrompt().setOutputPath(root).setUrl("http://localhost/v1")
          .setModel("model").setInputCol("text")
        val schema = new StructType().add("text", StringType).add("retained", dataType)
        val error = intercept[IllegalArgumentException](stage.transformSchema(schema))
        assert(error.getMessage.contains("nonnegative decimal scales"))
        assert(!Files.exists(Paths.get(root)))
      }
    } finally previous.fold(spark.conf.unset(setting))(spark.conf.set(setting, _))
  }

  test("negative decimal scales stay valid lazily but fail durable transform before I/O") {
    delta()
    val setting = "spark.sql.legacy.allowNegativeScaleOfDecimal"
    val previous = spark.conf.getOption(setting)
    try {
      spark.conf.set(setting, "true")
      val amount = lit(120).cast(DecimalType(8, -1))
      val values = Seq(amount, struct(amount.as("amount")), array(amount),
        map(lit("key"), amount), map(amount, lit("value")))
      withServer { server =>
        values.zipWithIndex.foreach { case (retained, index) =>
          val data = spark.range(1).select(lit("hello").as("text"), retained.as("retained"))
          val root = path()
          val stage = prompt(root, server)
          val lazyStage = stage.copy(ParamMap.empty)
          lazyStage.clear(lazyStage.outputPath)
          val lazyRow = lazyStage.transform(data).head()
          assert(lazyRow.getAs[String]("answer") == "answer")
          assert(lazyRow.getAs[Any]("retained") == data.head().getAs[Any]("retained"))
          val error = intercept[IllegalArgumentException](stage.transform(data))
          assert(error.getMessage.contains("nonnegative decimal scales"))
          assert(!Files.exists(Paths.get(root)) && server.requests.get() == index + 1)
        }
      }
    } finally previous.fold(spark.conf.unset(setting))(spark.conf.set(setting, _))
  }

  test("nonnegative decimal scales retain values through durable transform load and resume") {
    delta()
    val data = spark.range(1).select(lit("hello").as("text"),
      lit(120).cast(DecimalType(8, 0)).as("whole"),
      array(lit("12.34").cast(DecimalType(8, 2))).as("nested"))
    withServer { server =>
      val root = path()
      val stage = prompt(root, server)
      Seq(stage.transform(data), AIPrompt.loadOutput(spark, root), stage.resume(spark)).foreach { output =>
        assert(output.schema == stage.transformSchema(data.schema))
        assert(output.select("whole", "nested").head() == data.select("whole", "nested").head())
      }
      assert(server.requests.get() == 1 && AIPrompt.getOutputStatus(spark, root).allSucceeded)
    }
  }

  Seq("sql alias", "file metadata").foreach { source =>
    test(s"durable storage accepts Spark internal metadata from $source") {
      delta()
      val input = if (source == "sql alias") {
        spark.sql("SELECT lower('HELLO')").toDF("text")
      } else {
        val parquet = path()
        spark.range(1).select(lit("hello").as("text")).write.parquet(parquet)
        spark.read.parquet(parquet).select(col("text"), col("_metadata"))
      }
      assert(input.schema.fields.exists(_.metadata != Metadata.empty))
      withServer { server =>
        val root = path()
        val stage = prompt(root, server)
        val transformed = stage.transform(input)
        val loaded = AIPrompt.loadOutput(spark, root)
        val resumed = stage.resume(spark)
        Seq(transformed, loaded, resumed).foreach { output =>
          assert(output.schema == stage.transformSchema(input.schema))
          assert(output.select(input.columns.map(col): _*).collect().toSeq == input.collect().toSeq)
          assert(output.head().getAs[String]("answer") == "answer")
        }
        assert(AIPrompt.getOutputStatus(spark, root).allSucceeded && server.requests.get() == 1)
      }
    }
  }

  test("durable schemas remove source-column behavior while preserving nested user metadata") {
    delta()
    val user = new MetadataBuilder().putString("description", "retained").build()
    val generated = new MetadataBuilder().withMetadata(user)
      .putString("delta.generationExpression", "lower('HELLO')").build()
    val internal = new MetadataBuilder().withMetadata(user).putBoolean("__autoGeneratedAlias", true)
      .putLong("delta.identity.start", 1L).putLong("delta.identity.step", 1L)
      .putLong("delta.identity.highWaterMark", 2L).putBoolean("delta.identity.allowExplicitInsert", true).build()
    val nested = StructType(Seq(StructField("value", StringType, nullable = true, internal)))
    val schema = StructType(Seq(StructField("text", StringType, nullable = true, generated),
      StructField("nested", ArrayType(nested), nullable = true, user),
      StructField("mapped", MapType(StringType, nested), nullable = true, user)))
    val input = spark.createDataFrame(Arrays.asList(
      Row("hello", Seq(Row("item")), Map("key" -> Row("item"))),
      Row("second", Seq(Row("other")), Map("key" -> Row("other")))), schema)
    withServer { server =>
      val root = path()
      val stage = prompt(root, server).setOutputBatchSize(1)
      val expected = StructType(Seq(StructField("text", StringType, nullable = true, user),
        StructField("nested", ArrayType(StructType(Seq(StructField("value", StringType, true, user)))), true, user),
        StructField("mapped", MapType(StringType, StructType(Seq(StructField("value", StringType, true, user)))),
          true, user)))
      assert(AIPromptOutput.storageSchema(schema) == expected)
      intercept[IllegalStateException] {
        AIPromptOutput.transformObserved(stage, input, event =>
          if (event == "batch_committed") throw new IllegalStateException("stop after first metadata batch"))
      }
      val partial = AIPrompt.getOutputStatus(spark, root)
      assert(partial.committedCount == 1 && partial.pendingCount == 1 && server.requests.get() == 1)
      assert(AIPrompt.loadOutput(spark, root).count() == 1 && server.requests.get() == 1)
      AIPromptOutput.releaseOutputOwner(spark, root, partial.ownerToken.get, confirmStopped = true)
      val recovery = spark.newSession()
      Seq(stage.resume(recovery), AIPrompt.loadOutput(recovery, root), stage.resume(recovery)).foreach { output =>
        assert(output.schema == stage.transformSchema(schema))
        val restored = output.select(input.columns.map(col): _*)
        assert(restored.schema == expected)
        assert(restored.orderBy("text").collect().toSeq == input.orderBy("text").collect().toSeq)
        val changed = new MetadataBuilder().putString("description", "modified").build()
        val modified = restored.select(col("text").as("text", changed), col("nested"), col("mapped"))
        intercept[IllegalArgumentException](AIPromptDelta.validateSchema(modified, expected))
      }
      assert(server.requests.get() == 2 && AIPrompt.getOutputStatus(recovery, root).allSucceeded)
    }
  }

  test("file authorities reject before public durable I/O even in Spark local mode") {
    delta()
    withServer { server =>
      val root = path()
      val suffix = new java.io.File(root).toURI.getPath
      Seq("file://remote.invalid", "file://localhost").foreach { prefix =>
        val location = prefix + suffix
        val qualified = AIPromptDelta.qualify(spark, location)
        val direct = intercept[IllegalArgumentException](AIPromptDelta.requireRuntime(spark, qualified))
        assert(direct.getMessage.contains("same-host local directory"))
        val stage = prompt(location, server)
        val input = spark.range(1).select(lit("hello").as("text"))
        val error = intercept[IllegalArgumentException](stage.transform(input))
        assert(error.getMessage.contains("same-host local directory"))
      }
      assert(!Files.exists(Paths.get(root)) && server.requests.get() == 0)
    }
  }

  Seq("retained", "input", "extraBody", "responseFormat").foreach { binding =>
    test(s"empty input names remain valid for lazy $binding bindings but not durable storage") {
      val reply = """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"}}]}"""
      val value = binding match {
        case "extraBody" => """{"metadata":{"source":"empty-name"}}"""
        case "responseFormat" => """{"type":"json_object"}"""
        case _ => "empty-name"
      }
      val input = spark.range(1).select(lit("hello").as("text"), lit(value).as(""))
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        val stage = new AIPrompt().setUrl(server.url + "/v1").setModel("model")
          .setInputCol("text").setOutputCol("answer").setErrorCol("error")
        binding match {
          case "input" => stage.setInputCol("")
          case "extraBody" => stage.setExtraBodyCol("")
          case "responseFormat" => stage.setResponseFormatCol("")
          case _ => ()
        }
        val output = stage.transform(input)
        assert(output.schema == stage.transformSchema(input.schema))
        val row = output.head()
        assert(row.getAs[String]("") == value)
        assert(row.getAs[String]("answer") == "answer" && row.getAs[Row]("error") == null)
        val body = AIJson.objectString(server.requests.peek()._2)
        val message = body.fields("messages").asInstanceOf[JsArray].elements.last.asJsObject
        assert(message.fields("content") == JsString(if (binding == "input") value else "hello"))
        if (binding == "extraBody") assert(body.fields("metadata") == JsObject("source" -> JsString("empty-name")))
        if (binding == "responseFormat") assert(body.fields("response_format") == AIJson.parse(value))
        val root = path()
        val durable = stage.copy(ParamMap.empty).setOutputPath(root)
        assert(intercept[IllegalArgumentException](durable.transformSchema(input.schema))
          .getMessage.contains("Delta-compatible column names"))
        if (AIPromptDeltaTestSession.hasDelta) {
          assert(intercept[IllegalArgumentException](durable.transform(input))
            .getMessage.contains("Delta-compatible column names"))
        } else {
          assert(intercept[IllegalStateException](durable.transform(input))
            .getMessage.contains("requires a compatible, caller-configured Delta runtime"))
        }
        assert(!Files.exists(Paths.get(root)) && server.requests.size() == 1)
      }
    }
  }

  test("vector map keys retain SQL identity through lazy and durable transform load and resume") {
    delta()
    withServer { server =>
      val root = path()
      val stage = prompt(root, server)
      val schema = new StructType().add("dense", SQLDataTypes.VectorType)
        .add("sparse", SQLDataTypes.VectorType)
      val vectors = spark.createDataFrame(Arrays.asList(
        Row(Vectors.dense(1.0), Vectors.sparse(1, Array(0), Array(1.0)))), schema)
      val weights = map_from_arrays(array(col("dense"), col("sparse")), array(lit("dense"), lit("sparse")))
      val vectorValues = map_from_arrays(array(col("dense"), col("sparse")), array(col("sparse"), col("dense")))
      val nested = array(struct(weights.as("weights"),
        array(col("dense"), col("sparse")).as("vectors")))
      val optional = struct(when(lit(false), weights).as("weights"), when(lit(false), nested).as("nested"))
      val metadata = new MetadataBuilder().putString("source", "vector-key-map").build()
      val input = vectors.select(lit("test").as("text"), weights.as("weights", metadata), nested.as("nested"),
        vectorValues.as("vectorValues"), optional.as("optional"), when(lit(false), optional).as("missing"))
      assert(input.select(size(col("weights")), size(col("nested").getItem(0).getField("weights"))).head() ==
        Row(2, 2))
      def verify(output: DataFrame, expected: StructType): Unit = {
        assert(output.schema == expected)
        Seq(col("weights"), col("nested").getItem(0).getField("weights")).foreach { column =>
          val entries = output.select(explode(column)).collect()
          assert(entries.length == 2)
          val keys = entries.map(row => row.getString(1) -> row.getAs[Vector](0)).toMap
          assert(keys("dense").isInstanceOf[DenseVector])
          assert(keys("sparse").isInstanceOf[SparseVector])
          assert(keys.values.forall(_.toArray.toSeq == Seq(1.0)))
        }
        val nestedVectors = output.select(col("nested").getItem(0).getField("vectors")).head().getSeq[Vector](0)
        assert(nestedVectors.head.isInstanceOf[DenseVector] && nestedVectors(1).isInstanceOf[SparseVector])
        val pairs = output.select(explode(col("vectorValues"))).collect().map { row =>
          row.getAs[Vector](0).getClass -> row.getAs[Vector](1).getClass
        }.toSet
        assert(pairs == Set(classOf[DenseVector] -> classOf[SparseVector],
          classOf[SparseVector] -> classOf[DenseVector]))
        assert(output.select(col("optional.weights").isNull, col("optional.nested").isNull,
          col("missing").isNull).head() == Row(true, true, true))
      }
      val lazyStage = stage.copy(ParamMap.empty)
      lazyStage.clear(lazyStage.outputPath)
      val lazyOutput = lazyStage.transform(input).cache()
      try verify(lazyOutput, lazyStage.transformSchema(input.schema)) finally lazyOutput.unpersist()
      val transformed = stage.transform(input)
      Seq(transformed, AIPrompt.loadOutput(spark, root), stage.resume(spark)).foreach { output =>
        verify(output, stage.transformSchema(input.schema))
      }
      val replacing = lazyStage.setOutputCol("weights")
      val replaced = replacing.transform(input).cache()
      try {
        assert(replaced.schema == replacing.transformSchema(input.schema))
        assert(replaced.select(col("weights"), size(col("nested").getItem(0).getField("weights"))).head() ==
          Row("answer", 2))
      } finally replaced.unpersist()
      assert(server.requests.get() == 3)
    }
  }

  Seq(false, true).foreach { withVectors =>
    test(s"durable nested Unicode names follow SQL resolution with vectors=$withVectors") {
      delta()
      val caseSensitive = spark.conf.get("spark.sql.caseSensitive")
      try {
        spark.conf.set("spark.sql.caseSensitive", "false")
        val vectors = spark.createDataFrame(Arrays.asList(Row(Vectors.dense(1.0))),
          new StructType().add("vector", SQLDataTypes.VectorType))
        val fields = Seq(lit(1).as("\u03c3"), lit(2).as("\u03c2")) ++ (if (withVectors)
          Seq(map_from_arrays(array(col("vector")), array(lit("weight"))).as("weights")) else Seq.empty)
        val payload = struct(fields: _*)
        val input = vectors.select(lit("hello").as("text"), payload.as("payload"),
          array(payload).as("items"), map(lit("item"), payload).as("mapping"))
        withServer { server =>
          val root = path()
          val stage = prompt(root, server)
          Seq[() => Any](() => stage.transformSchema(input.schema), () => stage.transform(input)).foreach { call =>
            assert(intercept[IllegalArgumentException](call()).getMessage.contains("Ambiguous durable fields"))
          }
          assert(!Files.exists(Paths.get(root)) && server.requests.get() == 0)
          spark.conf.set("spark.sql.caseSensitive", "true")
          val expected = stage.transformSchema(input.schema)
          val transformed = stage.transform(input)
          Seq(transformed, AIPrompt.loadOutput(spark, root), stage.resume(spark)).foreach { output =>
            assert(output.schema == expected)
            val row = output.head()
            Seq(row.getAs[Row]("payload"), row.getAs[Seq[Row]]("items").head,
              row.getAs[Map[String, Row]]("mapping")("item")).foreach { value =>
              assert(value.getInt(0) == 1 && value.getInt(1) == 2)
              if (withVectors) assert(value.getMap[Vector, String](2) == Map(Vectors.dense(1.0) -> "weight"))
            }
            assert(row.getAs[String]("answer") == "answer" && row.getAs[Row]("error") == null)
          }
          assert(server.requests.get() == 1)
        }
      } finally spark.conf.set("spark.sql.caseSensitive", caseSensitive)
    }
  }

  private def assertPruned(frame: DataFrame): Unit = {
    val selected = frame.filter(col("id") === 1).select("id", "answer")
    val scans = selected.queryExecution.sparkPlan.collect { case scan: FileSourceScanExec => scan }
    assert(scans.size == 1)
    assert(scans.head.requiredSchema.fieldNames.toSet == Set("id", "answer"))
    assert(scans.head.dataFilters.flatMap(_.references.map(_.name)).contains("id"))
    assert(selected.collect().toSeq == Seq(Row(1, "answer")))
    val counted = frame.agg(count(lit(1)).as("rows"))
    val countScans = counted.queryExecution.sparkPlan.collect { case scan: FileSourceScanExec => scan }
    assert(countScans.forall(_.requiredSchema.isEmpty))
    assert(counted.head().getLong(0) == 2L)
  }

  Seq(false, true).foreach { withVectors =>
    test(s"durable returned output preserves scan pruning and filters with vectors=$withVectors") {
      delta()
      withServer { server =>
        val root = path()
        val stage = prompt(root, server)
        val metadata = new MetadataBuilder().putString("source", "retained").build()
        val primitive = StructType(Seq(StructField("id", IntegerType, nullable = false),
          StructField("text", StringType, nullable = false, metadata)))
        val features = StructType(Seq(
          StructField("vectors", ArrayType(SQLDataTypes.VectorType)),
          StructField("weights", MapType(StringType, SQLDataTypes.VectorType))))
        val schema = if (withVectors) primitive.add("features", features, nullable = true, metadata) else primitive
        val sparse = Vectors.sparse(4, Array(1, 3), Array(2.0, 0.0))
        val missing = Option.empty[Vector].orNull
        val rows = if (withVectors) {
          Arrays.asList(
            Row(1, "first", Row(Seq(sparse, missing), Map("dense" -> Vectors.dense(1.0, 2.0)))),
            Row(2, "second", Row(Option.empty[Seq[Vector]].orNull, Map("missing" -> missing))))
        } else Arrays.asList(Row(1, "first"), Row(2, "second"))
        val input = spark.createDataFrame(rows, schema)
        val transformed = stage.transform(input)
        val loaded = AIPrompt.loadOutput(spark, root)
        val resumed = stage.resume(spark)
        Seq(transformed, loaded, resumed).foreach { output =>
          assert(output.schema == stage.transformSchema(input.schema))
          assert(output.schema("text").metadata == metadata)
          assertPruned(output)
        }
        assertPruned(AIPromptStorageCodec.encode(loaded, loaded.schema))
        if (withVectors) {
          assert(loaded.schema("features").metadata == metadata)
          assert(loaded.select("id", "features").orderBy("id").collect().toSeq ==
            input.select("id", "features").orderBy("id").collect().toSeq)
        }
        assert(server.requests.get() == 2)
      }
    }
  }
}
