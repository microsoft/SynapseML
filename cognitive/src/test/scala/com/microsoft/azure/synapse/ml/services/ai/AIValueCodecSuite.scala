// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import org.apache.spark.ml.linalg.{DenseVector, Matrices, SQLDataTypes, SparseVector, Vector, Vectors}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spray.json._

import java.util.Arrays
import scala.util.Using

class AIValueCodecSuite extends AIPromptTestBase {
  test("schema reconciliation widens non-nullable expressions without changing values") {
    val metadata = new MetadataBuilder().putString("source", "schema-reconciliation").build()
    val input = spark.range(1).select(lit("hello").as("text", metadata),
      struct(lit(1).as("value")).as("record"), array(lit(2)).as("items"))
    assert(input.schema.fields.forall(!_.nullable))
    val expected = StructType(input.schema.fields.map(_.copy(nullable = true)))
    val output = AIValueCodec.reconcileSchema(input, expected)
    assert(output.schema == expected)
    assert(output.collect().toSeq == input.collect().toSeq)
  }

  test("negative decimal scales remain valid at every nesting position") {
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
          val stage = prompt(server)
          val output = stage.transform(data)
          assert(output.schema == stage.transformSchema(data.schema))
          val row = output.head()
          assert(row.getAs[String]("answer") == "answer")
          assert(row.getAs[Any]("retained") == data.head().getAs[Any]("retained"))
          assert(server.requests.get() == index + 1)
        }
      }
    } finally previous.fold(spark.conf.unset(setting))(spark.conf.set(setting, _))
  }

  Seq("retained", "input", "extraBody", "responseFormat").foreach { binding =>
    test(s"empty input names remain valid for $binding bindings") {
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
        assert(server.requests.size() == 1)
      }
    }
  }

  test("input names reject nulls and ambiguity and output roles require nonempty names") {
    val stage = new AIPrompt().setUrl("http://localhost/v1").setModel("model").setInputCol("text")
    val valid = new StructType().add("text", StringType)
    val invalid = Seq(valid.add(Option.empty[String].orNull, StringType), valid.add("", StringType).add("", StringType))
    invalid.foreach(schema => intercept[IllegalArgumentException](stage.transformSchema(schema)))
    intercept[IllegalArgumentException](stage.setOutputCol("").transformSchema(valid))
  }

  test("matrix map keys reject before Row conversion instead of losing SQL-distinct entries") {
    val matrices = spark.createDataFrame(Arrays.asList(
      Row(Matrices.dense(1, 1, Array(1.0)), Matrices.sparse(1, 1, Array(0, 1), Array(0), Array(1.0)))),
      new StructType().add("dense", SQLDataTypes.MatrixType).add("sparse", SQLDataTypes.MatrixType))
    val weights = map_from_arrays(array(col("dense"), col("sparse")), array(lit("dense"), lit("sparse")))
    val input = matrices.select(lit("hello").as("text"), weights.as("weights"),
      array(struct(weights.as("weights"))).as("nested"))
    assert(input.select(size(col("weights")), size(col("nested").getItem(0).getField("weights"))).head() ==
      Row(2, 2))
    withServer { server =>
      val stage = new AIPrompt().setUrl(server.url + "/v1").setModel("model").setInputCol("text")
      Seq(input.drop("nested"), input.drop("weights")).foreach { data =>
        Seq[() => Any](() => stage.transformSchema(data.schema), () => stage.transform(data)).foreach { call =>
          assert(intercept[IllegalArgumentException](call()).getMessage.contains("UDT map keys"))
        }
      }
      assert(server.requests.get() == 0)
      val ordinary = matrices.select(lit("hello").as("text"), col("dense"), col("sparse"))
      val output = stage.transform(ordinary)
      assert(output.schema == stage.transformSchema(ordinary.schema))
      assert(output.head().getAs[org.apache.spark.ml.linalg.Matrix]("dense") == Matrices.dense(1, 1, Array(1.0)))
      assert(server.requests.get() == 1)
    }
  }

  test("nullable vector-map containers preserve non-nullable children and null parents") {
    val vectors = spark.createDataFrame(Arrays.asList(Row(Vectors.dense(1.0))),
      new StructType().add("vector", SQLDataTypes.VectorType, nullable = false))
    val weights = map_from_arrays(array(col("vector")), array(lit("weight")))
    val payload = when(col("id") === 0, struct(lit(1).as("k"), weights.as("weights")))
    val input = vectors.crossJoin(spark.range(2)).select(col("id"), lit("hello").as("text"),
      payload.as("payload"), array(payload).as("items"), map(lit("item"), payload).as("mapping"))
    assert(input.schema("payload").nullable)
    assert(input.schema("payload").dataType.asInstanceOf[StructType].fields.forall(!_.nullable))
    withServer { server =>
      val stage = new AIPrompt().setUrl(server.url + "/v1").setModel("model").setInputCol("text")
        .setOutputCol("answer").setErrorCol("error")
      val output = stage.transform(input)
      assert(output.schema == stage.transformSchema(input.schema))
      val rows = output.collect().sortBy(_.getAs[Long]("id"))
      val present = rows.head
      Seq(present.getAs[Row]("payload"), present.getAs[Seq[Row]]("items").head,
        present.getAs[Map[String, Row]]("mapping")("item")).foreach { value =>
        assert(value.getInt(0) == 1 && value.getMap[Vector, String](1) == Map(Vectors.dense(1.0) -> "weight"))
      }
      val absent = rows.last
      val missing = Option.empty[Row].orNull
      assert(absent.getAs[Row]("payload") == null && absent.getAs[Seq[Row]]("items") == Seq(missing))
      assert(absent.getAs[Map[String, Row]]("mapping") == Map("item" -> missing))
      assert(rows.forall(row => row.getAs[String]("answer") == "answer" && row.getAs[Row]("error") == null))
      assert(server.requests.get() == 2)
    }
  }

  Seq(false, true).foreach { withVectors =>
    test(s"case-distinct nested fields are retained by ordinal with vectors=$withVectors") {
      val caseSensitive = spark.conf.get("spark.sql.caseSensitive")
      try {
        spark.conf.set("spark.sql.caseSensitive", "false")
        val vectors = spark.createDataFrame(Arrays.asList(Row(Vectors.dense(1.0))),
          new StructType().add("vector", SQLDataTypes.VectorType))
        val fields = Seq(lit(1).as("a"), lit(2).as("A")) ++ (if (withVectors)
          Seq(map_from_arrays(array(col("vector")), array(lit("weight"))).as("weights")) else Seq.empty)
        val payload = struct(fields: _*)
        val metadata = new MetadataBuilder().putString("source", "case-distinct").build()
        val input = vectors.select(lit("hello").as("text"), payload.as("payload", metadata),
          array(payload).as("items"), map(lit("item"), payload).as("mapping"))
        val reply = """{"choices":[{"index":0,"message":{"role":"assistant","content":"{\"value\":\"ok\"}"}}]}"""
        Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
          Seq(false, true).foreach { parsed =>
            val stage = new AIPrompt().setUrl(server.url + "/v1").setModel("model").setInputCol("text")
              .setOutputCol("answer").setErrorCol("error")
            if (parsed) stage.setPostProcessingOptions(Map("jsonSchema" -> "value STRING"))
            val output = stage.transform(input)
            assert(output.schema == stage.transformSchema(input.schema))
            val row = output.head()
            Seq(row.getAs[Row]("payload"), row.getAs[Seq[Row]]("items").head,
              row.getAs[Map[String, Row]]("mapping")("item")).foreach { value =>
              assert(value.getInt(0) == 1 && value.getInt(1) == 2)
              if (withVectors) assert(value.getMap[Vector, String](2) == Map(Vectors.dense(1.0) -> "weight"))
            }
            assert(row.getAs[Row]("error") == null)
            if (parsed) assert(row.getAs[Row]("answer").getString(0) == "ok")
            else assert(row.getAs[String]("answer") == """{"value":"ok"}""")
          }
          val replacing = new AIPrompt().setUrl(server.url + "/v1").setModel("model").setInputCol("text")
            .setOutputCol("payload")
          val output = replacing.transform(input)
          assert(output.schema == replacing.transformSchema(input.schema))
          assert(output.head().getAs[String]("payload") == """{"value":"ok"}""")
          assert(server.requests.size() == 3)
        }
      } finally spark.conf.set("spark.sql.caseSensitive", caseSensitive)
    }
  }

  test("vector map keys retain SQL identity through inference and output replacement") {
    withServer { server =>
      val stage = prompt(server)
      val schema = new StructType().add("dense", SQLDataTypes.VectorType).add("sparse", SQLDataTypes.VectorType)
      val vectors = spark.createDataFrame(Arrays.asList(
        Row(Vectors.dense(1.0), Vectors.sparse(1, Array(0), Array(1.0)))), schema)
      val weights = map_from_arrays(array(col("dense"), col("sparse")), array(lit("dense"), lit("sparse")))
      val vectorValues = map_from_arrays(array(col("dense"), col("sparse")), array(col("sparse"), col("dense")))
      val nested = array(struct(weights.as("weights"), array(col("dense"), col("sparse")).as("vectors")))
      val optional = struct(when(lit(false), weights).as("weights"), when(lit(false), nested).as("nested"))
      val metadata = new MetadataBuilder().putString("source", "vector-key-map").build()
      val input = vectors.select(lit("test").as("text"), weights.as("weights", metadata), nested.as("nested"),
        vectorValues.as("vectorValues"), optional.as("optional"), when(lit(false), optional).as("missing"))
      assert(input.select(size(col("weights")), size(col("nested").getItem(0).getField("weights"))).head() ==
        Row(2, 2))
      val output = stage.transform(input).cache()
      try {
        assert(output.schema == stage.transformSchema(input.schema))
        Seq(col("weights"), col("nested").getItem(0).getField("weights")).foreach { column =>
          val entries = output.select(explode(column)).collect()
          assert(entries.length == 2)
          val keys = entries.map(row => row.getString(1) -> row.getAs[Vector](0)).toMap
          assert(keys("dense").isInstanceOf[DenseVector] && keys("sparse").isInstanceOf[SparseVector])
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
      } finally output.unpersist()
      val replacing = stage.setOutputCol("weights")
      val replaced = replacing.transform(input).cache()
      try {
        assert(replaced.schema == replacing.transformSchema(input.schema))
        assert(replaced.select(col("weights"), size(col("nested").getItem(0).getField("weights"))).head() ==
          Row("answer", 2))
      } finally replaced.unpersist()
      assert(server.requests.get() == 2)
    }
  }
}
