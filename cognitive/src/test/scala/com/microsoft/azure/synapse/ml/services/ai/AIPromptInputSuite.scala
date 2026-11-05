// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import com.microsoft.azure.synapse.ml.io.http.{HTTPSchema, URLKey}
import com.microsoft.azure.synapse.ml.param.GlobalParams
import com.microsoft.azure.synapse.ml.services.OpenAIApiVersion
import com.microsoft.azure.synapse.ml.services.aifoundry.AIFoundryModel
import com.microsoft.azure.synapse.ml.services.openai.{OpenAIDecisions, OpenAIEmbeddingDeploymentNameKey}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{Encoders, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spray.json._

import java.util.Collections
import scala.collection.JavaConverters._
import scala.util.Using

class AIPromptInputSuite extends AIPromptTestBase {
  private val chatReply = """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"}}]}"""

  private def contents(server: AIRecordingServer): Vector[String] =
    server.requests.asScala.toVector.map { case (_, body) =>
      AIJson.objectString(body).fields("messages").asInstanceOf[JsArray].elements.last
        .asJsObject.fields("content").asInstanceOf[JsString].value
    }.sorted

  test("inherited optional parameters use fully initialized response format parameters") {
    Seq(new AIPrompt(), new AISystemOne(), new OpenAIDecisions()).foreach { stage =>
      stage.setResponseFormat("json_object")
      assert(stage.getOptionalParams(Row.empty)("response_format") == Map("type" -> "json_object"))
      assert(stage.params.count(_.name == "responseFormat") == 1)
      assert(stage.copy(ParamMap.empty).getOptionalParams(Row.empty)("response_format") ==
        Map("type" -> "json_object"))
    }
  }

  test("null bound API versions produce row errors without requests on dated routes") {
    val previous = GlobalParams.getGlobalParam(OpenAIApiVersion)
    val data = spark.createDataFrame(
      Seq(Row("valid", "selected-version"), Row("missing", Option.empty[String].orNull)).asJava,
      new StructType().add("text", StringType).add("version", StringType))
    try {
      Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
        Seq(false, true).foreach { global =>
          val stage = new AIPrompt().setUrl(server.url).setModel("model").setInputCol("text")
            .setOutputCol("answer").setErrorCol("error")
          if (global) GlobalParams.setGlobalParam(OpenAIApiVersion, Right("version"))
          else stage.setApiVersionCol("version")
          val result = stage.transform(data)
          assert(result.schema == stage.transformSchema(data.schema))
          val rows = result.collect().map(row => row.getAs[String]("text") -> row).toMap
          assert(rows("valid").getAs[String]("answer") == "answer")
          assert(rows("missing").getAs[String]("answer") == null)
          assert(AIJson.objectString(rows("missing").getAs[Row]("error").getString(0))
            .fields("code") == JsString("missing_input"))
        }
        assert(server.requests.size() == 2)
        assert(server.requests.asScala.forall(_._1.endsWith("?api-version=selected-version")))
      }
    } finally {
      previous match {
        case Some(value) => GlobalParams.setGlobalParam(OpenAIApiVersion, value)
        case None => GlobalParams.resetGlobalParam(OpenAIApiVersion)
      }
    }
  }

  test("implicit embedding deployment excludes unused model defaults from planning") {
    val previousModel = GlobalParams.getGlobalParam(AIFoundryModel)
    val previousEmbedding = GlobalParams.getGlobalParam(OpenAIEmbeddingDeploymentNameKey)
    val reply = """{"data":[{"index":0,"embedding":[0.25,0.5]}],"model":"embedding-model"}"""
    val input = spark.createDataFrame(Collections.singletonList(Row("hello")), new StructType().add("text", StringType))
    try {
      GlobalParams.setGlobalParam(OpenAIEmbeddingDeploymentNameKey, Left("embedding-model"))
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        val stage = new AIPrompt().setApiType("embeddings").setUrl(server.url + "/v1").setInputCol("text")
          .setOutputCol("answer").setErrorCol("error")
        GlobalParams.setGlobalParam(AIFoundryModel, Left("unused-model"))
        val expected = stage.transformSchema(input.schema)
        Seq[Either[String, String]](Right("absent-model"), Left("changed-unused-model")).foreach { value =>
          GlobalParams.setGlobalParam(AIFoundryModel, value)
          assert(stage.transformSchema(input.schema) == expected)
          assert(stage.transform(input).head().getAs[Row]("error") == null)
          assert(AIJson.objectString(server.requests.asScala.last._2).fields("model") == JsString("embedding-model"))
        }
        assert(server.requests.size() == 2)
      }
    } finally {
      previousModel match {
        case Some(value) => GlobalParams.setGlobalParam(AIFoundryModel, value)
        case None => GlobalParams.resetGlobalParam(AIFoundryModel)
      }
      previousEmbedding match {
        case Some(value) => GlobalParams.setGlobalParam(OpenAIEmbeddingDeploymentNameKey, value)
        case None => GlobalParams.resetGlobalParam(OpenAIEmbeddingDeploymentNameKey)
      }
    }
  }

  test("wide column validation does not repeatedly traverse input prefixes") {
    val caseSensitive = spark.conf.get("spark.sql.caseSensitive")
    try {
      Seq(false, true).foreach { sensitive =>
        spark.conf.set("spark.sql.caseSensitive", sensitive.toString)
        var reads = 0
        val names = new IndexedSeq[String] {
          override def length: Int = 10000
          override def apply(index: Int): String = {
            reads += 1
            assert(reads <= 5 * length, "Column validation repeatedly traversed the input names")
            s"feature_$index"
          }
        }
        AIColumnPlan.distinct(names, "input columns")
      }
    } finally spark.conf.set("spark.sql.caseSensitive", caseSensitive)
  }

  test("column ambiguity preserves Spark Unicode case comparison in public schema planning") {
    val caseSensitive = spark.conf.get("spark.sql.caseSensitive")
    try {
      Seq(false, true).foreach { sensitive =>
        spark.conf.set("spark.sql.caseSensitive", sensitive.toString)
        Seq("text" -> "TEXT", "i" -> "\u0131", "\u03c3" -> "\u03c2", "\u00e9" -> "e").foreach {
          case (left, right) =>
            val schema = new StructType().add(left, StringType).add(right, StringType)
            val stage = new AIPrompt().setUrl("http://example.invalid/v1").setModel("model")
              .setInputCol(left).setOutputCol("answer").setErrorCol("error")
            if (org.apache.spark.sql.internal.SQLConf.get.resolver(left, right)) {
              assert(intercept[IllegalArgumentException](stage.transformSchema(schema))
                .getMessage.contains("Ambiguous input columns"))
            } else assert(stage.transformSchema(schema).fieldNames.take(2).toSeq == Seq(left, right))
        }
      }
    } finally spark.conf.set("spark.sql.caseSensitive", caseSensitive)
  }

  test("non-row-local templates reject before inference") {
    val data = spark.range(2).selectExpr("id",
      "CASE WHEN id = 0 THEN CAST('2020-01-01 00:00:00' AS TIMESTAMP) END AS ts")
    withServer { server =>
      Seq("{cast(window(ts, '1 hour').start as string)}",
        "{cast(window(ts, '1 hour', '30 minutes').start as string)}",
        "{cast(session_window(ts, '1 hour').start as string)}",
        "{first(cast(id as string))}", "{row_number() over (order by id)}").foreach { template =>
        val stage = prompt(server).setPromptTemplate(template)
        Seq(intercept[IllegalArgumentException](stage.transformSchema(data.schema)),
          intercept[IllegalArgumentException](stage.transform(data))).foreach { error =>
          assert(error.getMessage.contains("row-local") && Option(error.getCause).isEmpty)
        }
        assert(server.requests.get() == 0)
      }
    }
  }

  Seq("SHOW TABLES" -> "tableName", "SHOW COLUMNS" -> "col_name").foreach { case (command, column) =>
    test(s"templates preserve command-result input from $command") {
      val view = "aiprompt_command_" + java.util.UUID.randomUUID().toString.replace("-", "")
      spark.range(1).selectExpr("id AS first", "'value' AS second").createOrReplaceTempView(view)
      try {
        val data = spark.sql(if (command == "SHOW TABLES") s"$command LIKE '$view'" else s"$command IN $view")
        val expected = data.collect().toVector
        assert(expected.nonEmpty)
        Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
          val stage = new AIPrompt().setUrl(server.url + "/v1").setModel("model")
            .setPromptTemplate(s"Describe {$column}").setOutputCol("answer").setErrorCol("error")
          val output = stage.transform(data)
          assert(output.schema == stage.transformSchema(data.schema) && server.requests.isEmpty)
          val rows = output.collect()
          assert(rows.map(_.toSeq.take(data.schema.length)).toSet == expected.map(_.toSeq).toSet)
          assert(rows.forall(row => row.getAs[String]("answer") == "answer" && row.getAs[Row]("error") == null))
          assert(contents(server) == expected.map(row => s"Describe ${row.getAs[String](column)}").sorted)
          assert(server.requests.size() == expected.size)
        }
      } finally spark.catalog.dropTempView(view)
    }
  }

  Seq(
    ("to_json(map(*))", Seq("concat('key-', id) AS first", "cast(id as string) AS second")),
    ("nvl(*)", Seq("CASE WHEN id = 0 THEN 'first' END AS first", "cast(id as string) AS second")),
    ("cast(coalesce(*) as string)", Seq(
      "CASE WHEN id = 0 THEN DATE '2020-01-01' END AS first", "DATE '2020-01-02' AS second")),
    ("to_json(struct(*))", Seq("'duplicate' AS text", "'public' AS _aiprompt_row_public"))
  ).foreach { case (expression, columns) =>
    test(s"wildcard template $expression uses only public columns") {
      val data = spark.range(3).selectExpr(columns: _*)
      val expected = data.selectExpr(expression).collect().map(_.getString(0)).toVector.sorted
      Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
        val stage = new AIPrompt().setUrl(server.url + "/v1").setModel("model")
          .setPromptTemplate(s"{$expression}").setOutputCol("answer").setErrorCol("error")
        val output = stage.transform(data)
        val rows = output.collect()
        assert(output.schema == stage.transformSchema(data.schema) && rows.length == 3)
        assert(rows.forall(row => row.getAs[String]("answer") == "answer" && row.getAs[Row]("error") == null))
        assert(rows.map(row => row.toSeq.take(2)).toSet == data.collect().map(_.toSeq).toSet)
        assert(contents(server) == expected && server.requests.size() == 3)
      }
    }
  }

  test("templates preserve precomputed window columns and null input rows") {
    val data = spark.range(2).selectExpr("id", "row_number() over (order by id) AS ordinal",
      "CASE WHEN id = 0 THEN CAST('2020-01-01 00:00:00' AS TIMESTAMP) END AS ts")
    withServer { server =>
      val stage = prompt(server).setPromptTemplate(
        "{coalesce(cast(ts as string), 'missing')}:{cast(ordinal as string)}")
      val output = stage.transform(data)
      assert(output.schema == stage.transformSchema(data.schema))
      val rows = output.collect().sortBy(_.getAs[Long]("id"))
      assert(rows.map(_.getAs[Int]("ordinal")).toSeq == Seq(1, 2))
      assert(rows(1).isNullAt(rows(1).fieldIndex("ts")))
      assert(rows.forall(row => row.getAs[String]("answer") == "answer" && row.getAs[Row]("error") == null))
      assert(server.requests.get() == 2)
    }
  }

  test("nested message schema attachment validates the whole array before returning") {
    val schema = new StructType().add("messages", ArrayType(new StructType().add("role", StringType)))
    intercept[IndexOutOfBoundsException](AIColumnPlan.withSchema(Row(Seq(Row("user"), Row.empty)), schema))
  }

  test("fused JVM message arrays preserve retained values and invalid row errors") {
    val messageType = new StructType().add("role", StringType)
      .add("content", ArrayType(MapType(StringType, StringType)))
    val schema = new StructType().add("id", "long").add("messages", ArrayType(messageType))
    val input = spark.range(2).map { id =>
      val message = if (id == 0) Row("user", Array(Map("type" -> "text", "text" -> "hello")))
      else Option.empty[Row].orNull
      Row(id, Array(message))
    }(Encoders.row(schema))
    val expected = input.repartition(1).collect().map(row => row.getLong(0) -> row).toMap
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
      val stage = new AIPrompt().setUrl(server.url + "/v1").setModel("model").setMessagesCol("messages")
        .setOutputCol("answer").setErrorCol("error")
      val output = stage.transform(input)
      assert(output.schema == stage.transformSchema(schema))
      val rows = output.collect().map(row => row.getLong(0) -> row).toMap
      assert(rows(0L).getAs[String]("answer") == "answer" && rows(0L).getAs[Row]("error") == null)
      assert(rows(1L).getAs[Row]("error").getString(0).contains("invalid_request"))
      rows.foreach { case (id, row) =>
        assert(row.getAs[Seq[Row]]("messages") == expected(id).getAs[Seq[Row]]("messages"))
      }
      assert(contents(server) == Vector("hello"))
    }
  }

  test("templates support UDFs subqueries and random IV encryption without early evaluation") {
    val session = spark.newSession()
    import session.implicits._
    session.udf.register("aiprompt_render_ticket", (text: String) => "old:" + text)
    // Keep plaintext as an input attribute so Spark does not constant-fold encryption.
    val data = Seq.fill(2)("hello").toDF("text")
    AIPrompt.withSession(session) {
      Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
        Seq("{aiprompt_render_ticket(text)}" -> "old:hello",
          "{concat((SELECT 'old'), ':', text)}" -> "old:hello",
          "{(SELECT max('old'))}:{text}" -> "old:hello").foreach { case (template, expected) =>
          val stage = new AIPrompt().setUrl(server.url + "/v1").setModel("model")
            .setPromptTemplate(template).setOutputCol("answer").setErrorCol("error")
          val output = stage.transform(data.limit(1))
          assert(server.requests.isEmpty && output.schema == stage.transformSchema(data.schema))
          assert(output.head().getAs[String]("answer") == "answer")
          assert(contents(server) == Vector(expected))
          server.requests.clear()
        }
        val encrypted = new AIPrompt().setUrl(server.url + "/v1").setModel("model")
          .setPromptTemplate("{base64(aes_encrypt(text, '0000111122223333'))}")
          .setOutputCol("answer").setErrorCol("error")
        assert(encrypted.transform(data).collect().forall(_.getAs[Row]("error") == null))
        assert(contents(server).size == 2 && contents(server).distinct.size == 2)
      }
    }
  }

  test("schema planning allows row credentials extensions handlers and runtime templates without jobs") {
    val session = spark.newSession()
    session.udf.register("aiprompt_schema_udf", (value: String) => value)
    val schema = new StructType().add("text", StringType).add("credential", StringType)
      .add("headers", MapType(StringType, StringType))
    AIPrompt.withSession(session) {
      withServer { server =>
        val settings: Seq[AIPrompt => AIPrompt] = Seq(
          _.setSubscriptionKeyCol("credential"), _.setAADTokenCol("credential"),
          _.setCustomAuthHeaderCol("credential"), stage => stage.setVectorParam(stage.customHeaders, "headers"),
          stage => stage.setVectorParam(stage.telemHeaders, "headers"), stage => stage.setHandler(stage.getHandler),
          _.setPreviousResponseId("provider-response"), _.setPreviousResponseIdCol("credential"),
          _.setExtraBody(Map("file_id" -> "provider-file")),
          _.setExtraBody(Map("extension" -> Map("file_url" -> "https://example.invalid/media"))))
        val group = java.util.UUID.randomUUID().toString
        session.sparkContext.setJobGroup(group, "Schema-only configuration validation")
        try {
          settings.foreach(configure =>
            configure(prompt(server).setApiType("responses").setUrl(server.url + "/v1")).transformSchema(schema))
          Seq("{current_timestamp()}", "{aiprompt_schema_udf(text)}", "{(select 'external')}", "{upper(text)}:{1 / 0}")
            .foreach(template => prompt(server).setPromptTemplate(template).transformSchema(schema))
          assert(session.sparkContext.statusTracker.getJobIdsForGroup(group).isEmpty)
          assert(server.requests.get() == 0)
        } finally session.sparkContext.clearJobGroup()
      }
    }
  }

  test("telemetry cannot override implicit Fabric classification but explicit endpoints retain caller headers") {
    val previous = GlobalParams.getGlobalParam(URLKey)
    val root = "https://fabric.example.invalid/cognitive/openai"
    val headers = Map("X-Llm-Service-Tier" -> "standard", "x-llm-service-tier" -> "other",
      "X-Taxonomy-TrafficType" -> "Interactive", "x-taxonomy-traffictype" -> "Other",
      "X-Taxonomy-ExtendedProperties" -> "caller", "x-taxonomy-extendedproperties" -> "other", "x-trace" -> "retained")
    try {
      GlobalParams.setGlobalParam(URLKey, root)
      for (perRow <- Seq(false, true); explicitKey <- Seq(false, true); explicitUrl <- Seq(false, true)) {
        val data = spark.range(1).withColumn("text", lit("hello")).withColumn("headers", typedLit(headers))
        val stage = new TestableAIFabricAuthPrompt("fabric-telemetry").setModel("model")
          .setInputCol("text").setOutputCol("answer")
        if (explicitKey) stage.setSubscriptionKey("synthetic-explicit-key")
        if (explicitUrl) stage.setUrl(root)
        if (perRow) stage.setVectorParam(stage.telemHeaders, "headers") else stage.setTelemHeaders(headers)
        val reply = chatReply
        stage.setHandler { (_, request) =>
          val grouped = request.headers.groupBy(_.name.toLowerCase(java.util.Locale.ROOT))
          assert(grouped("x-trace").map(_.value).toSeq == Seq("retained"))
          if (explicitUrl) {
            assert(grouped("x-llm-service-tier").map(_.value).toSet == Set("standard", "other"))
          } else {
            Seq("x-llm-service-tier", "x-taxonomy-traffictype", "x-taxonomy-extendedproperties").foreach { key =>
              assert(grouped(key).length == 1, s"Duplicate owned header $key")
            }
            assert(grouped("x-llm-service-tier").head.value == "flex")
            assert(grouped("x-taxonomy-traffictype").head.value == "Background")
            assert(AIJson.objectString(grouped("x-taxonomy-extendedproperties").head.value)
              .fields("feature") == JsString("synapseml"))
          }
          if (explicitKey) assert(grouped("api-key").head.value == "synthetic-explicit-key")
          HTTPSchema.stringToResponse(reply, 200, "OK")
        }
        assert(stage.transform(data).head().getAs[String]("answer") == "answer")
      }
    } finally {
      previous match {
        case Some(value) => GlobalParams.setGlobalParam(URLKey, value)
        case None => GlobalParams.resetGlobalParam(URLKey)
      }
    }
  }
}
