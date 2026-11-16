// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import com.microsoft.azure.synapse.ml.core.test.base.SparkSessionManagement
import com.microsoft.azure.synapse.ml.io.http.URLKey
import com.microsoft.azure.synapse.ml.param.GlobalParams
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.apache.spark.SparkConf
import org.apache.spark.ml.linalg.{SQLDataTypes, SparseVector, Vector, Vectors}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.hadoop.fs.{LocalFileSystem, Path}
import spray.json._

import java.io.File
import java.net.{InetSocketAddress, URI}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermissions
import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger
import scala.util.Try
import scala.util.Using

private[ai] object AIPromptSessionFileSystem {
  val Checks: AtomicInteger = new AtomicInteger()
}

class AIPromptSessionFileSystem extends LocalFileSystem {
  override def getWorkingDirectory: Path = Option(getConf)
    .flatMap(conf => Option(conf.get("aiprompt.test.workingDir"))).map(new Path(_))
    .getOrElse(super.getWorkingDirectory)

  override def exists(path: Path): Boolean = {
    AIPromptSessionFileSystem.Checks.incrementAndGet()
    super.exists(path)
  }
}

private[ai] object AIPromptDeltaTestSession extends SparkSessionManagement {
  def hasDelta: Boolean = Try(Class.forName("org.apache.spark.sql.delta.sources.DeltaDataSource")).isSuccess

  override def stopSparkSession(): Unit = {
    super.stopSparkSession()
    SparkSession.getActiveSession.filter(_.sparkContext.isStopped).foreach(_ => SparkSession.clearActiveSession())
    SparkSession.getDefaultSession.filter(_.sparkContext.isStopped).foreach(_ => SparkSession.clearDefaultSession())
  }

  override def sparkConfiguration: SparkConf = {
    val conf = super.sparkConfiguration
      .set("spark.sql.shuffle.partitions", "2")
      .set("spark.default.parallelism", "2")
      .set("spark.databricks.delta.snapshotPartitions", "1")
    if (hasDelta) conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    conf
  }
}

private[ai] class AIPromptRecoveryRecordingServer extends AutoCloseable {
  val requests: AtomicInteger = new AtomicInteger()
  private val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
  server.createContext("/", new HttpHandler {
    override def handle(exchange: HttpExchange): Unit = {
      requests.incrementAndGet()
      val stream = exchange.getRequestBody
      val source = scala.io.Source.fromInputStream(stream, "UTF-8")
      val body = try source.mkString finally source.close()
      val failed = body.contains("ROW_ERROR")
      val response = if (failed) {
        """{"error":{"code":"invalid_request","message":"synthetic row error"}}"""
      } else if (exchange.getRequestURI.getPath.endsWith("/embeddings")) {
        """{"object":"list","model":"local-model","data":[{"object":"embedding",
          |"index":0,"embedding":[1.25,0.0,-2.5]}],"usage":{"prompt_tokens":1,"total_tokens":1}}""".stripMargin
      } else {
        """{"id":"local-response","model":"local-model","choices":[{"index":0,
          |"message":{"role":"assistant","content":"answer"},"finish_reason":"stop"}],
          |"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}""".stripMargin
      }
      val bytes = response.getBytes(StandardCharsets.UTF_8)
      exchange.getResponseHeaders.add("Content-Type", "application/json")
      exchange.sendResponseHeaders(if (failed) 400 else 200, bytes.length)
      val output = exchange.getResponseBody
      try output.write(bytes) finally output.close()
    }
  })
  server.start()
  val url: String = s"http://127.0.0.1:${server.getAddress.getPort}/v1/chat/completions"
  override def close(): Unit = server.stop(0)
}

private[ai] object AIPromptStorageTestConfig {
  def configure(prompt: AIPrompt, path: String, url: String): AIPrompt = {
    prompt.setApiType("chat_completions")
      .setUrl(url)
      .setDeploymentName("local-model")
      .setSubscriptionKey("synthetic-key")
      .setPromptTemplate("{text}")
      .setOutputCol("answer")
      .setErrorCol("error")
      .setUsageCol("usage")
      .setRawResponseCol("raw")
      .setOutputBatchSize(2)
      .setOutputPath(path)
  }
}

class AIPromptOutputSuite extends AIPromptOutputTestBase {
  test("implicit Fabric scalar model preflight rejects unavailable models before creating durable output") {
    delta()
    val previous = GlobalParams.getGlobalParam(URLKey)
    val root = path()
    try {
      GlobalParams.setGlobalParam(URLKey, "https://fabric.example.invalid/cognitive/openai")
      val input = spark.createDataFrame(Collections.singletonList(Row("hello")),
        new StructType().add("text", StringType))
      val stage = new TestableAIFabricAuthPrompt("fabric-durable-preflight").setModel("blocked-model")
        .setInputCol("text").setOutputPath(root)
      stage.transformSchema(input.schema)
      val error = intercept[IllegalArgumentException](stage.transform(input))
      assert(error.getMessage.contains("Synthetic Fabric model is unavailable"))
      assert(!Files.exists(new File(root).toPath))
    } finally {
      previous match {
        case Some(value) => GlobalParams.setGlobalParam(URLKey, value)
        case None => GlobalParams.resetGlobalParam(URLKey)
      }
    }
  }

  test("storage schema and codec preserve sparse vectors, nested metadata, and nullable values") {
    val metadata = new MetadataBuilder().putString("description", "documented metadata").build()
    val nested = StructType(Seq(StructField("vector", SQLDataTypes.VectorType, nullable = false, metadata)))
    val schema = StructType(Seq(
      StructField("vectors", ArrayType(nested, containsNull = false), nullable = false, metadata),
      StructField("labels", MapType(StringType, IntegerType, valueContainsNull = false), nullable = false)))
    val planned = AIPromptOutput.storageSchema(schema)
    val json = AIPromptStorageCodec.schemaJson(planned)
    assert(AIPromptStorageCodec.readSchema(json) == planned)
    assert(!json.contains("VectorUDT"))
    val sparse = Vectors.sparse(4, Array(1, 3), Array(2.0, 0.0))
    val row = Row(Seq(Row(sparse), Row(Vectors.dense(1.0, 2.0))), Map("one" -> 1))
    val input = spark.createDataFrame(Collections.singletonList(row), schema)
    val decoded = AIPromptStorageCodec.decode(AIPromptStorageCodec.encode(input, planned), planned)
    assert(decoded.schema == planned)
    val result = decoded.head().getSeq[Row](0).head.getAs[Vector](0)
    assert(result == sparse)
    assert(result.isInstanceOf[SparseVector])
    assert(planned("vectors").metadata == metadata)
    assert(planned("vectors").dataType.asInstanceOf[ArrayType].containsNull)
    val missing = Row(Seq(Option.empty[Row].orNull, Row(Option.empty[Vector].orNull)),
      Map("missing" -> Option.empty[Integer].orNull))
    val nullableInput = spark.createDataFrame(Collections.singletonList(missing), planned)
    val nullableOutput = AIPromptStorageCodec.decode(AIPromptStorageCodec.encode(nullableInput, planned), planned)
    assert(nullableOutput.head() == missing)
  }

  test("storage schemas reject arbitrary class metadata and incompatible Delta names") {
    intercept[IllegalArgumentException] {
      AIPromptStorageCodec.readSchema("""{"type":"udt","class":"do.not.load.ArbitraryClass"}""")
    }
    intercept[IllegalArgumentException] {
      AIPromptOutput.storageSchema(StructType(Seq(StructField("bad name", StringType))))
    }
    intercept[IllegalArgumentException] {
      AIPromptOutput.storageSchema(StructType(Seq(StructField("x", NullType))))
    }
    intercept[IllegalArgumentException] {
      AIPromptOutput.storageSchema(StructType(Seq(StructField("matrix", SQLDataTypes.MatrixType))))
    }
    intercept[IllegalArgumentException] {
      AIPromptOutput.storageSchema(StructType(Seq(StructField("A", StringType), StructField("a", StringType))))
    }
  }

  test("vector decoding rejects missing stored elements instead of silently producing zero") {
    val logical = StructType(Seq(StructField("vector", SQLDataTypes.VectorType)))
    val physical = AIPromptStorageCodec.physical(logical).asInstanceOf[StructType]
    val corrupt = Row(Row(false, 2, Seq.empty[Int], Seq[Any](1.0, Option.empty[java.lang.Double].orNull)))
    val frame = spark.createDataFrame(Collections.singletonList(corrupt), physical)
    val error = intercept[Exception](AIPromptStorageCodec.decode(frame, logical).collect())
    val causes = Iterator.iterate(Option(error: Throwable))(_.flatMap(e => Option(e.getCause)))
      .takeWhile(_.nonEmpty).flatten
    assert(causes.exists(e => Option(e.getMessage).exists(_.contains("Invalid encoded AIPrompt vector elements"))))
  }

  test("missing Delta fails before creating a run") {
    assume(!AIPromptDeltaTestSession.hasDelta, "Run in the no-Delta classpath profile.")
    val root = path()
    val error = intercept[IllegalStateException] {
      AIPromptDelta.requireRuntime(spark, AIPromptDelta.qualify(spark, root))
    }
    assert(error.getMessage.contains("Delta runtime"))
    assert(!new File(root).exists())
  }

  test("fingerprint rotates scalar authentication but retains request settings") {
    val schema = StructType(Seq(StructField("text", StringType)))
    val first = new AIPrompt("same").setSubscriptionKey("first").setPromptTemplate("{text}")
    val second = new AIPrompt("same").setSubscriptionKey("second").setPromptTemplate("{text}")
    val output = StructType(Seq(StructField("answer", StringType)))
    assert(AIPromptOutput.fingerprint(first, schema, output, spark) ==
      AIPromptOutput.fingerprint(second, schema, output, spark))
    val differentUid = new AIPrompt("different").setSubscriptionKey("rotated").setPromptTemplate("{text}")
    differentUid.restoreDurableOutputDefaults(Map(
      "outputCol" -> first.getOutputCol, "errorCol" -> first.getErrorCol, "messagesCol" -> first.getMessagesCol))
    assert(!differentUid.isSet(differentUid.responseIdCol))
    assert(AIPromptOutput.fingerprint(first, schema, output, spark) ==
      AIPromptOutput.fingerprint(differentUid, schema, output, spark))
    second.setCustomHeaders(Map("Authorization" -> "Bearer rotated"))
    assert(AIPromptOutput.fingerprint(first, schema, output, spark) ==
      AIPromptOutput.fingerprint(second, schema, output, spark))
    second.setCustomHeaders(Map("x-request-behavior" -> "different"))
    assert(AIPromptOutput.fingerprint(first, schema, output, spark) !=
      AIPromptOutput.fingerprint(second, schema, output, spark))
    second.setPromptTemplate("different {text}")
    assert(AIPromptOutput.fingerprint(first, schema, output, spark) !=
      AIPromptOutput.fingerprint(second, schema, output, spark))
    val customHandler = new AIPrompt("same")
    customHandler.setHandler(customHandler.getHandler)
    intercept[IllegalArgumentException](AIPromptOutput.fingerprint(customHandler, schema, output, spark))
  }

  test("durable results are eager, bounded, terminal, storage-backed and keep duplicate source rows") {
    delta()
    withServer { server =>
      val session = spark
      import session.implicits._
      val root = path()
      val stage = prompt(root, server)
      val input = Seq("duplicate", "duplicate", "ROW_ERROR").toDF("text")
      val result = stage.transform(input)
      assert(server.requests.get() == 3)
      assert(result.schema == stage.transformSchema(input.schema))
      assert(result.count() == 3)
      result.collect()
      assert(server.requests.get() == 3)
      val status = AIPromptOutput.getOutputStatus(spark, root)
      assert(status.isComplete && !status.allSucceeded && status.rowErrorCount == 1)
      assert(status.committedCount == 3 && status.pendingCount == 0 && status.ownerToken.isEmpty)
      val manifest = AIPromptDelta.control(spark, AIPromptDelta.qualify(spark, root))
      val snapshot = AIPromptDelta.input(spark, AIPromptDelta.qualify(spark, root), manifest)
      assert(snapshot.select(manifest.keyColumn).distinct().count() == 3)
      val text = spark.read.format("delta").load(AIPromptDelta.child(root, "_control")).toJSON.head()
      assert(!text.contains("synthetic-key") && !text.contains("{text}") && !text.contains(server.url))
      val controlVersion = AIPromptDelta.version(spark, root, "_control")
      val resumed = AIPromptOutput.resumeObserved(prompt(root, server).setSubscriptionKey("rotated"), spark,
        event => fail(s"Completed resume must not acquire ownership or write state: $event"))
      assert(resumed.count() == 3 && server.requests.get() == 3)
      assert(AIPromptDelta.version(spark, root, "_control") == controlVersion)
      assert(AIPromptDelta.control(spark, root) == manifest)
      intercept[IllegalArgumentException](stage.transform(input))
    }
  }

  test("durable malformed body columns commit terminal row errors instead of aborting initialization") {
    delta()
    withServer { server =>
      val session = spark
      import session.implicits._
      val root = path()
      val stage = prompt(root, server).setExtraBodyCol("body")
      val input = Seq(("valid", "{}"), ("malformed", "{"), ("nonobject", "[]"),
        ("duplicate", """{"x":1,"x":2}""")).toDF("text", "body")
      val output = stage.transform(input)
      assert(output.count() == 4 && server.requests.get() == 1)
      val rows = output.collect()
      assert(rows.count(_.getAs[Row]("error") == null) == 1)
      assert(rows.filter(_.getAs[Row]("error") != null)
        .forall(_.getAs[Row]("error").getString(0).contains("invalid_request")))
      val status = AIPrompt.getOutputStatus(spark, root)
      assert(status.isComplete && status.rowErrorCount == 3)
      assert(stage.resume(spark).count() == 4 && AIPrompt.loadOutput(spark, root).count() == 4)
      assert(server.requests.get() == 1)
    }
  }

  test("empty input initializes typed readable output without HTTP") {
    delta()
    withServer { server =>
      val input = spark.createDataFrame(Collections.emptyList[Row](),
        StructType(Seq(StructField("text", StringType, nullable = false))))
      val root = path()
      val stage = prompt(root, server)
      val output = stage.transform(input)
      assert(output.schema == stage.transformSchema(input.schema))
      assert(output.count() == 0 && server.requests.get() == 0)
      assert(AIPromptOutput.getOutputStatus(spark, root).allSucceeded)
    }
  }

  test("durable paths and directory checks honor the supplied session filesystem configuration") {
    delta()
    withServer { server =>
      val session = spark.newSession()
      import session.implicits._
      val root = path()
      val original = Option(spark.sparkContext.hadoopConfiguration.get("fs.file.impl"))
      session.conf.set("fs.file.impl", classOf[AIPromptSessionFileSystem].getName)
      session.conf.set("fs.file.impl.disable.cache", "true")
      session.conf.set("aiprompt.test.workingDir", new File(root).toURI.toString)
      val qualified = AIPromptDelta.qualify(session, "run")
      assert(new File(new java.net.URI(qualified)) == new File(root, "run"))
      AIPromptSessionFileSystem.Checks.set(0)
      val stage = prompt(qualified, server)
      val output = AIPromptOutput.transformObserved(stage, Seq("one").toDF("text"), event =>
        if (event == "before_control_create") assert(AIPromptSessionFileSystem.Checks.get() > 0))
      assert(output.count() == 1 && server.requests.get() == 1)
      assert(AIPrompt.getOutputStatus(session, qualified).allSucceeded)
      assert(Option(spark.sparkContext.hadoopConfiguration.get("fs.file.impl")) == original)
    }
  }

  test("durable malformed Decisions JSON commits terminal row errors without blocking valid rows") {
    delta()
    Using.resource(new AIRecordingServer((_, _) => 200 -> NativeRequestFixtures.DecisionResponse)) { server =>
      val session = spark
      import session.implicits._
      val root = path()
      val stage = new AIPrompt().setApiType("decisions").setUrl(server.url + "/v1").setModel("model")
        .setInputCol("input").setInputMode("json").setQuestions(NativeRequestFixtures.DecisionQuestions)
        .setOutputCol("answer").setErrorCol("error").setOutputPath(root).setOutputBatchSize(10)
      val data = Seq("\"valid\"", "{", "null", """[{"role":"system","content":"bad role"}]""",
        """[{"role":"user","content":[]}]""").toDF("input")
      val result = stage.transform(data)
      assert(result.count() == 5 && server.requests.size() == 1)
      val errors = result.select("error").collect().flatMap(row => Option(row.getAs[Row](0)))
      assert(errors.length == 4 && errors.forall(_.getString(0).contains("invalid_request")))
      assert(AIPrompt.getOutputStatus(spark, root).rowErrorCount == 4)
      assert(stage.resume(spark).count() == 5 && AIPrompt.loadOutput(spark, root).count() == 5)
      assert(server.requests.size() == 1)
    }
  }

  test("durable malformed messages commit row errors while valid messages are sent once") {
    delta()
    withServer { server =>
      val root = path()
      val schema = StructType(Seq(StructField("messages", ArrayType(
        StructType(Seq(StructField("role", StringType), StructField("content", StringType)))))))
      val data = spark.createDataFrame(java.util.Arrays.asList(
        Row(Seq(Row("user", "valid"))), Row(Seq(Row("user", Option.empty[String].orNull))),
        Row(Seq(Option.empty[Row].orNull))), schema)
      val stage = prompt(root, server)
      stage.clear(stage.promptTemplate)
      stage.setMessagesCol("messages")
      val result = stage.transform(data)
      assert(result.count() == 3 && server.requests.get() == 1)
      assert(result.select("error").collect().count(!_.isNullAt(0)) == 2)
      assert(AIPrompt.getOutputStatus(spark, root).rowErrorCount == 2)
      assert(stage.resume(spark).count() == 3 && server.requests.get() == 1)
    }
  }

  test("durable extensions reject nested resources and provider retrieval before inference") {
    delta()
    withServer { server =>
      val session = spark
      import session.implicits._
      val data = Seq("one").toDF("text")
      val bodies: Seq[Map[String, Any]] = Seq(
        Map("attachments" -> Seq(Map("file_id" -> "synthetic-file"))),
        Map("data_sources" -> Seq(Map("type" -> "azure_search",
          "parameters" -> Map("endpoint" -> "https://search.example.invalid", "index_name" -> "docs")))),
        Map("web_search_options" -> Map.empty[String, Any]),
        Map("prompt" -> Map("id" -> "synthetic-stored-prompt")))
      bodies.foreach { body =>
        val root = path()
        intercept[IllegalArgumentException](prompt(root, server).setExtraBody(body).transform(data))
        assert(!new File(root).exists() && server.requests.get() == 0)
      }
      val bodyRows = bodies.map(body => ("one", AIJson.fromAny(body).compactPrint)).toDF("text", "body")
      intercept[org.apache.spark.SparkException] {
        prompt(path(), server).setExtraBodyCol("body").transform(bodyRows)
      }
      assert(server.requests.get() == 0)
      val schema = Map("type" -> "object", "properties" -> Map("file_id" -> Map("type" -> "string"),
        "prompt" -> Map("type" -> "string")))
      val inert = Map("response_format" -> Map("type" -> "json_schema",
        "json_schema" -> Map("name" -> "answer", "schema" -> schema)), "provider_option" -> true)
      assert(prompt(path(), server).setExtraBody(inert).transform(data).count() == 1)
      assert(server.requests.get() == 1)
    }
  }

  test("corrupt mandatory output bindings fail status read and resume instead of fabricating success") {
    delta()
    withServer { server =>
      val session = spark
      import session.implicits._
      val root = path()
      val stage = prompt(root, server)
      stage.transform(Seq("ROW_ERROR").toDF("text"))
      val original = AIPromptDelta.control(spark, root).bindings.parseJson.asJsObject.fields
      val corrupt = Seq(original - "errorCol", original - "outputCol",
        original.updated("errorCol", JsString("answer")), original.updated("errorCol", JsNull),
        original.updated("outputCol", original("errorCol")))
      corrupt.foreach { bindings =>
        val json = JsObject(bindings).compactPrint
        spark.sql(s"UPDATE ${AIPromptDelta.table(root, "_control")} SET bindings = ${AIPromptDelta.literal(json)}")
        intercept[IllegalArgumentException](AIPrompt.getOutputStatus(spark, root))
        intercept[IllegalArgumentException](AIPrompt.loadOutput(spark, root))
        intercept[IllegalArgumentException](stage.resume(spark))
      }
      assert(server.requests.get() == 1)
    }
  }

  test("durable multibatch ledger workload records Spark job counts without replay") {
    delta()
    withServer { server =>
      val group = java.util.UUID.randomUUID().toString
      val input = spark.range(8).select(col("id").cast("string").as("text"))
      val stage = prompt(path(), server)
      spark.sparkContext.setJobGroup(group, "AIPrompt durable ledger workload")
      val start = System.nanoTime()
      try {
        val result = stage.transform(input)
        assert(result.count() == 8 && server.requests.get() == 8)
        val jobs = spark.sparkContext.statusTracker.getJobIdsForGroup(group).length
        val seconds = (System.nanoTime() - start).toDouble / 1e9
        info(f"Durable ledger workload: rows=8 batches=4 jobs=$jobs seconds=$seconds%.2f")
      } finally spark.sparkContext.clearJobGroup()
    }
  }

  test("real Delta restores embedding VectorType and metadata without retaining inference") {
    delta()
    withServer { server =>
      val session = spark
      import session.implicits._
      val root = path()
      val stage = prompt(root, server).setApiType("embeddings")
        .setUrl(server.url.replace("chat/completions", "embeddings")).setTextCol("text")
      stage.clear(stage.promptTemplate)
      val metadata = new MetadataBuilder().putString("source", "retained").build()
      val input = Seq("embedding input").toDF("text").select(col("text").as("text", metadata))
      val output = stage.transform(input)
      assert(output.schema == stage.transformSchema(input.schema))
      assert(output.schema("answer").dataType == SQLDataTypes.VectorType)
      assert(output.schema("text").metadata == metadata)
      assert(output.head().getAs[Vector]("answer") == Vectors.dense(1.25, 0.0, -2.5))
      assert(AIPromptOutput.loadOutput(spark, root).head().getAs[Vector]("answer") == Vectors.dense(1.25, 0.0, -2.5))
      assert(server.requests.get() == 1)
    }
  }

  test("an ambiguous acknowledged commit retains ownership and resumes committed errors without replay") {
    delta()
    withServer { server =>
      val session = spark
      import session.implicits._
      val root = path()
      val stage = prompt(root, server)
      intercept[IllegalStateException] {
        AIPromptOutput.transformObserved(stage, Seq("ok", "ROW_ERROR", "last").toDF("text"), event =>
          if (event == "batch_committed") throw new IllegalStateException("simulated lost acknowledgement"))
      }
      val partial = AIPromptOutput.getOutputStatus(spark, root)
      assert(partial.committedCount == 2 && partial.pendingCount == 1 && partial.ownerToken.isDefined)
      assert(AIPromptOutput.loadOutput(spark, root).count() == 2 && server.requests.get() == 2)
      intercept[IllegalArgumentException](stage.resume(spark))
      intercept[IllegalArgumentException] {
        AIPromptOutput.releaseOutputOwner(spark, root, partial.ownerToken.get, confirmStopped = false)
      }
      intercept[IllegalArgumentException] {
        AIPromptOutput.releaseOutputOwner(spark, root, "wrong-owner", confirmStopped = true)
      }
      AIPromptOutput.releaseOutputOwner(spark, root, partial.ownerToken.get, confirmStopped = true)
      assert(prompt(root, server).setSubscriptionKey("new-key").setOutputBatchSize(1).resume(spark).count() == 3)
      assert(server.requests.get() == 3)
    }
  }

  test("every incomplete initialization boundary fails explicitly without HTTP") {
    delta()
    withServer { server =>
      val session = spark
      import session.implicits._
      Seq("control_created", "input_committed", "results_created").foreach { boundary =>
        val root = path()
        intercept[IllegalStateException] {
          AIPromptOutput.transformObserved(prompt(root, server), Seq("ok").toDF("text"), event =>
            if (event == boundary) throw new IllegalStateException("initialization failure"))
        }
        val failure = intercept[IllegalArgumentException](AIPromptOutput.loadOutput(spark, root))
        assert(failure.getMessage.contains("initialization is incomplete"))
        assert(failure.getMessage.contains("new outputPath"))
        val statusFailure = intercept[IllegalArgumentException](AIPrompt.getOutputStatus(spark, root))
        assert(statusFailure.getMessage.contains("new outputPath"))
        intercept[IllegalArgumentException](prompt(root, server).resume(spark))
      }
      assert(server.requests.get() == 0)
    }
  }

  test("resume rejects request and output changes before HTTP and restores implicit UID output bindings") {
    delta()
    withServer { server =>
      val session = spark
      import session.implicits._
      val root = path()
      val first = prompt(root, server)
      first.clear(first.outputCol)
      first.clear(first.errorCol)
      first.transform(Seq(java.sql.Timestamp.valueOf("2026-01-01 12:00:00")).toDF("text"))
      val second = prompt(root, server)
      second.clear(second.outputCol)
      second.clear(second.errorCol)
      assert(second.uid != first.uid)
      assert(second.resume(spark).columns.contains(first.getOutputCol))
      val timeZone = spark.conf.get("spark.sql.session.timeZone")
      try {
        spark.conf.set("spark.sql.session.timeZone", if (timeZone == "UTC") "America/Los_Angeles" else "UTC")
        val failure = intercept[IllegalArgumentException](second.resume(spark))
        assert(failure.getMessage.contains("configuration, public output schema or Spark SQL semantics"))
      } finally spark.conf.set("spark.sql.session.timeZone", timeZone)
      intercept[IllegalArgumentException](prompt(root, server).setOutputCol("changed").resume(spark))
      intercept[IllegalArgumentException](second.setPromptTemplate("changed {text}").resume(spark))
      assert(server.requests.get() == 1)
    }
  }

  test("resume restores retained generated messages after input and output collisions with a different UID") {
    delta()
    withServer { server =>
      val session = spark
      import session.implicits._
      val root = path()
      val first = prompt(root, server).setDropPrompt(false).setOutputBatchSize(1)
      val base = first.getMessagesCol
      first.setOutputCol(base + "_2")
      val input = Seq(("one", "user-base-one", "user-next-one"), ("two", "user-base-two", "user-next-two"))
        .toDF("text", base, base + "_1")
      val expected = first.transformSchema(input.schema)
      val retained = base + "_3"
      assert(expected.fieldNames.contains(retained))
      intercept[IllegalStateException] {
        AIPromptOutput.transformObserved(first, input, event =>
          if (event == "batch_committed") throw new IllegalStateException("interrupted after a committed batch"))
      }
      val manifest = AIPromptDelta.control(spark, AIPromptDelta.qualify(spark, root))
      assert(manifest.outputType == expected)
      assert(AIPromptOutput.loadOutput(spark, root).schema == expected)
      val status = AIPromptOutput.getOutputStatus(spark, root)
      AIPromptOutput.releaseOutputOwner(spark, root, status.ownerToken.get, confirmStopped = true)
      val second = prompt(root, server).setDropPrompt(false).setOutputCol(first.getOutputCol)
      val untouchedDefault = second.getMessagesCol
      assert(untouchedDefault != base && second.uid != first.uid)
      val resumed = second.resume(spark)
      assert(resumed.schema == expected && resumed.count() == 2)
      assert(second.getMessagesCol == untouchedDefault && !second.isSet(second.messagesCol))
      val preserved = resumed.select(AIPromptDelta.column(base), AIPromptDelta.column(base + "_1")).collect()
        .map(row => row.getString(0) -> row.getString(1)).toSet
      assert(preserved == Set("user-base-one" -> "user-next-one", "user-base-two" -> "user-next-two"))
      assert(resumed.select(AIPromptDelta.column(retained)).head().getSeq[Row](0).nonEmpty)
      assert(server.requests.get() == 2)
    }
  }

  test("saved input is evaluated once and unsupported inputs fail before run creation") {
    delta()
    withServer { server =>
      val evaluations = spark.sparkContext.longAccumulator("durable-source-evaluations")
      val source = udf { id: Long => evaluations.add(1); s"row-$id" }
      val input = spark.range(3).select(source(col("id")).as("text"))
      prompt(path(), server).transform(input).count()
      assert(evaluations.value == 3L)
      val root = path()
      val bad = input.withColumn("bad name", lit(1))
      intercept[IllegalArgumentException](prompt(root, server).transform(bad))
      assert(!new File(root).exists())
      assert(server.requests.get() == 3)
    }
  }

  test("credential columns fail before snapshot creation or HTTP even with distinct per-row principals") {
    delta()
    withServer { server =>
      val session = spark
      import session.implicits._
      val input = Seq(("one", "synthetic-principal-one"), ("two", "synthetic-principal-two"))
        .toDF("text", "credential")
        .withColumn("headers", map(lit("Authorization"), col("credential")))
      val settings: Seq[AIPrompt => Unit] = Seq(
        stage => stage.setSubscriptionKeyCol("credential"),
        stage => stage.setAADTokenCol("credential"),
        stage => stage.setCustomAuthHeaderCol("credential"),
        stage => stage.setVectorParam(stage.customHeaders, "headers"))
      settings.foreach { setting =>
        val root = path()
        val stage = prompt(root, server)
        setting(stage)
        intercept[IllegalArgumentException](stage.transform(input))
        assert(!new File(root).exists())
      }
      assert(server.requests.get() == 0)
    }
  }

  test("durable validation rejects provider resource bindings without a storage dependency") {
    val session = spark
    import session.implicits._
    val input = Seq("ordinary https://example.invalid text").toDF("text")
    val stage = new AIPrompt().setInputCol("text")
    stage.validateDurableInput(input.schema, session)
    stage.validateDurableRows(input)
    intercept[IllegalArgumentException] {
      stage.setPreviousResponseId("saved-response").validateDurableInput(input.schema, session)
    }
    stage.clear(stage.previousResponseId)
    Seq("conversation", "file_id", "tools", "retrieve", "run_id").foreach { key =>
      stage.setExtraBody(Map("include" -> Seq("message.output_text.logprobs"), key -> "external-resource"))
      intercept[IllegalArgumentException](stage.validateDurableInput(input.schema, session))
    }
    stage.setExtraBodyCol("body")
    stage.validateDurableRows(Seq("""{"provider_option":true}""", Option.empty[String].orNull).toDF("body"))
    intercept[org.apache.spark.SparkException] {
      stage.validateDurableRows(Seq("""{"file_id":"external-resource"}""").toDF("body"))
    }
  }

  test("durable row validation defers malformed content but rejects external references") {
    val session = spark
    import session.implicits._
    val stage = new AIPrompt().setMessagesCol("messages")
    def check(content: String): Unit = {
      val json = s"""{"messages":[{"role":"user","content":$content}]}"""
      stage.validateDurableRows(spark.read.json(Seq(json).toDS()))
    }
    Seq(
      "\"An ordinary https://example.invalid address in text\"",
      """[{"type":"text","text":"hello"},{"type":"input_text","text":"hello"}]""",
      """[{"type":"input_image","image_url":"data:image/png;base64,AQID","detail":"low"}]""",
      """[{"type":"image_url","image_url":{"url":"data:image/png;base64,AQID"}}]""",
      """[{"type":"input_file","file_data":"data:text/plain;base64,aGVsbG8=","filename":"note.txt"}]""",
      """[{"type":"input_file","file_data":"aGVsbG8=","filename":"note.txt"}]""",
      """[{"type":"input_file","file_data":"https://example.invalid/not-a-location"}]""",
      """[{"type":"audio","data":"unknown"}]""",
      """[{"type":"text","text":"hello","resource":"unsupported-field"}]""",
      "null")
      .foreach(check)
    Seq(
      """[{"type":"input_image","image_url":"https://example.invalid/image.png"}]""",
      """[{"type":"input_file","file_id":"provider-file"}]""",
      """[{"type":"input_file","file_url":"https://example.invalid/file"}]""").foreach { content =>
      intercept[org.apache.spark.SparkException](check(content))
    }
    val decisions = new AIPrompt().setApiType("decisions").setInputCol("input").setInputMode("json")
    decisions.validateDurableRows(Seq("\"inert URL https://example.invalid\"").toDF("input"))
    intercept[org.apache.spark.SparkException] {
      decisions.validateDurableRows(Seq(
        """[{"role":"user","content":[{"type":"input_image","image_url":"https://example.invalid"}]}]""")
        .toDF("input"))
    }
  }

  test("public durable media validation runs before attachment fetching or inference") {
    delta()
    withServer { server =>
      val session = spark
      import session.implicits._
      val root = path()
      val stage = prompt(root, server).setPromptTemplate("{text} {asset}").setColumnTypes(Map("asset" -> "path"))
      intercept[org.apache.spark.SparkException] {
        stage.transform(Seq(("look at this", server.url + "/image.png")).toDF("text", "asset"))
      }
      assert(server.requests.get() == 0)
      assert(AIPromptDelta.control(spark, AIPromptDelta.qualify(spark, root)).state == "initializing")
      val valid = stage.copy(org.apache.spark.ml.param.ParamMap.empty).setOutputPath(path())
      val result = valid.transform(Seq(("https://example.invalid is inert text",
        "data:text/plain;base64,aGVsbG8=")).toDF("text", "asset"))
      assert(result.head().getAs[Row]("error") == null)
      assert(result.count() == 1 && server.requests.get() == 1)
    }
  }

  test("permission failure at the real Delta commit retains ownership and never retries inference") {
    delta()
    assume(java.nio.file.FileSystems.getDefault.supportedFileAttributeViews().contains("posix"),
      "Requires POSIX permissions for a real commit failure.")
    withServer { server =>
      val session = spark
      import session.implicits._
      val directory = Files.createTempDirectory(tmpDir, "aiprompt-commit-permission-")
      val root = directory.resolve("run").toString
      val log = directory.resolve("run").resolve("results").resolve("_delta_log")
      val permissions = PosixFilePermissions.fromString("rwx------")
      try {
        Files.setPosixFilePermissions(directory, PosixFilePermissions.fromString("r-x------"))
        assume(!Files.isWritable(directory), "Requires a non-root user with effective POSIX permission enforcement.")
      } finally {
        Files.setPosixFilePermissions(directory, permissions)
      }
      try {
        val failure = intercept[Exception] {
          AIPromptOutput.transformObserved(prompt(root, server), Seq("one").toDF("text"), boundary =>
            if (boundary == "before_batch_commit") {
              Files.setPosixFilePermissions(log, PosixFilePermissions.fromString("r-x------"))
            })
        }
        val causes = Iterator.iterate(Option(failure: Throwable))(_.flatMap(e => Option(e.getCause)))
          .takeWhile(_.isDefined).flatten.map(_.toString).mkString(" ")
        assert(causes.contains("Permission denied") || causes.contains("AccessDenied"))
        assert(server.requests.get() == 1)
        val status = AIPromptOutput.getOutputStatus(spark, root)
        assert(status.committedCount == 0 && status.pendingCount == 1 && status.ownerToken.isDefined)
        assert(AIPromptOutput.loadOutput(spark, root).count() == 0)
        assert(server.requests.get() == 1)
      } finally {
        if (Files.exists(log)) Files.setPosixFilePermissions(log, permissions)
      }
    }
  }

  test("corrupt committed keys and modified input snapshots fail instead of returning empty success") {
    delta()
    withServer { server =>
      val session = spark
      import session.implicits._
      Seq("results", "_input", "format", "null_format").foreach { table =>
        val root = path()
        prompt(root, server).transform(Seq("one").toDF("text"))
        if (table == "format") {
          spark.sql(s"UPDATE ${AIPromptDelta.table(root, "_control")} SET formatVersion = 999")
        } else if (table == "null_format") {
          spark.sql(s"UPDATE ${AIPromptDelta.table(root, "_control")} SET formatVersion = NULL")
        } else {
          val location = AIPromptDelta.child(root, table)
          spark.read.format("delta").load(location).write.format("delta").mode("append").save(location)
        }
        Seq[() => Any](() => AIPromptOutput.loadOutput(spark, root),
          () => AIPromptOutput.getOutputStatus(spark, root),
          () => prompt(root, server).resume(spark)).foreach { operation =>
          val error = intercept[IllegalArgumentException](operation())
          if (table == "format" || table == "null_format") {
            assert(error.getMessage.contains("Unsupported AIPrompt output format version"))
          } else if (table == "results") {
            assert(error.getMessage.contains("Corrupt AIPrompt row identity: keys must be unique and non-null"))
          } else {
            assert(error.getMessage.contains("The immutable AIPrompt input snapshot has been modified"))
          }
        }
      }
      assert(server.requests.get() == 4)
    }
  }

  test("qualified ABFS storage runs and recovers with the caller's configured identity") {
    delta()
    val base = AILiveTestEnvironment.get("AIPROMPT_ABFS_TEST_ROOT")
    assume(base.isDefined, "ABFS/OneLake qualification requires AIPROMPT_ABFS_TEST_ROOT and caller storage identity.")
    assert(Try(new URI(base.get)).toOption.exists(uri => Set("abfs", "abfss").contains(uri.getScheme)),
      "ABFS qualification requires an abfs:// or abfss:// test root.")
    withServer { server =>
      val session = spark
      import session.implicits._
      val root = base.get.stripSuffix("/") + "/" + java.util.UUID.randomUUID().toString
      val stage = prompt(root, server)
      try {
        intercept[IllegalStateException] {
          AIPromptOutput.transformObserved(stage, Seq("one", "two", "three").toDF("text"), event =>
            if (event == "batch_committed") throw new IllegalStateException("interruption"))
        }
        val status = AIPromptOutput.getOutputStatus(spark, root)
        AIPromptOutput.releaseOutputOwner(spark, root, status.ownerToken.get, confirmStopped = true)
        assert(stage.resume(spark).count() == 3 && server.requests.get() == 3)
      } finally {
        val location = new org.apache.hadoop.fs.Path(root)
        location.getFileSystem(spark.sparkContext.hadoopConfiguration).delete(location, true)
      }
    }
  }
}
