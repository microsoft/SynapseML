// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.vision

import com.microsoft.azure.synapse.ml.core.test.fuzzing._
import com.microsoft.azure.synapse.ml.core.utils.JarLoadingUtils
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.http.client.utils.URLEncodedUtils
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.Row
import org.scalatest.Outcome
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.net.{InetSocketAddress, URI}
import java.lang.reflect.ParameterizedType
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.JavaConverters._

class AnalyzeImageV4Suite extends TransformerFuzzing[AnalyzeImageV4] {

  import spark.implicits._

  private case class Request(uri: URI, contentType: String, body: Array[Byte]) {
    def query: Map[String, String] =
      URLEncodedUtils.parse(uri, StandardCharsets.UTF_8).asScala.map(p => p.getName -> p.getValue).toMap
  }

  private val imageUrl = "https://example.org/image.jpg"
  // Constructor-test generation calls testObjects outside a running ScalaTest fixture.
  private var fuzzEndpoint = "http://127.0.0.1:0"
  private val box = """{"x":1,"y":2,"w":30,"h":40}"""
  private val polygon = """[{"x":1,"y":2},{"x":31,"y":2},{"x":31,"y":42},{"x":1,"y":42}]"""
  private val response =
    s"""{"modelVersion":"2023-10-01","metadata":{"width":100,"height":50},
       |"tagsResult":{"values":[{"name":"cat","confidence":0.99}]},
       |"objectsResult":{"values":[{"id":"1","boundingBox":$box,"tags":[{"name":"cat","confidence":0.98}]}]},
       |"captionResult":{"text":"a cat","confidence":0.97},
       |"denseCaptionsResult":{"values":[{"text":"a cat","confidence":0.96,"boundingBox":$box}]},
       |"readResult":{"blocks":[{"lines":[{"text":"hello","boundingPolygon":$polygon,
       |"words":[{"text":"hello","confidence":0.95,"boundingPolygon":$polygon}]}]},
       |{"lines":[{"text":"world","boundingPolygon":$polygon,"words":[]}]}]},
       |"smartCropsResult":{"values":[{"aspectRatio":1.25,"boundingBox":$box}]},
       |"peopleResult":{"values":[{"confidence":0.94,"boundingBox":$box}]}}""".stripMargin

  private def withServer[T](testCode: (String, ConcurrentLinkedQueue[Request]) => T): T = {
    val requests = new ConcurrentLinkedQueue[Request]()
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext("/", new HttpHandler {
      override def handle(exchange: HttpExchange): Unit = {
        try {
          requests.add(Request(exchange.getRequestURI, exchange.getRequestHeaders.getFirst("Content-Type"),
            IOUtils.toByteArray(exchange.getRequestBody)))
          val (status, body) = exchange.getRequestURI.getPath match {
            case "/error" => (400, """{"error":{"code":"InvalidRequest","message":"Invalid image"}}""")
            case "/malformed" => (200, "{not-json")
            case "/tags-only" =>
              (200, """{"modelVersion":"2023-10-01","metadata":{"width":100,"height":50},
                |"tagsResult":{"values":[]}}""".stripMargin)
            case _ => (200, response)
          }
          val bytes = body.getBytes(StandardCharsets.UTF_8)
          exchange.getResponseHeaders.add("Content-Type", "application/json")
          exchange.sendResponseHeaders(status, bytes.length)
          exchange.getResponseBody.write(bytes)
        } finally {
          exchange.close()
        }
      }
    })
    server.start()
    try testCode(s"http://127.0.0.1:${server.getAddress.getPort}", requests) finally server.stop(0)
  }

  override protected def withFixture(test: NoArgTest): Outcome = {
    withServer { (endpoint, _) =>
      val previousEndpoint = fuzzEndpoint
      fuzzEndpoint = endpoint
      try super.withFixture(test) finally fuzzEndpoint = previousEndpoint
    }
  }

  private def stage(endpoint: String): AnalyzeImageV4 =
    new AnalyzeImageV4().setEndpoint(endpoint).setImageUrlCol("image")
      .setOutputCol("response").setErrorCol("error").setConcurrency(1)

  test("GA requests encode all features and options and parse the separate v4 schema") {
    withServer { (endpoint, requests) =>
      val features = Seq("tags", "objects", "caption", "denseCaptions", "read", "smartCrops", "people")
      val transformer = stage(endpoint).setFeatures(features).setLanguage("en")
        .setGenderNeutralCaption(true).setSmartCropsAspectRatios(Seq(0.75, 1.8))
      val data = Seq(imageUrl).toDF("image").coalesce(1)
      val output = transformer.transform(data)
      assert(output.schema.simpleString == transformer.transformSchema(data.schema).simpleString)
      val row = output.head()
      assert(row.getAs[Row]("error") == null)
      val result = row.getAs[Row]("response")
      assert(result.schema.simpleString == ImageAnalysisV4Response.schema.simpleString)
      val typed = ImageAnalysisV4Response.makeFromRowConverter(result)
      assert(typed.tagsResult.get.values.head.name == "cat")
      assert(typed.objectsResult.get.values.head.boundingBox.w == 30)
      assert(typed.objectsResult.get.values.head.tags.head.confidence == 0.98)
      assert(typed.captionResult.get.text == "a cat")
      assert(typed.denseCaptionsResult.get.values.head.boundingBox.h == 40)
      assert(typed.readResult.get.blocks.flatMap(_.lines).map(_.text) == Seq("hello", "world"))
      assert(typed.readResult.get.blocks.head.lines.head.words.head.confidence == 0.95)
      assert(typed.smartCropsResult.get.values.head.aspectRatio == 1.25)
      assert(typed.peopleResult.get.values.head.confidence == 0.94)
      assert(!result.schema.fieldNames.contains("description"))
      assert(requests.size() == 1)
      val sent = requests.peek()
      assert(sent.uri.getPath == "/computervision/imageanalysis:analyze")
      assert(sent.query == Map("api-version" -> "2024-02-01", "features" -> features.mkString(","),
        "language" -> "en", "gender-neutral-caption" -> "true", "smartcrops-aspect-ratios" -> "0.75,1.8"))
      assert(sent.contentType.startsWith("application/json"))
      assert(new String(sent.body, StandardCharsets.UTF_8).parseJson == Map("url" -> imageUrl).toJson)
    }
  }

  test("URL queries are merged once and explicit stage values replace preview parameters") {
    withServer { (endpoint, requests) =>
      val transformer = stage(endpoint).setFeatures(Seq("tags", "objects"))
        .setUrl(endpoint + "/explicit?api-version=preview&features=read&trace=a%26b%20c")
      transformer.transform(Seq(imageUrl).toDF("image").coalesce(1)).collect()
      val sent = requests.peek()
      assert(sent.uri.getPath == "/explicit")
      assert(sent.query == Map("api-version" -> "2024-02-01", "features" -> "tags,objects", "trace" -> "a&b c"))
      assert(sent.uri.getRawQuery.count(_ == '?') == 0)
    }
  }

  test("location resource name and endpoint setters reset earlier overrides") {
    withServer { (endpoint, requests) =>
      val transformer = stage(endpoint).setUrl(endpoint + "/stale").setLocation("eastus")
      assert(transformer.getUrl == "https://eastus.api.cognitive.microsoft.com/computervision/imageanalysis:analyze")
      transformer.setCustomServiceName("example")
      assert(transformer.getUrl == "https://example.cognitiveservices.azure.com/computervision/imageanalysis:analyze")
      Seq(endpoint, endpoint + "/").foreach { root =>
        transformer.setUrl(endpoint + "/stale").setEndpoint(root)
        transformer.transform(Seq(imageUrl).toDF("image").coalesce(1)).collect()
      }
      assert(requests.size() == 2)
      assert(requests.asScala.forall(_.uri.getPath == "/computervision/imageanalysis:analyze"))
      Seq("", "relative", endpoint + "?key=value", endpoint + "#fragment").foreach { invalid =>
        intercept[IllegalArgumentException](transformer.setEndpoint(invalid))
      }
    }
  }

  test("byte input and vector features use one synchronous request and skip null rows") {
    withServer { (endpoint, requests) =>
      val bytes = Array[Byte](1, 2, 3)
      val data = Seq((Some(bytes), Some(Seq("read"))), (None, Some(Seq("read"))), (Some(bytes), None))
        .toDF("image", "features").coalesce(1)
      val transformer = new AnalyzeImageV4().setEndpoint(endpoint).setImageBytesCol("image")
        .setFeaturesCol("features").setOutputCol("response").setErrorCol("error")
      val rows = transformer.transform(data).collect()
      assert(rows.head.getAs[Row]("response").getAs[Row]("readResult") != null)
      assert(rows.tail.forall(_.getAs[Row]("response") == null))
      assert(rows.forall(_.getAs[Row]("error") == null))
      assert(requests.size() == 1)
      assert(requests.peek().contentType == "application/octet-stream")
      assert(requests.peek().body.sameElements(bytes))
      assert(requests.peek().query == Map("api-version" -> "2024-02-01", "features" -> "read"))
    }
  }

  test("missing or ambiguous input and invalid feature selections never send HTTP requests") {
    withServer { (endpoint, requests) =>
      val transformer = stage(endpoint)
      Seq(Seq.empty[String], Seq("Categories"), Seq("tags", "Faces")).foreach { invalid =>
        intercept[IllegalArgumentException](transformer.setFeatures(invalid))
      }
      Seq(Seq(0.5), Seq(2.0), Seq(Double.NaN)).foreach { invalid =>
        intercept[IllegalArgumentException](transformer.setSmartCropsAspectRatios(invalid))
      }
      intercept[IllegalArgumentException] {
        new AnalyzeImageV4().setEndpoint(endpoint).transform(Seq(imageUrl).toDF("image")).collect()
      }
      val both = transformer.setImageBytes(Array[Byte](1)).transform(Seq(imageUrl).toDF("image")).head()
      assert(both.getAs[Row]("response") == null)
      assert(requests.isEmpty)
    }
  }

  test("HTTP errors remain visible and absent or malformed successful fields are null") {
    withServer { (endpoint, requests) =>
      val transformer = stage(endpoint)
      val data = Seq(Some(imageUrl), None).toDF("image").coalesce(1)
      val errorRows = transformer.setUrl(endpoint + "/error").transform(data).collect()
      val error = errorRows.head.getAs[Row]("error")
      assert(error.getAs[Row]("status").getAs[Int]("statusCode") == 400)
      assert(error.getAs[String]("response").contains("InvalidRequest"))
      assert(errorRows.forall(_.getAs[Row]("response") == null))
      assert(errorRows.last.getAs[Row]("error") == null)
      val tags = transformer.setUrl(endpoint + "/tags-only").transform(data).head().getAs[Row]("response")
      assert(tags.getAs[Row]("tagsResult").getAs[Seq[Row]]("values").isEmpty)
      assert(tags.getAs[Row]("readResult") == null)
      val malformed = transformer.setUrl(endpoint + "/malformed").transform(data).head()
      // Match the shared JSONOutputParser's permissive behavior; a 200 is not an HTTP error.
      assert(malformed.getAs[Row]("error") == null)
      assert(malformed.getAs[Row]("response").toSeq.forall(_ == null))
      assert(requests.size() == 3)
    }
  }

  test("copy and save load preserve the v4 class query parameters and complete results") {
    withServer { (endpoint, requests) =>
      val transformer = stage(endpoint).setFeatures(Seq("tags", "read"))
        .setUrl(endpoint + "/explicit?trace=roundtrip").setGenderNeutralCaption(true)
      val directory = Files.createTempDirectory(Paths.get("."), "vision-v4-roundtrip-").toFile
      try {
        val path = new java.io.File(directory, "model").toString
        transformer.write.save(path)
        val data = Seq(imageUrl).toDF("image").coalesce(1)
        val expected = transformer.transform(data).collect().toSeq
        Seq(transformer.copy(ParamMap.empty), AnalyzeImageV4.load(path)).foreach { restored =>
          assert(restored.isInstanceOf[AnalyzeImageV4])
          assert(restored.transform(data).collect().toSeq == expected)
        }
        assert(requests.size() == 3)
        assert(requests.asScala.forall(_.query == Map("api-version" -> "2024-02-01", "features" -> "tags,read",
          "trace" -> "roundtrip", "gender-neutral-caption" -> "true")))
      } finally {
        FileUtils.deleteDirectory(directory)
      }
    }

  }

  test("the stage is discoverable in all four root fuzzing registries") {
    val suite = JarLoadingUtils.AllClasses.find(_ == classOf[AnalyzeImageV4Suite]).get
    val contracts = Seq(
      classOf[ExperimentFuzzing[_]] -> "experimentTestObjects",
      classOf[SerializationFuzzing[_]] -> "serializationTestObjects",
      classOf[PyTestFuzzing[_]] -> "pyTestObjects",
      classOf[RTestFuzzing[_]] -> "rTestObjects")
    contracts.foreach { case (contract, method) =>
      assert(contract.isAssignableFrom(suite), contract.getName)
      val stageType = suite.getMethod(method).getGenericReturnType.asInstanceOf[ParameterizedType]
        .getActualTypeArguments.head.asInstanceOf[ParameterizedType].getActualTypeArguments.head
      assert(Class.forName(stageType.getTypeName) == classOf[AnalyzeImageV4])
    }
  }

  override def testObjects(): Seq[TestObject[AnalyzeImageV4]] = Seq(
    new TestObject(stage(fuzzEndpoint).setFeatures(Seq("tags", "read")),
      Seq(imageUrl).toDF("image").coalesce(1)),
    new TestObject(new AnalyzeImageV4().setEndpoint(fuzzEndpoint).setImageBytesCol("image")
      .setFeatures(Seq("tags", "objects")).setOutputCol("response").setErrorCol("error"),
      Seq(Array[Byte](1, 2, 3)).toDF("image").coalesce(1))
  )

  override def reader: MLReadable[_] = AnalyzeImageV4
}
