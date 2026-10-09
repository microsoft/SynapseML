// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.vision

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.services.CognitiveServicesBaseNoHandler
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.http.client.utils.URLEncodedUtils
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.Row
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.net.{InetSocketAddress, URI}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.JavaConverters._

class ComputerVisionEndpointSuite extends TestBase {

  import spark.implicits._

  private case class Request(method: String, uri: URI, contentType: String, body: Array[Byte]) {
    def query: Map[String, String] =
      URLEncodedUtils.parse(uri, StandardCharsets.UTF_8).asScala.map(p => p.getName -> p.getValue).toMap
  }

  private val imageUrl = "https://example.org/image.jpg"
  private val metadata = """"metadata":{"width":100,"height":50,"format":"Jpeg"}"""
  private val description = """"description":{"tags":["cat"],"captions":[{"text":"a cat","confidence":0.9}]}"""
  private val analysis = s"""{"requestId":"local-request",$metadata,$description,
                           |"tags":[{"name":"cat","confidence":0.9}],"modelVersion":"2021-05-01"}""".stripMargin
  private val readResult =
    """{"status":"succeeded","createdDateTime":"2021-02-04T06:32:08Z",
      |"lastUpdatedDateTime":"2021-02-04T06:32:09Z","analyzeResult":{"version":"3.2",
      |"modelVersion":"2022-04-30","readResults":[{"page":1,"angle":0,"width":100,"height":50,
      |"unit":"pixel","lines":[{"boundingBox":[0,0,20,0,20,10,0,10],"text":"hello",
      |"words":[{"boundingBox":[0,0,20,0,20,10,0,10],"text":"hello","confidence":0.9}]}]}]}}""".stripMargin
  private val thumbnail = Array[Byte](1, 2, 3, 4)
  private val retired = """{"error":{"code":"ApiVersionRetired","message":"This API version is retired."}}"""

  private def responseFor(path: String): String = path match {
    case "/vision/v3.2/analyze" | "/explicit/analyze" => analysis
    case "/vision/v3.2/tag" =>
      s"""{"requestId":"local-request",$metadata,"tags":[{"name":"cat","confidence":0.9}]}"""
    case "/vision/v3.2/describe" => s"""{"requestId":"local-request",$metadata,$description}"""
    case "/vision/v3.2/models/landmarks/analyze" =>
      s"""{"requestId":"local-request",$metadata,"result":{"landmarks":[{"name":"tower","confidence":0.9}]}}"""
    case "/vision/v3.2/ocr" =>
      """{"language":"en","orientation":"Up","textAngle":0,"regions":[{"boundingBox":"0,0,20,10",
        |"lines":[{"boundingBox":"0,0,20,10","words":[{"boundingBox":"0,0,20,10","text":"hello"}]}]}]}""".stripMargin
    case "/vision/v3.2/read/analyze" => "{}"
    case "/vision/v3.2/read/analyzeResults/local" => readResult
    case "/vision/v3.2/generateThumbnail" => ""
    case _ => retired
  }

  private def withServer(testCode: (String, ConcurrentLinkedQueue[Request]) => Unit): Unit = {
    val requests = new ConcurrentLinkedQueue[Request]()
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    val baseUrl = s"http://127.0.0.1:${server.getAddress.getPort}"
    server.createContext("/", new HttpHandler {
      override def handle(exchange: HttpExchange): Unit = {
        try {
          val uri = exchange.getRequestURI
          requests.add(Request(exchange.getRequestMethod, uri,
            exchange.getRequestHeaders.getFirst("Content-Type"), IOUtils.toByteArray(exchange.getRequestBody)))
          val response = responseFor(uri.getPath)
          val isThumbnail = uri.getPath == "/vision/v3.2/generateThumbnail"
          val bytes = if (isThumbnail) thumbnail else response.getBytes(StandardCharsets.UTF_8)
          val status = if (response == retired) {
            410
          } else if (uri.getPath == "/vision/v3.2/read/analyze") {
            exchange.getResponseHeaders.add("Operation-Location", baseUrl + "/vision/v3.2/read/analyzeResults/local")
            202
          } else {
            200
          }
          exchange.getResponseHeaders.add("Content-Type", if (isThumbnail) "image/jpeg" else "application/json")
          exchange.sendResponseHeaders(status, bytes.length)
          exchange.getResponseBody.write(bytes)
        } finally {
          exchange.close()
        }
      }
    })
    server.start()
    try {
      testCode(baseUrl, requests)
    } finally {
      server.stop(0)
    }
  }

  private def configure[T <: CognitiveServicesBaseNoHandler with HasImageInput](stage: T, endpoint: String): T = {
    stage.setEndpoint(endpoint + "/").setImageUrlCol("image").setOutputCol("response")
      .setErrorCol("error").setConcurrency(1)
    stage
  }

  private def result(stage: CognitiveServicesBaseNoHandler): Row = {
    val input = Seq(imageUrl).toDF("image").coalesce(1)
    val output = stage.transform(input)
    // JSON parsing makes nested fields nullable without changing their names or types.
    assert(output.schema.simpleString == stage.transformSchema(input.schema).simpleString)
    val row = output.head()
    assert(row.isNullAt(row.fieldIndex("error")), row.toString)
    row
  }

  private def assertImageRequest(request: Request, path: String, query: Map[String, String]): Unit = {
    assert(request.method == "POST")
    assert(request.uri.getPath == path)
    assert(request.query == query)
    assert(request.contentType.startsWith("application/json"))
    assert(new String(request.body, StandardCharsets.UTF_8).parseJson == Map("url" -> imageUrl).toJson)
  }

  test("AnalyzeImage sends v3.2 requests and preserves description tags and column parameters") {
    withServer { (endpoint, requests) =>
      val features = Seq(
        "Categories", "Tags", "Description", "Faces", "ImageType", "Color", "Adult", "Objects", "Brands")
      val stage = configure(new AnalyzeImage(), endpoint)
        .setVisualFeatures(features).setDetails(Seq("Celebrities", "Landmarks"))
        .setDescriptionExclude(Seq("Celebrities")).setLanguageCol("language")
      val input = Seq((Some(imageUrl), Some("en")), (Some(imageUrl), None), (None, Some("en")))
        .toDF("image", "language").coalesce(1)
      val output = stage.transform(input)
      assert(output.schema.simpleString == stage.transformSchema(input.schema).simpleString)
      val rows = output.collect()
      rows.take(2).foreach { row =>
        assert(row.isNullAt(row.fieldIndex("error")), row.toString)
        val response = row.getAs[Row]("response")
        assert(response.schema.simpleString == AIResponse.schema.simpleString)
        assert(response.getAs[Row]("description").getAs[Seq[String]]("tags") == Seq("cat"))
      }
      assert(rows.last.isNullAt(rows.last.fieldIndex("response")))
      assert(requests.size() == 2)
      val common = Map("visualFeatures" -> features.mkString(","), "details" -> "Celebrities,Landmarks",
        "descriptionExclude" -> "Celebrities")
      assertImageRequest(requests.asScala.head, "/vision/v3.2/analyze", common + ("language" -> "en"))
      assertImageRequest(requests.asScala.last, "/vision/v3.2/analyze", common)
    }
  }

  test("OCR keeps its synchronous region line word response in v3.2") {
    withServer { (endpoint, requests) =>
      val stage = configure(new OCR(), endpoint).setDetectOrientation(true).setLanguage("en")
      val response = result(stage).getAs[Row]("response")
      assert(OCRResponse.makeFromRowConverter(response).regions.head.lines.head.words.head.text == "hello")
      assertImageRequest(requests.peek(), "/vision/v3.2/ocr", Map("detectOrientation" -> "true", "language" -> "en"))
    }
  }

  test("TagImage and DescribeImage keep tag and caption response shapes in v3.2") {
    withServer { (endpoint, requests) =>
      val tagged = result(configure(new TagImage(), endpoint).setLanguage("en")).getAs[Row]("response")
      assert(tagged.getAs[Seq[Row]]("tags").head.getAs[String]("name") == "cat")
      val described = result(configure(new DescribeImage(), endpoint).setLanguage("en").setMaxCandidates(2))
        .getAs[Row]("response").getAs[Row]("description")
      assert(described.getAs[Seq[String]]("tags") == Seq("cat"))
      assert(described.getAs[Seq[Row]]("captions").head.getAs[String]("text") == "a cat")
      assertImageRequest(requests.asScala.head, "/vision/v3.2/tag", Map("language" -> "en"))
      assertImageRequest(requests.asScala.last, "/vision/v3.2/describe",
        Map("language" -> "en", "maxCandidates" -> "2"))
    }
  }

  test("domain recognition appends the selected model to the v3.2 endpoint") {
    withServer { (endpoint, requests) =>
      val stage = configure(new RecognizeDomainSpecificContent(), endpoint).setModel("landmarks")
      val response = DSIRResponse.makeFromRowConverter(result(stage).getAs[Row]("response"))
      assert(response.result.landmarks.get.head.name == "tower")
      assertImageRequest(requests.peek(), "/vision/v3.2/models/landmarks/analyze", Map.empty)
    }
  }

  test("GenerateThumbnails sends binary input and returns binary output with v3.2") {
    withServer { (endpoint, requests) =>
      val stage = new GenerateThumbnails().setEndpoint(endpoint + "/").setImageBytes(thumbnail)
        .setWidth(50).setHeight(50).setSmartCropping(true).setOutputCol("response").setErrorCol("error")
      val row = stage.transform(Seq(imageUrl).toDF("image").coalesce(1)).head()
      assert(row.isNullAt(row.fieldIndex("error")), row.toString)
      assert(row.getAs[Array[Byte]]("response").sameElements(thumbnail))
      val sent = requests.peek()
      assert(sent.method == "POST")
      assert(sent.uri.getPath == "/vision/v3.2/generateThumbnail")
      assert(sent.query == Map("width" -> "50", "height" -> "50", "smartCropping" -> "true"))
      assert(sent.contentType == "application/octet-stream")
      assert(sent.body.sameElements(thumbnail))
    }
  }

  test("ReadImage submits to v3.2 and polls the operation location without changing its response schema") {
    withServer { (endpoint, requests) =>
      val stage = configure(new ReadImage(), endpoint).setLanguage("en")
        .setInitialPollingDelay(0).setPollingDelay(0).setMaxPollingRetries(1)
      val response = result(stage).getAs[Row]("response")
      assert(response.schema.simpleString == ReadResponse.schema.simpleString)
      val read = ReadResponse.makeFromRowConverter(response)
      assert(read.analyzeResult.readResults.head.lines.head.text == "hello")
      assertImageRequest(requests.asScala.head, "/vision/v3.2/read/analyze", Map("language" -> "en"))
      assert(requests.size() == 2)
      assert(requests.asScala.last.method == "GET")
      assert(requests.asScala.last.uri.getPath == "/vision/v3.2/read/analyzeResults/local")
    }
  }

  test("explicit URLs survive copy and save load and take precedence when set after location") {
    withServer { (endpoint, requests) =>
      val stage = configure(new AnalyzeImage(), endpoint).setLocation("eastus")
        .setUrl(endpoint + "/explicit/analyze").setVisualFeatures(Seq("Description"))
      val directory = Files.createTempDirectory(Paths.get("."), "vision-roundtrip-").toFile
      try {
        val path = new java.io.File(directory, "model").toString
        stage.write.save(path)
        Seq(stage.copy(ParamMap.empty).asInstanceOf[AnalyzeImage], AnalyzeImage.load(path)).foreach { restored =>
          assert(restored.getUrl == endpoint + "/explicit/analyze")
          assert(result(restored).getAs[Row]("response").getAs[Row]("description")
            .getAs[Seq[String]]("tags") == Seq("cat"))
        }
        assert(requests.size() == 2)
        requests.asScala.foreach(assertImageRequest(_, "/explicit/analyze", Map("visualFeatures" -> "Description")))
      } finally {
        FileUtils.deleteDirectory(directory)
      }
    }
  }

  test("an explicit retired URL reports HTTP 410 without retrying or rewriting the override") {
    withServer { (endpoint, requests) =>
      val stage = configure(new AnalyzeImage(), endpoint).setUrl(endpoint + "/vision/v2.0/analyze")
        .setVisualFeatures(Seq("Description"))
      val row = stage.transform(Seq(imageUrl).toDF("image").coalesce(1)).head()
      assert(row.isNullAt(row.fieldIndex("response")))
      val error = row.getAs[Row]("error")
      assert(error.getAs[Row]("status").getAs[Int]("statusCode") == 410)
      assert(error.getAs[String]("response").parseJson.asJsObject.fields("error")
        .asJsObject.fields("code") == JsString("ApiVersionRetired"))
      assert(requests.size() == 1)
      assertImageRequest(requests.peek(), "/vision/v2.0/analyze", Map("visualFeatures" -> "Description"))
    }
  }

  test("missing image input is rejected before sending a request") {
    withServer { (endpoint, requests) =>
      val stage = new AnalyzeImage().setEndpoint(endpoint + "/").setOutputCol("response")
      intercept[IllegalArgumentException] {
        stage.transform(Seq(imageUrl).toDF("image")).collect()
      }
      assert(requests.isEmpty)
    }
  }
}
