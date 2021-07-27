// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cognitive.split1

import com.microsoft.ml.spark.FluentAPI._
import com.microsoft.ml.spark.cognitive.FormsFlatteners._
import com.microsoft.ml.spark.cognitive.RESTHelpers.retry
import com.microsoft.ml.spark.cognitive._
import com.microsoft.ml.spark.core.env.StreamUtilities.using
import com.microsoft.ml.spark.core.test.base.{Flaky, TestBase}
import com.microsoft.ml.spark.core.test.fuzzing.{TestObject, TransformerFuzzing}
import com.microsoft.ml.spark.stages.UDFTransformer
import org.apache.commons.io.IOUtils
import org.apache.http.client.methods._
import org.apache.http.entity.StringEntity
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.scalactic.Equality
import spray.json._

import java.net.URI

object TrainCustomModelProtocol extends DefaultJsonProtocol {
  implicit val SourceFilterEnc: RootJsonFormat[SourceFilter] = jsonFormat2(SourceFilter)
  implicit val TrainCustomModelEnc: RootJsonFormat[TrainCustomModelSchema] = jsonFormat3(TrainCustomModelSchema)
}

import com.microsoft.ml.spark.cognitive.split1.TrainCustomModelProtocol._

case class TrainCustomModelSchema(source: String, sourceFilter: SourceFilter, useLabelFile: Boolean)

case class SourceFilter(prefix: String, includeSubFolders: Boolean)

object FormRecognizerUtils extends CognitiveKey {

  import RESTHelpers._

  val PollingDelay = 1000

  def formSend(request: HttpRequestBase, path: String,
               params: Map[String, String] = Map()): String = {

    val paramString = if (params.isEmpty) {
      ""
    } else {
      "?" + URLEncodingUtils.format(params)
    }
    request.setURI(new URI(path + paramString))

    retry(List(100, 500, 1000), { () =>
      request.addHeader("Ocp-Apim-Subscription-Key", cognitiveKey)
      request.addHeader("Content-Type", "application/json")
      using(Client.execute(request)) { response =>
        if (!response.getStatusLine.getStatusCode.toString.startsWith("2")) {
          val bodyOpt = request match {
            case er: HttpEntityEnclosingRequestBase => IOUtils.toString(er.getEntity.getContent, "UTF-8")
            case _ => ""
          }
          throw new RuntimeException(s"Failed: response: $response " + s"requestUrl: ${request.getURI}" +
            s"requestBody: $bodyOpt")
        }
        if (response.getStatusLine.getReasonPhrase == "No Content") {
          ""
        }
        else if (response.getStatusLine.getReasonPhrase == "Created") {
          response.getHeaders("Location").head.getValue
        }
        else {
          IOUtils.toString(response.getEntity.getContent, "UTF-8")
        }
      }.get
    })
  }

  def formDelete(path: String, params: Map[String, String] = Map()): String = {
    formSend(new HttpDelete(),
      "https://eastus.api.cognitive.microsoft.com/formrecognizer/v2.1/custom/models/" + path, params)
  }

  def formPost(path: String, body: TrainCustomModelSchema, params: Map[String, String] = Map())
              (implicit format: JsonFormat[TrainCustomModelSchema]): String = {
    val post = new HttpPost()
    post.setEntity(new StringEntity(body.toJson.compactPrint))
    formSend(post, "https://eastus.api.cognitive.microsoft.com/formrecognizer/v2.1/custom/models" + path, params)
  }

  def formGet(path: String, params: Map[String, String] = Map()): String = {
    formSend(new HttpGet(), path, params)
  }
}

trait FormRecognizerUtils extends TestBase with CognitiveKey with Flaky {

  import spark.implicits._

  def createTestDataframe(baseUrl: String, docs: Seq[String], returnBytes: Boolean): DataFrame = {
    val df = docs.map(doc => baseUrl + doc).toDF("source")
    if (returnBytes) {
      BingImageSearch
        .downloadFromUrls("source", "imageBytes", 4, 10000)
        .transform(df)
        .select("imageBytes")
    } else {
      df
    }
  }

  val baseUrl = "https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/"

  lazy val imageDf1: DataFrame = createTestDataframe(baseUrl, Seq("layout1.jpg"), returnBytes = false)

  lazy val bytesDF1: DataFrame = createTestDataframe(baseUrl, Seq("layout1.jpg"), returnBytes = true)

  lazy val imageDf2: DataFrame = createTestDataframe(baseUrl, Seq("receipt1.png"), returnBytes = false)

  lazy val bytesDF2: DataFrame = createTestDataframe(baseUrl, Seq("receipt1.png"), returnBytes = true)

  lazy val imageDf3: DataFrame = createTestDataframe(baseUrl, Seq("business_card.jpg"), returnBytes = false)

  lazy val bytesDF3: DataFrame = createTestDataframe(baseUrl, Seq("business_card.jpg"), returnBytes = true)

  lazy val imageDf4: DataFrame = createTestDataframe(baseUrl, Seq("invoice2.png"), returnBytes = false)

  lazy val bytesDF4: DataFrame = createTestDataframe(baseUrl, Seq("invoice2.png"), returnBytes = true)

  lazy val imageDf5: DataFrame = createTestDataframe(baseUrl, Seq("id1.jpg"), returnBytes = false)

  lazy val bytesDF5: DataFrame = createTestDataframe(baseUrl, Seq("id1.jpg"), returnBytes = true)

  lazy val pdfDf1: DataFrame = createTestDataframe(baseUrl, Seq("layout2.pdf"), returnBytes = false)

  lazy val pdfDf2: DataFrame = createTestDataframe(baseUrl, Seq("invoice1.pdf", "invoice3.pdf"), returnBytes = false)

  lazy val pathDf: DataFrame = createTestDataframe(baseUrl, Seq(""), returnBytes = false)

  // TODO refactor tests to share structure
  def basicTest(df: DataFrame,
                method: Transformer with FormRecognizerBase,
                flatteners: Seq[UDFTransformer],
                truePrefixes: Seq[String]): Unit = {
    val results = df.mlTransform(Seq(method) ++ flatteners: _*)
      .select(flatteners.map(f => col(f.getOutputCol)): _*)
      .collect()
    flatteners.indices.zip(truePrefixes).foreach { case (i, truePrefix) =>
      assert(results.head.getString(i).startsWith(truePrefix))
    }
  }
}

class AnalyzeLayoutSuite extends TransformerFuzzing[AnalyzeLayout] with FormRecognizerUtils {

  lazy val analyzeLayout: AnalyzeLayout = new AnalyzeLayout()
    .setSubscriptionKey(cognitiveKey).setLocation("eastus")
    .setImageUrlCol("source").setOutputCol("layout").setConcurrency(5)

  lazy val bytesAnalyzeLayout: AnalyzeLayout = new AnalyzeLayout()
    .setSubscriptionKey(cognitiveKey).setLocation("eastus")
    .setImageBytesCol("imageBytes").setOutputCol("layout").setConcurrency(5)

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    def prep(df: DataFrame) = {
      df.select("source", "layout.analyzeResult.readResults")
    }

    super.assertDFEq(prep(df1), prep(df2))(eq)
  }

  test("Basic Usage with URL") {
    val results = imageDf1.mlTransform(analyzeLayout,
      flattenReadResults("layout", "readlayout"),
      flattenPageResults("layout", "pageLayout"))
      .select("readlayout", "pageLayout")
      .collect()
    val headStr = results.head.getString(0)
    assert(headStr.startsWith("Purchase Order Hero Limited Purchase Order Company Phone: 555-348-6512 " +
      "Website: www.herolimited.com "))
    val pageHeadStr = results.head.getString(1)
    assert(pageHeadStr.contains("Tables: Details | Quantity | Unit Price | Total"))
  }

  test("Basic Usage with pdf") {
    val results = pdfDf1.mlTransform(analyzeLayout,
      flattenReadResults("layout", "readlayout"),
      flattenPageResults("layout", "pageLayout"))
      .select("readlayout", "pageLayout")
      .collect()
    val headStr = results.head.getString(0)
    val correctPrefix = "UNITED STATES SECURITIES AND EXCHANGE COMMISSION Washington, D.C. 20549 FORM 10-Q"
    assert(headStr.startsWith(correctPrefix))
    val pageHeadStr = results.head.getString(1)
    assert(pageHeadStr.contains("Tables: Title of each class | Trading Symbol | Name of exchange on which registered "))
  }

  test("Basic Usage with Bytes") {
    val results = bytesDF1.mlTransform(bytesAnalyzeLayout,
      flattenReadResults("layout", "readlayout"),
      flattenPageResults("layout", "pageLayout"))
      .select("readlayout", "pageLayout")
      .collect()
    val headStr = results.head.getString(0)
    assert(headStr.startsWith("Purchase Order Hero Limited Purchase Order Company Phone: 555-348-6512" +
      " Website: www.herolimited.com "))
    val pageHeadStr = results.head.getString(1)
    assert(pageHeadStr.contains("Tables: Details | Quantity | Unit Price | Total"))

  }

  override def testObjects(): Seq[TestObject[AnalyzeLayout]] =
    Seq(new TestObject(analyzeLayout, imageDf1))

  override def reader: MLReadable[_] = AnalyzeLayout
}

class AnalyzeReceiptsSuite extends TransformerFuzzing[AnalyzeReceipts] with FormRecognizerUtils {

  lazy val analyzeReceipts: AnalyzeReceipts = new AnalyzeReceipts()
    .setSubscriptionKey(cognitiveKey).setLocation("eastus")
    .setImageUrlCol("source").setOutputCol("receipts").setConcurrency(5)

  lazy val bytesAnalyzeReceipts: AnalyzeReceipts = new AnalyzeReceipts()
    .setSubscriptionKey(cognitiveKey).setLocation("eastus")
    .setImageBytesCol("imageBytes").setOutputCol("receipts").setConcurrency(5)

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    def prep(df: DataFrame) = {
      df.select("source", "receipts.analyzeResult.readResults")
    }

    super.assertDFEq(prep(df1), prep(df2))(eq)
  }

  test("Basic Usage with URL") {
    val results = imageDf2.mlTransform(analyzeReceipts,
      flattenReadResults("receipts", "readReceipts"),
      flattenDocumentResults("receipts", "docReceipts"))
      .select("readReceipts", "docReceipts")
      .collect()
    val headStr = results.head.getString(0)
    assert(headStr === "")
    val docHeadStr = results.head.getString(1)
    assert(docHeadStr.startsWith(
      ("""{"Tax":{"valueNumber":104.4,"page":1,"boundingBox"""").stripMargin))
  }

  test("Basic Usage with Bytes") {
    val results = bytesDF2.mlTransform(bytesAnalyzeReceipts,
      flattenReadResults("receipts", "readReceipts"),
      flattenDocumentResults("receipts", "docReceipts"))
      .select("readReceipts", "docReceipts")
      .collect()
    val headStr = results.head.getString(0)
    assert(headStr === "")
    val docHeadStr = results.head.getString(1)
    assert(docHeadStr.startsWith(
      ("""{"Tax":{"valueNumber":104.4,"page":1,"boundingBox":""").stripMargin))
  }

  override def testObjects(): Seq[TestObject[AnalyzeReceipts]] =
    Seq(new TestObject(analyzeReceipts, imageDf2))

  override def reader: MLReadable[_] = AnalyzeReceipts
}

class AnalyzeBusinessCardsSuite extends TransformerFuzzing[AnalyzeBusinessCards] with FormRecognizerUtils {

  lazy val analyzeBusinessCards: AnalyzeBusinessCards = new AnalyzeBusinessCards()
    .setSubscriptionKey(cognitiveKey).setLocation("eastus")
    .setImageUrlCol("source").setOutputCol("businessCards").setConcurrency(5)

  lazy val bytesAnalyzeBusinessCards: AnalyzeBusinessCards = new AnalyzeBusinessCards()
    .setSubscriptionKey(cognitiveKey).setLocation("eastus")
    .setImageBytesCol("imageBytes").setOutputCol("businessCards").setConcurrency(5)

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    def prep(df: DataFrame) = {
      df.select("source", "businessCards.analyzeResult.readResults")
    }

    super.assertDFEq(prep(df1), prep(df2))(eq)
  }

  test("Basic Usage with URL") {
    val results = imageDf3.mlTransform(analyzeBusinessCards,
      flattenReadResults("businessCards", "readBusinessCards"),
      flattenDocumentResults("businessCards", "docBusinessCards"))
      .select("readBusinessCards", "docBusinessCards")
      .collect()
    val headStr = results.head.getString(0)
    assert(headStr === "")
    val docHeadStr = results.head.getString(1)
    assert(docHeadStr.startsWith((
      """{"Addresses":{"type":"array","valueArray":["{\"type\":\"string\",\"valueString""").stripMargin))
  }

  test("Basic Usage with Bytes") {
    val results = bytesDF3.mlTransform(bytesAnalyzeBusinessCards,
      flattenReadResults("businessCards", "readBusinessCards"),
      flattenDocumentResults("businessCards", "docBusinessCards"))
      .select("readBusinessCards", "docBusinessCards")
      .collect()
    val headStr = results.head.getString(0)
    assert(headStr === "")
    val docHeadStr = results.head.getString(1)
    assert(docHeadStr.startsWith((
      """{"Addresses":{"type":"array","valueArray":["{\"type\":\"string\",\"valueString\"""").stripMargin))
  }

  override def testObjects(): Seq[TestObject[AnalyzeBusinessCards]] =
    Seq(new TestObject(analyzeBusinessCards, imageDf3))

  override def reader: MLReadable[_] = AnalyzeBusinessCards
}

class AnalyzeInvoicesSuite extends TransformerFuzzing[AnalyzeInvoices] with FormRecognizerUtils {

  lazy val analyzeInvoices: AnalyzeInvoices = new AnalyzeInvoices()
    .setSubscriptionKey(cognitiveKey).setLocation("eastus")
    .setImageUrlCol("source").setOutputCol("invoices").setConcurrency(5)

  lazy val bytesAnalyzeInvoices: AnalyzeInvoices = new AnalyzeInvoices()
    .setSubscriptionKey(cognitiveKey).setLocation("eastus")
    .setImageBytesCol("imageBytes").setOutputCol("invoices").setConcurrency(5)

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    def prep(df: DataFrame) = {
      df.select("source", "invoices.analyzeResult.readResults")
    }

    super.assertDFEq(prep(df1), prep(df2))(eq)
  }

  test("Basic Usage with URL") {
    val results = imageDf4.mlTransform(analyzeInvoices,
      flattenReadResults("invoices", "readInvoices"),
      flattenDocumentResults("invoices", "docInvoices"))
      .select("readInvoices", "docInvoices")
      .collect()
    val headStr = results.head.getString(0)
    assert(headStr === "")
    val docHeadStr = results.head.getString(1)
    assert(docHeadStr.startsWith((
      """{"CustomerAddress":{"page":1,"valueString":"1020 Enterprise Way Sunnayvale, CA 87659"""").stripMargin))
  }

  test("Basic Usage with pdf") {
    val results = pdfDf2.mlTransform(analyzeInvoices,
      flattenReadResults("invoices", "readInvoices"),
      flattenDocumentResults("invoices", "docInvoices"))
      .select("readInvoices", "docInvoices")
      .collect()
    val headStr = results.head.getString(0)
    assert(headStr === "")
    val docHeadStr = results.head.getString(1)
    assert(docHeadStr.startsWith("""{"CustomerAddress":{"page":1,"valueString":"123 Other St, Redmond WA, 98052","""))
  }

  test("Basic Usage with Bytes") {
    val results = bytesDF4.mlTransform(bytesAnalyzeInvoices,
      flattenReadResults("invoices", "readInvoices"),
      flattenDocumentResults("invoices", "docInvoices"))
      .select("readInvoices", "docInvoices")
      .collect()
    val headStr = results.head.getString(0)
    assert(headStr === "")
    val docHeadStr = results.head.getString(1)
    assert(docHeadStr.startsWith((
      """{"CustomerAddress":{"page":1,"valueString":"1020 Enterprise Way Sunnayvale, CA 87659"""").stripMargin))
  }

  override def testObjects(): Seq[TestObject[AnalyzeInvoices]] =
    Seq(new TestObject(analyzeInvoices, imageDf4))

  override def reader: MLReadable[_] = AnalyzeInvoices
}

class AnalyzeIDDocumentsSuite extends TransformerFuzzing[AnalyzeIDDocuments] with FormRecognizerUtils {

  lazy val analyzeIDDocuments: AnalyzeIDDocuments = new AnalyzeIDDocuments()
    .setSubscriptionKey(cognitiveKey).setLocation("eastus")
    .setImageUrlCol("source").setOutputCol("ids").setConcurrency(5)

  lazy val bytesAnalyzeIDDocuments: AnalyzeIDDocuments = new AnalyzeIDDocuments()
    .setSubscriptionKey(cognitiveKey).setLocation("eastus")
    .setImageBytesCol("imageBytes").setOutputCol("ids").setConcurrency(5)

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    def prep(df: DataFrame) = {
      df.select("source", "ids.analyzeResult.readResults")
    }

    super.assertDFEq(prep(df1), prep(df2))(eq)
  }

  test("Basic Usage with URL") {
    val results = imageDf5.mlTransform(analyzeIDDocuments,
      flattenReadResults("ids", "readIds"),
      flattenDocumentResults("ids", "docIds"))
      .select("readIds", "docIds")
      .collect()
    val headStr = results.head.getString(0)
    assert(headStr === "")
    val docHeadStr = results.head.getString(1)
    assert(docHeadStr.startsWith((
      """{"DateOfExpiration":{"page":1,"boundingBox":""").stripMargin))
  }

  test("Basic Usage with Bytes") {
    val results = bytesDF5.mlTransform(bytesAnalyzeIDDocuments,
      flattenReadResults("ids", "readIds"),
      flattenDocumentResults("ids", "docIds"))
      .select("readIds", "docIds")
      .collect()
    val headStr = results.head.getString(0)
    assert(headStr === "")
    val docHeadStr = results.head.getString(1)
    assert(docHeadStr.startsWith((
      """{"DateOfExpiration":{"page":1,"boundingBox":""").stripMargin))
  }

  override def testObjects(): Seq[TestObject[AnalyzeIDDocuments]] =
    Seq(new TestObject(analyzeIDDocuments, imageDf5))

  override def reader: MLReadable[_] = AnalyzeIDDocuments
}

trait CustomModelUtils extends TestBase {

  // TODO: renew the SAS after 2022-07-01 since it will expire
  lazy val trainingDataSAS: String = "https://mmlspark.blob.core.windows.net/datasets" //?sp=rl&st=2021" +
  //"-06-30T04:29:50Z&se=2022-07-01T04:45:00Z&sv=2020-08-04&sr=c&sig=sdsOSpWptIoI3aSceGlGvQhjnOTJTAABghIajrOXJD8%3D"

  lazy val getRequestUrl: String = FormRecognizerUtils.formPost("", TrainCustomModelSchema(
    trainingDataSAS, SourceFilter("CustomModelTrain", includeSubFolders = false), useLabelFile = false))

  var modelToDelete = false

  lazy val modelId: Option[String] = retry(List(10000, 20000, 30000), () => {
    val resp = FormRecognizerUtils.formGet(getRequestUrl)
    val modelInfo = resp.parseJson.asJsObject.fields.getOrElse("modelInfo", "")
    val status = modelInfo match {
      case x: JsObject => x.fields.getOrElse("status", "") match {
        case y: JsString => y.value
        case _ => throw new RuntimeException(s"No status found in response/modelInfo: $resp/$modelInfo")
      }
      case _ => throw new RuntimeException(s"No modelInfo found in response: $resp")
    }
    status match {
      case "ready" =>
        modelToDelete = true
        modelInfo.asInstanceOf[JsObject].fields.get("modelId").map(_.asInstanceOf[JsString].value)
      case "creating" => throw new RuntimeException("model creating ...")
      case s => throw new RuntimeException(s"Received unknown status code: $s")
    }
  })

  override def afterAll(): Unit = {
    if (modelToDelete) {
      modelId.foreach(FormRecognizerUtils.formDelete(_))
    }
    super.afterAll()
  }
}

class ListCustomModelsSuite extends TransformerFuzzing[ListCustomModels]
  with FormRecognizerUtils with CustomModelUtils {

  lazy val listCustomModels: ListCustomModels = {
    new ListCustomModels()
      .setSubscriptionKey(cognitiveKey)
      .setLocation("eastus")
      .setOp("full")
      .setOutputCol("models")
      .setConcurrency(5)
  }

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    def prep(df: DataFrame) = {
      df.select("models.summary.count")
    }

    super.assertDFEq(prep(df1), prep(df2))(eq)
  }

  test("List model list details") {
    print(modelId) // Trigger model creation
    val results = pathDf.mlTransform(listCustomModels,
      flattenModelList("models", "modelIds"))
      .select("modelIds")
      .collect()
    assert(results.head.getString(0) != "")
  }

  test("List model list summary") {
    print(modelId) // Trigger model creation
    val results = listCustomModels.setOp("summary").transform(pathDf)
      .withColumn("modelCount", col("models").getField("summary").getField("count"))
      .select("modelCount")
      .collect()
    assert(results.head.getInt(0) >= 1)
  }

  override def testObjects(): Seq[TestObject[ListCustomModels]] =
    Seq(new TestObject(listCustomModels, pathDf))

  override def reader: MLReadable[_] = ListCustomModels
}

class GetCustomModelSuite extends TransformerFuzzing[GetCustomModel]
  with FormRecognizerUtils with CustomModelUtils {

  lazy val getCustomModel: GetCustomModel = new GetCustomModel()
    .setSubscriptionKey(cognitiveKey).setLocation("eastus")
    .setModelId(modelId.get).setIncludeKeys(true)
    .setOutputCol("model").setConcurrency(5)

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    def prep(df: DataFrame) = {
      df.select("model.trainResult.trainingDocuments")
    }

    super.assertDFEq(prep(df1), prep(df2))(eq)
  }

  test("Get model detail") {
    val results = getCustomModel.transform(pathDf)
      .withColumn("keys", col("model").getField("keys"))
      .select("keys")
      .collect()
    assert(results.head.getString(0) ===
      ("""{"clusters":{"0":["BILL TO:","CUSTOMER ID:","CUSTOMER NAME:","DATE:","DESCRIPTION",""" +
        """"DUE DATE:","F.O.B. POINT","INVOICE:","P.O. NUMBER","QUANTITY","REMIT TO:","REQUISITIONER",""" +
        """"SALESPERSON","SERVICE ADDRESS:","SHIP TO:","SHIPPED VIA","TERMS","TOTAL","UNIT PRICE"]}}""").stripMargin)
  }

  override def testObjects(): Seq[TestObject[GetCustomModel]] =
    Seq(new TestObject(getCustomModel, pathDf))

  override def reader: MLReadable[_] = GetCustomModel
}

class AnalyzeCustomModelSuite extends TransformerFuzzing[AnalyzeCustomModel]
  with FormRecognizerUtils with CustomModelUtils {

  lazy val analyzeCustomModel: AnalyzeCustomModel = new AnalyzeCustomModel()
    .setSubscriptionKey(cognitiveKey).setLocation("eastus").setModelId(modelId.get)
    .setImageUrlCol("source").setOutputCol("form").setConcurrency(5)

  lazy val bytesAnalyzeCustomModel: AnalyzeCustomModel = new AnalyzeCustomModel()
    .setSubscriptionKey(cognitiveKey).setLocation("eastus").setModelId(modelId.get)
    .setImageBytesCol("imageBytes").setOutputCol("form").setConcurrency(5)

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    def prep(df: DataFrame) = {
      df.select("source", "form.analyzeResult.readResults")
    }

    super.assertDFEq(prep(df1), prep(df2))(eq)
  }

  test("Basic Usage with URL") {
    val results = imageDf4.mlTransform(analyzeCustomModel,
      flattenReadResults("form", "readForm"),
      flattenPageResults("form", "pageForm"),
      flattenDocumentResults("form", "docForm"))
      .select("readForm", "pageForm", "docForm")
      .collect()
    assert(results.head.getString(0) === "")
    assert(results.head.getString(1)
      .contains("""Tables: Invoice Number | Invoice Date | Invoice Due Date | Charges | VAT ID"""))
    assert(results.head.getString(2) === "")
  }

  test("Basic Usage with Bytes") {
    val results = bytesDF4.mlTransform(bytesAnalyzeCustomModel,
      flattenReadResults("form", "readForm"),
      flattenPageResults("form", "pageForm"),
      flattenDocumentResults("form", "docForm"))
      .select("readForm", "pageForm", "docForm")
      .collect()
    assert(results.head.getString(0) === "")
    assert(results.head.getString(1)
      .contains("""Tables: Invoice Number | Invoice Date | Invoice Due Date | Charges | VAT ID"""))
    assert(results.head.getString(2) === "")
  }

  override def testObjects(): Seq[TestObject[AnalyzeCustomModel]] =
    Seq(new TestObject(analyzeCustomModel, imageDf4))

  override def reader: MLReadable[_] = AnalyzeCustomModel
}
