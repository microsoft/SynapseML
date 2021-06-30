// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cognitive.split1

import com.microsoft.ml.spark.FluentAPI._
import com.microsoft.ml.spark.build.BuildInfo
import com.microsoft.ml.spark.cognitive._
import com.microsoft.ml.spark.core.env.StreamUtilities.using
import com.microsoft.ml.spark.core.schema.{DatasetExtensions, SparkBindings}
import com.microsoft.ml.spark.core.test.base.{Flaky, TestBase}
import com.microsoft.ml.spark.core.test.fuzzing.{EstimatorFuzzing, ModelFuzzing, TestObject, TransformerFuzzing}
import com.microsoft.ml.spark.io.http.HandlingUtils.{convertAndClose, sendWithRetries}
import com.microsoft.ml.spark.io.http.{HTTPRequestData, HTTPResponseData,
  HandlingUtils, HeaderValues, SimpleHTTPTransformer}
import com.microsoft.ml.spark.stages.{DropColumns, Lambda}
import org.apache.commons.io.IOUtils
import org.apache.http.client.methods._
import org.apache.http.entity.{AbstractHttpEntity, ContentType, StringEntity}
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.spark.ml.param.{ParamMap, ServiceParam}
import org.apache.spark.ml.{ComplexParamsReadable, Estimator, NamespaceInjections, PipelineModel}
import org.apache.spark.ml.util.{Identifiable, MLReadable}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.{col, lit, struct}
import org.apache.spark.sql.types.{DataType, StructType}
import org.scalactic.Equality
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.net.URI
import java.util.concurrent.TimeoutException
import scala.concurrent.blocking

object TrainCustomModelResponse extends SparkBindings[TrainCustomModelResponse]

case class TrainCustomModelResponse(modelInfo: ModelInfo, trainResult: TrainResult)

object TrainCustomModel extends ComplexParamsReadable[TrainCustomModel]

class TrainCustomModel(override val uid: String) extends Estimator[AnalyzeCustomModel]
  with CustomFormRecognizer {
  logClass()

  def this() = this(Identifiable.randomUID("TrainCustomModel"))

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/formrecognizer/v2.1/custom/models")

  def getLocation: String = getUrl.split("/")(2).split("\\.")(0)

  val prefix = new ServiceParam[String](this, "prefix", "A case-sensitive prefix string to filter" +
    " documents in the source path for training. For example, when using a Azure storage blob Uri, use the prefix " +
    "to restrict sub folders for training.")

  def setPrefix(v: String): this.type = setScalarParam(prefix, v)

  val includeSubFolders = new ServiceParam[Boolean](this, "includeSubFolders", "A flag to indicate " +
    "if sub folders within the set of prefix folders will also need to be included when searching for content " +
    "to be preprocessed.")

  def setIncludeSubFolders(v: Boolean): this.type = setScalarParam(includeSubFolders, v)

  val useLabelFile = new ServiceParam[Boolean](this, "useLabelFile", "Use label file for training a model.")

  def setUseLabelFile(v: Boolean): this.type = setScalarParam(useLabelFile, v)

  setDefault(outputCol -> (this.uid + "_output"), errorCol -> (this.uid + "_error"),
    includeSubFolders -> Left(false), useLabelFile -> Left(false))

  override protected def prepareEntity: Row => Option[AbstractHttpEntity] = {
    r => Some(new StringEntity(Map("source" -> getValue(r, imageUrl).toJson,
      "sourceFilter" -> Map("prefix" -> getValue(r, prefix).toJson,
        "includeSubFolders" -> getValue(r, includeSubFolders).toJson).toJson,
      "useLabelFile" -> getValue(r, useLabelFile).toJson).toJson.compactPrint, ContentType.APPLICATION_JSON))
  }

  private def queryForResult(key: Option[String],
                             client: CloseableHttpClient,
                             location: URI): Option[HTTPResponseData] = {
    val get = new HttpGet()
    get.setURI(location)
    key.foreach(get.setHeader("Ocp-Apim-Subscription-Key", _))
    get.setHeader("User-Agent", s"mmlspark/${BuildInfo.version}${HeaderValues.PlatformInfo}")
    val resp = convertAndClose(sendWithRetries(client, get, getBackoffs))
    get.releaseConnection()
    val modelInfo = IOUtils.toString(resp.entity.get.content, "UTF-8").
      parseJson.asJsObject.fields.getOrElse("modelInfo", "")
    var status = ""
    modelInfo match {
      case x: JsObject => x.fields.getOrElse("status", "") match {
        case y: JsString => status = y.value
        case _ => throw new RuntimeException(s"No status found in response/modelInfo: $resp/$modelInfo")
      }
      case _ => throw new RuntimeException(s"No modelInfo found in response: $resp")
    }
    status match {
      case "ready" | "invalid" => Some(resp)
      case "creating" => None
      case s => throw new RuntimeException(s"Received unknown status code: $s")
    }
  }

  override protected def handlingFunc(client: CloseableHttpClient,
                                      request: HTTPRequestData): HTTPResponseData = {
    val response = HandlingUtils.advanced(getBackoffs: _*)(client, request)
    if (response.statusLine.statusCode == 201) {
      val location = new URI(response.headers.filter(_.name == "Location").head.value)
      val maxTries = getMaxPollingRetries
      val key = request.headers.find(_.name == "Ocp-Apim-Subscription-Key").map(_.value)
      val it = (0 to maxTries).toIterator.flatMap { _ =>
        queryForResult(key, client, location).orElse({
          blocking {
            Thread.sleep(getPollingDelay.toLong)
          }
          None
        })
      }
      if (it.hasNext) {
        it.next()
      } else {
        throw new TimeoutException(
          s"Querying for results did not complete within $maxTries tries")
      }
    } else {
      response
    }
  }

  override def fit(dataset: Dataset[_]): AnalyzeCustomModel = {
    logFit({
      val modelId = getInternalTransformer(dataset.schema).transform(dataset)
        .withColumn("modelId", col($(outputCol))
          .getField("modelInfo").getField("modelId"))
        .select("modelId")
        .collect().head.getString(0)
      new AnalyzeCustomModel()
        .setSubscriptionKey(getSubscriptionKey)
        .setLocation(getLocation)
        .setModelId(modelId)
    })
  }

  override def copy(extra: ParamMap): Estimator[AnalyzeCustomModel] = defaultCopy(extra)

  protected def getInternalTransformer(schema: StructType): PipelineModel = {
    val dynamicParamColName = DatasetExtensions.findUnusedColumnName("dynamic", schema)
    val badColumns = getVectorParamMap.values.toSet.diff(schema.fieldNames.toSet)
    assert(badColumns.isEmpty,
      s"Could not find dynamic columns: $badColumns in columns: ${schema.fieldNames.toSet}")

    val dynamicParamCols = getVectorParamMap.values.toList.map(col) match {
      case Nil => Seq(lit(false).alias("placeholder"))
      case l => l
    }

    val stages = Array(
      Lambda(_.withColumn(dynamicParamColName, struct(dynamicParamCols: _*))),
      new SimpleHTTPTransformer()
        .setInputCol(dynamicParamColName)
        .setOutputCol(getOutputCol)
        .setInputParser(getInternalInputParser(schema))
        .setOutputParser(getInternalOutputParser(schema))
        .setHandler(handlingFunc)
        .setConcurrency(getConcurrency)
        .setConcurrentTimeout(get(concurrentTimeout))
        .setErrorCol(getErrorCol),
      new DropColumns().setCol(dynamicParamColName)
    )

    NamespaceInjections.pipelineModel(stages)
  }

  override def transformSchema(schema: StructType): StructType = {
    getInternalTransformer(schema).transformSchema(schema)
  }

  override protected def responseDataType: DataType = TrainCustomModelResponse.schema
}

object FormRecognizerUtils extends CognitiveKey {

  import RESTHelpers._

  val BaseURL = "https://eastus.api.cognitive.microsoft.com/formrecognizer/v2.1/custom/models"

  def formSend(request: HttpRequestBase, path: String,
               params: Map[String, String] = Map()): String = {

    val paramString = if (params.isEmpty) {
      ""
    } else {
      "?" + URLEncodingUtils.format(params)
    }
    request.setURI(new URI(BaseURL + path + paramString))

    retry(List(100, 500, 1000), { () =>
      request.addHeader("Ocp-Apim-Subscription-Key", cognitiveKey)
      request.addHeader("Content-Type", "application/json")
      using(Client.execute(request)) { response =>
        if (!response.getStatusLine.getStatusCode.toString.startsWith("2")) {
          val bodyOpt = request match {
            case er: HttpEntityEnclosingRequestBase => IOUtils.toString(er.getEntity.getContent, "UTF-8")
            case _ => ""
          }
          throw new RuntimeException(
            s"Failed: response: $response " +
              s"requestUrl: ${request.getURI}" +
              s"requestBody: $bodyOpt")
        }
        if (response.getStatusLine.getReasonPhrase == "No Content") {
          ""
        }
        else {
          IOUtils.toString(response.getEntity.getContent, "UTF-8")
        }
      }.get
    })
  }

  def formDelete(path: String, params: Map[String, String] = Map()): String = {
    formSend(new HttpDelete(), "/" + path, params)
  }
}

trait FormRecognizerUtils extends TestBase {

  import spark.implicits._

  def createTestDataframe(v: Seq[String], returnBytes: Boolean): DataFrame = {
    val df = v.toDF("source")
    if (returnBytes) {
      BingImageSearch
        .downloadFromUrls("source", "imageBytes", 4, 10000)
        .transform(df)
        .select("imageBytes")
    } else {
      df
    }
  }

  lazy val imageDf1: DataFrame = createTestDataframe(
    Seq("https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/layout1.jpg"), returnBytes = false)

  lazy val bytesDF1: DataFrame = createTestDataframe(
    Seq("https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/layout1.jpg"), returnBytes = true)

  lazy val imageDf2: DataFrame = createTestDataframe(
    Seq("https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/receipt1.png"), returnBytes = false)

  lazy val bytesDF2: DataFrame = createTestDataframe(
    Seq("https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/receipt1.png"), returnBytes = true)

  lazy val imageDf3: DataFrame = createTestDataframe(
    Seq("https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/business_card.jpg"), returnBytes = false)

  lazy val bytesDF3: DataFrame = createTestDataframe(
    Seq("https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/business_card.jpg"), returnBytes = true)

  lazy val imageDf4: DataFrame = createTestDataframe(
    Seq("https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/invoice2.png"), returnBytes = false)

  lazy val bytesDF4: DataFrame = createTestDataframe(
    Seq("https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/invoice2.png"), returnBytes = true)

  lazy val imageDf5: DataFrame = createTestDataframe(
    Seq("https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/id1.jpg"), returnBytes = false)

  lazy val bytesDF5: DataFrame = createTestDataframe(
    Seq("https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/id1.jpg"), returnBytes = true)

  lazy val pdfDf1: DataFrame = createTestDataframe(
    Seq("https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/layout2.pdf"), returnBytes = false)

  lazy val pdfDf2: DataFrame = createTestDataframe(
    Seq("https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/invoice1.pdf",
    "https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/invoice3.pdf"), returnBytes = false)

  // TODO: renew the SAS after 2022-07-01 since it will expire
  lazy val trainingDataSAS: DataFrame = createTestDataframe(
    Seq("https://mmlspark.blob.core.windows.net/datasets?sp=rl&st=2021-06-30T04:29:50Z&se=2022" +
      "-07-01T04:45:00Z&sv=2020-08-04&sr=c&sig=sdsOSpWptIoI3aSceGlGvQhjnOTJTAABghIajrOXJD8%3D"
  ), returnBytes = false)

  lazy val df: DataFrame = createTestDataframe(Seq(""), returnBytes = false)
}

class AnalyzeLayoutSuite extends TransformerFuzzing[AnalyzeLayout]
  with CognitiveKey with Flaky with FormRecognizerUtils {

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
      AnalyzeLayout.flattenReadResults("layout", "readlayout"),
      AnalyzeLayout.flattenPageResults("layout", "pageLayout"))
      .select("readlayout", "pageLayout")
      .collect()
    val headStr = results.head.getString(0)
    assert(headStr.startsWith("Purchase Order Hero Limited Purchase Order Company Phone: 555-348-6512 " +
      "Website: www.herolimited.com "))
    val pageHeadStr = results.head.getString(1)
    assert(pageHeadStr === "Details | Quantity | Unit Price | Total | Bindings | 20 | 1.00 | 20.00 | Covers Small" +
      " | 20 | 1.00 | 20.00 | Feather Bookmark | 20 | 5.00 | 100.00 | Copper Swirl Marker | 20 | 5.00 | " +
      "100.00\nSUBTOTAL | $140.00 | TAX | $4.00 |  |  | TOTAL | $144.00")
  }

  test("Basic Usage with pdf") {
    val results = pdfDf1.mlTransform(analyzeLayout,
      AnalyzeLayout.flattenReadResults("layout", "readlayout"),
      AnalyzeLayout.flattenPageResults("layout", "pageLayout"))
      .select("readlayout", "pageLayout")
      .collect()
    val headStr = results.head.getString(0)
    val correctPrefix = "UNITED STATES SECURITIES AND EXCHANGE COMMISSION Washington, D.C. 20549 FORM 10-Q"
    assert(headStr.startsWith(correctPrefix))
    val pageHeadStr = results.head.getString(1)
    assert(pageHeadStr === "Title of each class | Trading Symbol | Name of exchange on which registered | " +
      "Common stock, $0.00000625 par value per share | MSFT | NASDAQ | 2.125% Notes due 2021 | MSFT | NASDAQ |" +
      " 3.125% Notes due 2028 | MSFT | NASDAQ | 2.625% Notes due 2033 | MSFT | NASDAQ")
  }

  test("Basic Usage with Bytes") {
    val results = bytesDF1.mlTransform(bytesAnalyzeLayout,
      AnalyzeLayout.flattenReadResults("layout", "readlayout"),
      AnalyzeLayout.flattenPageResults("layout", "pageLayout"))
      .select("readlayout", "pageLayout")
      .collect()
    val headStr = results.head.getString(0)
    assert(headStr.startsWith("Purchase Order Hero Limited Purchase Order Company Phone: 555-348-6512" +
      " Website: www.herolimited.com "))
    val pageHeadStr = results.head.getString(1)
    assert(pageHeadStr === "Details | Quantity | Unit Price | Total | Bindings | 20 | 1.00 | 20.00 | Covers Small" +
      " | 20 | 1.00 | 20.00 | Feather Bookmark | 20 | 5.00 | 100.00 | Copper Swirl Marker | 20 | 5.00 | " +
      "100.00\nSUBTOTAL | $140.00 | TAX | $4.00 |  |  | TOTAL | $144.00")
  }

  override def testObjects(): Seq[TestObject[AnalyzeLayout]] =
    Seq(new TestObject(analyzeLayout, imageDf1))

  override def reader: MLReadable[_] = AnalyzeLayout
}

class AnalyzeReceiptsSuite extends TransformerFuzzing[AnalyzeReceipts]
  with CognitiveKey with Flaky with FormRecognizerUtils {

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
      AnalyzeReceipts.flattenReadResults("receipts", "readReceipts"),
      AnalyzeReceipts.flattenDocumentResults("receipts", "docReceipts"))
      .select("readReceipts", "docReceipts")
      .collect()
    val headStr = results.head.getString(0)
    assert(headStr === "")
    val docHeadStr = results.head.getString(1)
    assert(docHeadStr.startsWith(
      ("""{"Items":{"type":"array","valueArray":[{"type":"object",""" +
        """"valueObject":{"Name":{"type":"string","valueString":"Surface Pro 6","text":"Surface Pro 6""").stripMargin))
  }

  test("Basic Usage with Bytes") {
    val results = bytesDF2.mlTransform(bytesAnalyzeReceipts,
      AnalyzeReceipts.flattenReadResults("receipts", "readReceipts"),
      AnalyzeReceipts.flattenDocumentResults("receipts", "docReceipts"))
      .select("readReceipts", "docReceipts")
      .collect()
    val headStr = results.head.getString(0)
    assert(headStr === "")
    val docHeadStr = results.head.getString(1)
    assert(docHeadStr.startsWith(
      ("""{"Items":{"type":"array","valueArray":[{"type":"object",""" +
        """"valueObject":{"Name":{"type":"string","valueString":"Surface Pro 6","text":"Surface Pro 6""").stripMargin))
  }

  override def testObjects(): Seq[TestObject[AnalyzeReceipts]] =
    Seq(new TestObject(analyzeReceipts, imageDf2))

  override def reader: MLReadable[_] = AnalyzeReceipts
}

class AnalyzeBusinessCardsSuite extends TransformerFuzzing[AnalyzeBusinessCards]
  with CognitiveKey with Flaky with FormRecognizerUtils {

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
      AnalyzeBusinessCards.flattenReadResults("businessCards", "readBusinessCards"),
      AnalyzeBusinessCards.flattenDocumentResults("businessCards", "docBusinessCards"))
      .select("readBusinessCards", "docBusinessCards")
      .collect()
    val headStr = results.head.getString(0)
    assert(headStr === "")
    val docHeadStr = results.head.getString(1)
    assert(docHeadStr.startsWith((
      """{"Addresses":{"type":"array","valueArray":[{"type":""" +
      """"string","valueString":"2 Kingdom Street Paddington, London, W2 6BD""").stripMargin))
  }

  test("Basic Usage with Bytes") {
    val results = bytesDF3.mlTransform(bytesAnalyzeBusinessCards,
      AnalyzeBusinessCards.flattenReadResults("businessCards", "readBusinessCards"),
      AnalyzeBusinessCards.flattenDocumentResults("businessCards", "docBusinessCards"))
      .select("readBusinessCards", "docBusinessCards")
      .collect()
    val headStr = results.head.getString(0)
    assert(headStr === "")
    val docHeadStr = results.head.getString(1)
    assert(docHeadStr.startsWith((
      """{"Addresses":{"type":"array","valueArray":[{"type":""" +
        """"string","valueString":"2 Kingdom Street Paddington, London, W2 6BD""").stripMargin))
  }

  override def testObjects(): Seq[TestObject[AnalyzeBusinessCards]] =
    Seq(new TestObject(analyzeBusinessCards, imageDf3))

  override def reader: MLReadable[_] = AnalyzeBusinessCards
}

class AnalyzeInvoicesSuite extends TransformerFuzzing[AnalyzeInvoices]
  with CognitiveKey with Flaky with FormRecognizerUtils {

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
      AnalyzeInvoices.flattenReadResults("invoices", "readInvoices"),
      AnalyzeInvoices.flattenDocumentResults("invoices", "docInvoices"))
      .select("readInvoices", "docInvoices")
      .collect()
    val headStr = results.head.getString(0)
    assert(headStr === "")
    val docHeadStr = results.head.getString(1)
    assert(docHeadStr.startsWith((
      """{"CustomerAddress":{"type":"string","valueString":"1020 Enterprise Way Sunnayvale, CA 87659","text":""" +
        """"1020 Enterprise Way Sunnayvale, CA 87659""").stripMargin))
  }

  test("Basic Usage with pdf") {
    val results = pdfDf2.mlTransform(analyzeInvoices,
      AnalyzeInvoices.flattenReadResults("invoices", "readInvoices"),
      AnalyzeInvoices.flattenDocumentResults("invoices", "docInvoices"))
      .select("readInvoices", "docInvoices")
      .collect()
    val headStr = results.head.getString(0)
    assert(headStr === "")
    val docHeadStr = results.head.getString(1)
    assert(docHeadStr.startsWith("""{"AmountDue":{"type":"number","valueNumber":610,"text":"$610.00"""))
  }

  test("Basic Usage with Bytes") {
    val results = bytesDF4.mlTransform(bytesAnalyzeInvoices,
      AnalyzeInvoices.flattenReadResults("invoices", "readInvoices"),
      AnalyzeInvoices.flattenDocumentResults("invoices", "docInvoices"))
      .select("readInvoices", "docInvoices")
      .collect()
    val headStr = results.head.getString(0)
    assert(headStr === "")
    val docHeadStr = results.head.getString(1)
    assert(docHeadStr.startsWith((
      """{"CustomerAddress":{"type":"string","valueString":"1020 Enterprise Way Sunnayvale, CA 87659","text":""" +
        """"1020 Enterprise Way Sunnayvale, CA 87659""").stripMargin))
  }

  override def testObjects(): Seq[TestObject[AnalyzeInvoices]] =
    Seq(new TestObject(analyzeInvoices, imageDf4))

  override def reader: MLReadable[_] = AnalyzeInvoices
}

class AnalyzeIDDocumentsSuite extends TransformerFuzzing[AnalyzeIDDocuments]
  with CognitiveKey with Flaky with FormRecognizerUtils {

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
      AnalyzeIDDocuments.flattenReadResults("ids", "readIds"),
      AnalyzeIDDocuments.flattenDocumentResults("ids", "docIds"))
      .select("readIds", "docIds")
      .collect()
    val headStr = results.head.getString(0)
    assert(headStr === "")
    val docHeadStr = results.head.getString(1)
    assert(docHeadStr.startsWith((
      """{"Address":{"type":"string","valueString":"123 STREET ADDRESS YOUR CITY WA 99999-1234","text":""" +
        """"123 STREET ADDRESS YOUR CITY WA 99999-1234""").stripMargin))
  }

  test("Basic Usage with Bytes") {
    val results = bytesDF5.mlTransform(bytesAnalyzeIDDocuments,
      AnalyzeIDDocuments.flattenReadResults("ids", "readIds"),
      AnalyzeIDDocuments.flattenDocumentResults("ids", "docIds"))
      .select("readIds", "docIds")
      .collect()
    val headStr = results.head.getString(0)
    assert(headStr === "")
    val docHeadStr = results.head.getString(1)
    assert(docHeadStr.startsWith((
      """{"Address":{"type":"string","valueString":"123 STREET ADDRESS YOUR CITY WA 99999-1234","text":""" +
        """"123 STREET ADDRESS YOUR CITY WA 99999-1234""").stripMargin))
  }

  override def testObjects(): Seq[TestObject[AnalyzeIDDocuments]] =
    Seq(new TestObject(analyzeIDDocuments, imageDf5))

  override def reader: MLReadable[_] = AnalyzeIDDocuments
}

class ListCustomModelsSuite extends TransformerFuzzing[ListCustomModels]
  with CognitiveKey with Flaky with FormRecognizerUtils {

  lazy val trainCustomModel: TrainCustomModel = new TrainCustomModel()
    .setSubscriptionKey(cognitiveKey).setLocation("eastus")
    .setImageUrlCol("source").setPrefix("CustomModelTrain")
    .setOutputCol("customModelResult").setConcurrency(5)

  val modelId: String = trainCustomModel.fit(trainingDataSAS).getModelId

  override def afterAll(): Unit = {
    if (modelId != "") {
      FormRecognizerUtils.formDelete(modelId)
    }
    super.afterAll()
  }

  lazy val listCustomModels: ListCustomModels = new ListCustomModels()
    .setSubscriptionKey(cognitiveKey).setLocation("eastus")
    .setOp("full").setOutputCol("models").setConcurrency(5)

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    def prep(df: DataFrame) = {
      df.select("models.summary.count")
    }
    super.assertDFEq(prep(df1), prep(df2))(eq)
  }

  test("List model list details") {
    val results = df.mlTransform(listCustomModels,
      ListCustomModels.flattenModelList("models", "modelIds"))
      .select("modelIds")
      .collect()
    assert(results.head.getString(0) != "")
  }

  test("List model list summary") {
    val results = listCustomModels.setOp("summary").transform(df)
      .withColumn("modelCount", col("models").getField("summary").getField("count"))
      .select("modelCount")
      .collect()
    assert(results.head.getInt(0) >= 1)
  }

  override def testObjects(): Seq[TestObject[ListCustomModels]] =
    Seq(new TestObject(listCustomModels, df))

  override def reader: MLReadable[_] = ListCustomModels
}

class GetCustomModelSuite extends TransformerFuzzing[GetCustomModel]
  with CognitiveKey with Flaky with FormRecognizerUtils {

  lazy val trainCustomModel: TrainCustomModel = new TrainCustomModel()
    .setSubscriptionKey(cognitiveKey).setLocation("eastus")
    .setImageUrlCol("source").setPrefix("CustomModelTrain")
    .setOutputCol("customModelResult").setConcurrency(5)

  val modelId: String = trainCustomModel.fit(trainingDataSAS).getModelId

  override def afterAll(): Unit = {
    if (modelId != "") {
      FormRecognizerUtils.formDelete(modelId)
    }
    super.afterAll()
  }

  lazy val getCustomModel: GetCustomModel = new GetCustomModel()
    .setSubscriptionKey(cognitiveKey).setLocation("eastus")
    .setModelId(modelId).setIncludeKeys(true)
    .setOutputCol("model").setConcurrency(5)

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    def prep(df: DataFrame) = {
      df.select("model.trainResult.trainingDocuments")
    }
    super.assertDFEq(prep(df1), prep(df2))(eq)
  }

  test("Get model detail") {
    val results = getCustomModel.transform(df)
      .withColumn("keys", col("model").getField("keys"))
      .select("keys")
      .collect()
    assert(results.head.getString(0) ===
      ("""{"clusters":{"0":["BILL TO:","CUSTOMER ID:","CUSTOMER NAME:","DATE:","DESCRIPTION",""" +
        """"DUE DATE:","F.O.B. POINT","INVOICE:","P.O. NUMBER","QUANTITY","REMIT TO:","REQUISITIONER",""" +
        """"SALESPERSON","SERVICE ADDRESS:","SHIP TO:","SHIPPED VIA","TERMS","TOTAL","UNIT PRICE"]}}""").stripMargin)
  }

  override def testObjects(): Seq[TestObject[GetCustomModel]] =
    Seq(new TestObject(getCustomModel, df))

  override def reader: MLReadable[_] = GetCustomModel
}

class TrainCustomModelSuite extends EstimatorFuzzing[TrainCustomModel]
  with CognitiveKey with Flaky with FormRecognizerUtils {

  lazy val trainCustomModel: TrainCustomModel = new TrainCustomModel()
    .setSubscriptionKey(cognitiveKey).setLocation("eastus").setImageUrlCol("source")
    .setPrefix("CustomModelTrain").setOutputCol("customModelResult").setConcurrency(5)

  override def afterAll(): Unit = {
    val listCustomModels: ListCustomModels = new ListCustomModels()
      .setSubscriptionKey(cognitiveKey).setLocation("eastus")
      .setOp("full").setOutputCol("models").setConcurrency(5)
    val results = listCustomModels.transform(df)
      .withColumn("modelIds", col("models").getField("modelList").getField("modelId"))
      .select("modelIds")
      .collect()
    val modelIds = results.flatMap(_.getAs[Seq[String]](0))
    modelIds.foreach(
      x => FormRecognizerUtils.formDelete(x)
    )
    super.afterAll()
  }

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    def prep(df: DataFrame) = {
      df.select("source")
    }
    super.assertDFEq(prep(df1), prep(df2))(eq)
  }

  test("Basic Usage") {
    val analyzeCustomeModel = trainCustomModel.fit(trainingDataSAS)
      .setImageUrlCol("source").setOutputCol("form").setConcurrency(5)
    val results = imageDf4.mlTransform(analyzeCustomeModel,
      AnalyzeCustomModel.flattenReadResults("form", "readForm"),
      AnalyzeCustomModel.flattenPageResults("form", "pageForm"),
      AnalyzeCustomModel.flattenDocumentResults("form", "docForm"))
      .select("readForm", "pageForm", "docForm")
      .collect()
    assert(results.head.getString(0) === "")
    assert(results.head.getString(1)
      .startsWith(("""KeyValuePairs: key: Invoice For: value: Microsoft 1020 Enterprise Way""")))
    assert(results.head.getString(2) === "")
  }

  override def testObjects(): Seq[TestObject[TrainCustomModel]] =
    Seq(new TestObject(trainCustomModel, trainingDataSAS, imageDf4))

  override def reader: MLReadable[_] = TrainCustomModel

  override def modelReader: MLReadable[_] = reader
}

class AnalyzeCustomModelSuite extends ModelFuzzing[AnalyzeCustomModel]
  with CognitiveKey with Flaky with FormRecognizerUtils {

  lazy val trainCustomModel: TrainCustomModel = new TrainCustomModel()
    .setSubscriptionKey(cognitiveKey).setLocation("eastus").setImageUrlCol("source")
    .setPrefix("CustomModelTrain").setOutputCol("customModelResult").setConcurrency(5)

  val modelId: String = trainCustomModel.fit(trainingDataSAS).getModelId

  override def afterAll(): Unit = {
    if (modelId != "") {
      FormRecognizerUtils.formDelete(modelId)
    }
    super.afterAll()
  }

  lazy val analyzeCustomModel: AnalyzeCustomModel = new AnalyzeCustomModel()
    .setSubscriptionKey(cognitiveKey).setLocation("eastus").setModelId(modelId)
    .setImageUrlCol("source").setOutputCol("form").setConcurrency(5)

  lazy val bytesAnalyzeCustomModel: AnalyzeCustomModel = new AnalyzeCustomModel()
    .setSubscriptionKey(cognitiveKey).setLocation("eastus").setModelId(modelId)
    .setImageBytesCol("imageBytes").setOutputCol("form").setConcurrency(5)

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    def prep(df: DataFrame) = {
      df.select("source", "form.analyzeResult.readResults")
    }
    super.assertDFEq(prep(df1), prep(df2))(eq)
  }

  test("Basic Usage with URL") {
    val results = imageDf4.mlTransform(analyzeCustomModel,
      AnalyzeCustomModel.flattenReadResults("form", "readForm"),
      AnalyzeCustomModel.flattenPageResults("form", "pageForm"),
      AnalyzeCustomModel.flattenDocumentResults("form", "docForm"))
      .select("readForm", "pageForm", "docForm")
      .collect()
    assert(results.head.getString(0) === "")
    assert(results.head.getString(1)
      .startsWith(("""KeyValuePairs: key: Invoice For: value: Microsoft 1020 Enterprise Way""")))
    assert(results.head.getString(2) === "")
  }

  test("Basic Usage with Bytes") {
    val results = bytesDF4.mlTransform(bytesAnalyzeCustomModel,
      AnalyzeCustomModel.flattenReadResults("form", "readForm"),
      AnalyzeCustomModel.flattenPageResults("form", "pageForm"),
      AnalyzeCustomModel.flattenDocumentResults("form", "docForm"))
      .select("readForm", "pageForm", "docForm")
      .collect()
    assert(results.head.getString(0) === "")
    assert(results.head.getString(1)
      .startsWith(("""KeyValuePairs: key: Invoice For: value: Microsoft 1020 Enterprise Way""")))
    assert(results.head.getString(2) === "")
  }

  override def testObjects(): Seq[TestObject[AnalyzeCustomModel]] =
    Seq(new TestObject(analyzeCustomModel, imageDf4))

  override def reader: MLReadable[_] = AnalyzeCustomModel
}
