// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cognitive.anomaly

import com.microsoft.azure.synapse.ml.build.BuildInfo
import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.cognitive.anomaly.MADJsonProtocol._
import com.microsoft.azure.synapse.ml.cognitive.vision.HasAsyncReply
import com.microsoft.azure.synapse.ml.cognitive._
import com.microsoft.azure.synapse.ml.core.contracts.{HasInputCols, HasOutputCol}
import com.microsoft.azure.synapse.ml.core.env.StreamUtilities.using
import com.microsoft.azure.synapse.ml.io.http.HandlingUtils.{convertAndClose, sendWithRetries}
import com.microsoft.azure.synapse.ml.io.http.RESTHelpers.{Client, retry}
import com.microsoft.azure.synapse.ml.io.http._
import com.microsoft.azure.synapse.ml.logging.SynapseMLLogging
import com.microsoft.azure.synapse.ml.param.CognitiveServiceStructParam
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.http.client.methods._
import org.apache.http.entity.{AbstractHttpEntity, ContentType, StringEntity}
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.spark.ml._
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spray.json._

import java.io.{BufferedOutputStream, OutputStream, PrintWriter}
import java.net.URI
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeoutException
import java.util.zip.{ZipEntry, ZipOutputStream}
import scala.collection.parallel.mutable
import scala.collection.parallel.mutable.ParHashSet
import scala.concurrent.blocking
import scala.language.existentials

object MADUtils {

  private[ml] val CreatedModels: mutable.ParHashSet[String] = new ParHashSet[String]()

  private[ml] def madSend(request: HttpRequestBase,
                          path: String,
                          key: String,
                          params: Map[String, String] = Map()): String = {

    val paramString = if (params.isEmpty) {
      ""
    } else {
      "?" + URLEncodingUtils.format(params)
    }
    request.setURI(new URI(path + paramString))

    retry(List(100, 500, 1000), { () =>  //scalastyle:ignore magic.number
      request.addHeader("Ocp-Apim-Subscription-Key", key)
      request.addHeader("Content-Type", "application/json")
      using(Client.execute(request)) { response =>
        if (!response.getStatusLine.getStatusCode.toString.startsWith("2")) {
          val bodyOpt = request match {
            case er: HttpEntityEnclosingRequestBase => IOUtils.toString(er.getEntity.getContent, "UTF-8")
            case _ => ""
          }
          if (response.getStatusLine.getStatusCode.toString.equals("429")) {
            val retryTime = response.getHeaders("Retry-After").head.getValue.toInt * 1000
            Thread.sleep(retryTime.toLong)
          }
          throw new RuntimeException(s"Failed: response: $response " + s"requestUrl: ${request.getURI} " +
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

  private[ml] def madGetModel(url: String, modelId: String,
                              key: String, params: Map[String, String] = Map()): String = {
    madSend(new HttpGet(), url + modelId, key, params)
  }

  private[ml] def madUrl(location: String): String = {
    s"https://$location.api.cognitive.microsoft.com/anomalydetector/v1.1-preview/multivariate/"
  }

  private[ml] def madDelete(modelId: String,
                            key: String,
                            location: String,
                            params: Map[String, String] = Map()): String = {
    madSend(new HttpDelete(), madUrl(location) + "models/" + modelId, key, params)
  }

  private[ml] def madListModels(key: String,
                                location: String,
                                params: Map[String, String] = Map()): String = {
    madSend(new HttpGet(), madUrl(location) + "models", key, params)
  }

  private[ml] def cleanUpAllModels(key: String, location: String): Unit = {
    for (modelId <- CreatedModels) {
      println(s"Deleting mvad model $modelId")
      madDelete(modelId, key, location)
    }
    CreatedModels.clear()
  }

}

trait MADHttpRequest extends HasURL with HasSubscriptionKey with HasAsyncReply {
  protected def prepareUrl: String

  protected def prepareMethod(): HttpRequestBase = new HttpPost()

  protected def prepareEntity(source: String): Option[AbstractHttpEntity]

  protected val subscriptionKeyHeaderName = "Ocp-Apim-Subscription-Key"

  protected def contentType: String = "application/json"

  protected def prepareRequest(entity: AbstractHttpEntity): Option[HttpRequestBase] = {
    val req = prepareMethod()
    req.setURI(new URI(prepareUrl))
    req.setHeader(subscriptionKeyHeaderName, getSubscriptionKey)
    req.setHeader("Content-Type", contentType)

    req match {
      case er: HttpEntityEnclosingRequestBase =>
        er.setEntity(entity)
      case _ =>
    }
    Some(req)
  }

  protected def queryForResult(key: Option[String],
                               client: CloseableHttpClient,
                               location: URI): Option[HTTPResponseData] = {
    val get = new HttpGet()
    get.setURI(location)
    key.foreach(get.setHeader("Ocp-Apim-Subscription-Key", _))
    get.setHeader("User-Agent", s"synapseml/${BuildInfo.version}${HeaderValues.PlatformInfo}")
    val resp = convertAndClose(sendWithRetries(client, get, getBackoffs))
    get.releaseConnection()
    Some(resp)
  }

  //noinspection ScalaStyle
  protected def timeoutResult(key: Option[String], client: CloseableHttpClient,
                              location: URI, maxTries: Int): HTTPResponseData = {
    throw new TimeoutException(
      s"Querying for results did not complete within $maxTries tries")
  }

  //noinspection ScalaStyle
  protected def handlingFunc(client: CloseableHttpClient,  //scalastyle:ignore cyclomatic.complexity
                             request: HTTPRequestData): HTTPResponseData = {
    val response = HandlingUtils.advanced(getBackoffs: _*)(client, request)
    if (response.statusLine.statusCode == 201) {
      val location = new URI(response.headers.filter(_.name == "Location").head.value)
      val maxTries = getMaxPollingRetries
      val key = request.headers.find(_.name == "Ocp-Apim-Subscription-Key").map(_.value)
      val it = (0 to maxTries).toIterator.flatMap { _ =>
        val resp = queryForResult(key, client, location)
        val fields = IOUtils.toString(resp.get.entity.get.content, "UTF-8").parseJson.asJsObject.fields
        val status = fields match {
          case f if f.contains("modelInfo") => f("modelInfo").convertTo[MAEModelInfo].status
          case f if f.contains("summary") => f("summary").convertTo[DMASummary].status
          case _ => "None"
        }
        status.toLowerCase() match {
          case "ready" | "failed" => resp
          case "created" | "running" =>
            blocking {
              Thread.sleep(getPollingDelay.toLong)
            }
            None
          case s => throw new RuntimeException(s"Received unknown status code: $s")
        }
      }
      if (it.hasNext) {
        it.next()
      } else {
        timeoutResult(key, client, location, maxTries)
      }
    } else {
      val error = IOUtils.toString(response.entity.get.content, "UTF-8")
        .parseJson.convertTo[DMAError].toJson.compactPrint
      throw new RuntimeException(s"Caught error: $error")
    }
  }
}

private case class StorageInfo(account: String, container: String, key: String, blob: String)

trait MADBase extends HasOutputCol
  with MADHttpRequest with HasSetLocation with HasInputCols
  with ComplexParamsWritable with Wrappable
  with HasErrorCol with SynapseMLLogging {

  private def convertTimeFormat(name: String, v: String): String = {
    try {
      DateTimeFormatter.ISO_INSTANT.format(DateTimeFormatter.ISO_INSTANT.parse(v))
    }
    catch {
      case e: java.time.format.DateTimeParseException =>
        throw new IllegalArgumentException(
          s"${name.capitalize} should be ISO8601 format. e.g. 2021-01-01T00:00:00Z, received: ${e.toString}")
    }
  }

  val startTime = new Param[String](this, "startTime", "A required field, start time" +
    " of data to be used for detection/generating multivariate anomaly detection model, should be date-time.")

  def setStartTime(v: String): this.type = set(startTime, convertTimeFormat(startTime.name, v))

  def getStartTime: String = $(startTime)

  val endTime = new Param[String](this, "endTime", "A required field, end time of data" +
    " to be used for detection/generating multivariate anomaly detection model, should be date-time.")

  def setEndTime(v: String): this.type = set(endTime, convertTimeFormat(endTime.name, v))

  def getEndTime: String = $(endTime)

  val timestampCol = new Param[String](this, "timestampCol", "Timestamp column name")

  def setTimestampCol(v: String): this.type = set(timestampCol, v)

  def getTimestampCol: String = $(timestampCol)

  setDefault(timestampCol -> "timestamp")

  private def validateIntermediateSaveDir(dir: String): Boolean = {
    if(!dir.startsWith("wasbs://") && !dir.startsWith("abfss://")) {
      throw new IllegalArgumentException("improper HDFS loacation. Please use a wasb path such as: \n" +
        "wasbs://[CONTAINER]@[ACCOUNT].blob.core.windows.net/[DIRECTORY]" +
        "For more information on connecting storage accounts to spark visit " +
        "https://docs.microsoft.com/en-us/azure/databricks/data/data-sources" +
        "/azure/azure-storage#--access-azure-data-lake-storage-gen2-or-blob-storage-using-the-account-key"
      )
    }
    true
  }

  val intermediateSaveDir = new Param[String](
    this,
    "intermediateSaveDir",
    "Blob storage location in HDFS where intermediate data is saved while training.",
    isValid = validateIntermediateSaveDir _
  )

  def setIntermediateSaveDir(v: String): this.type = set(intermediateSaveDir, v)

  def getIntermediateSaveDir: String = $(intermediateSaveDir)

  setDefault(
    outputCol -> (this.uid + "_output"),
    errorCol -> (this.uid + "_error"))

  private def getStorageInfo: StorageInfo = {
    val uri = new URI(getIntermediateSaveDir)
    val account = uri.getHost.split(".".toCharArray).head
    val blobConfig = s"fs.azure.account.key.$account.blob.core.windows.net"
    val adlsConfig = s"fs.azure.account.key.$account.dfs.core.windows.net"
    val hc = SparkSession.builder().getOrCreate()
      .sparkContext.hadoopConfiguration
    val key = Option(hc.get(adlsConfig)).orElse(Option(hc.get(blobConfig)))

    if (key.isEmpty){
      throw new IllegalAccessError("Could not find the storage account credentials." +
        s" Make sure your hadoopConfiguration has the" +
        s" ''$blobConfig'' or ''$adlsConfig'' configuration set.")
    }

    StorageInfo(account, uri.getUserInfo, key.get, uri.getPath.stripPrefix("/"))
  }

  protected def blob: String = s"${getStorageInfo.blob}/$uid.zip"

  protected def blobPath: Path = new Path(new URI(getIntermediateSaveDir.stripSuffix("/") + s"/$uid.zip"))

  protected def upload(df: DataFrame): String = {
    val timestampColumn = df.schema
      .find(p => p.name == getTimestampCol)
      .get
    val timestampColIdx = df.schema.indexOf(timestampColumn)
    val rows = df.collect

    val storageInfo = getStorageInfo
    val hconf = df.sparkSession.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(blobPath.toUri, hconf)

    // Upload to Blob
    using(new ZipOutputStream(new BufferedOutputStream(fs.create(blobPath)))) { os =>
      // loop over all features
      for (feature <- df.schema.filter(p => p != timestampColumn).zipWithIndex) {
        val featureIdx = df.schema.indexOf(feature._1)
        // create zip entry. must be named series_{idx}
        os.putNextEntry(new ZipEntry(s"series_${feature._2}.csv"))
        // write CSV
        storeFeatureInCsv(rows, timestampColIdx, featureIdx, os)
        os.closeEntry()
      }
    }.get

    // generate SAS
    val offset = OffsetDateTime.now().plusDays(1)
    val sas = SlimStorageClient.generateReadSAS(
      storageInfo.account, storageInfo.container, blob, storageInfo.key, offset)
    s"https://${storageInfo.account}.blob.core.windows.net/${storageInfo.container}/$blob?$sas"
  }

  protected def storeFeatureInCsv(rows: Array[Row],
                                  timestampColIdx: Int,
                                  featureIdx: Int,
                                  out: OutputStream): Unit = {
    // create CSV file per feature
    val pw = new PrintWriter(out)
    // CSV header
    pw.println("timestamp,value")
    for (row <- rows) {
      // <timestamp>,<value>
      // make sure it's ISO8601. e.g. 2021-01-01T00:00:00Z
      val formattedTimestamp = convertTimeFormat("Timestamp column", row.getString(timestampColIdx))
      pw.print(formattedTimestamp)
      pw.write(',')
      // TODO: do we have to worry about locale?
      // pw.format(Locale.US, "%f", row.get(featureIdx))
      pw.println(row.get(featureIdx))
    }
    pw.flush()

  }

  def cleanUpIntermediateData(): Unit = {
    val hconf = SparkSession.builder().getOrCreate().sparkContext.hadoopConfiguration
    val fs = FileSystem.get(blobPath.toUri, hconf)
    fs.delete(blobPath, false)
  }

  override def pyAdditionalMethods: String = super.pyAdditionalMethods + {
    """
      |def cleanUpIntermediateData(self):
      |    self._java_obj.cleanUpIntermediateData()
      |    return
      |""".stripMargin
  }

  protected def submitDatasetAndJob(dataset: Dataset[_]): Map[String, JsValue] = {
    val df = dataset.toDF().select((Array(getTimestampCol) ++ getInputCols).map(col): _*)
    val sasUrl = upload(df)

    val httpRequestBase = prepareRequest(prepareEntity(sasUrl).get)
    val request = new HTTPRequestData(httpRequestBase.get)
    val response = handlingFunc(Client, request)

    val responseJson = IOUtils.toString(response.entity.get.content, "UTF-8")
      .parseJson.asJsObject.fields

    responseJson
  }

}

object FitMultivariateAnomaly extends ComplexParamsReadable[FitMultivariateAnomaly] with Serializable

class FitMultivariateAnomaly(override val uid: String) extends Estimator[DetectMultivariateAnomaly]
  with MADBase {
  logClass()

  def this() = this(Identifiable.randomUID("FitMultivariateAnomaly"))

  def urlPath: String = "anomalydetector/v1.1-preview/multivariate/models"

  val slidingWindow = new IntParam(this, "slidingWindow", "An optional field, indicates" +
    " how many history points will be used to determine the anomaly score of one subsequent point.")

  def setSlidingWindow(v: Int): this.type = {
    if ((v >= 28) && (v <= 2880)) {
      set(slidingWindow, v)
    } else {
      throw new IllegalArgumentException("slidingWindow must be between 28 and 2880 (both inclusive).")
    }
  }

  def getSlidingWindow: Int = $(slidingWindow)

  val alignMode = new Param[String](this, "alignMode", "An optional field, indicates how " +
    "we align different variables into the same time-range which is required by the model.{Inner, Outer}")

  def setAlignMode(v: String): this.type = {
    if (Set("inner", "outer").contains(v.toLowerCase)) {
      set(alignMode, v.toLowerCase.capitalize)
    } else {
      throw new IllegalArgumentException("alignMode must be either `inner` or `outer`.")
    }
  }

  def getAlignMode: String = $(alignMode)

  val fillNAMethod = new Param[String](this, "fillNAMethod", "An optional field, indicates how missed " +
    "values will be filled with. Can not be set to NotFill, when alignMode is Outer.{Previous, Subsequent," +
    " Linear, Zero, Fixed}")

  def setFillNAMethod(v: String): this.type = {
    if (Set("previous", "subsequent", "linear", "zero", "fixed").contains(v.toLowerCase)) {
      set(fillNAMethod, v.toLowerCase.capitalize)
    } else {
      throw new IllegalArgumentException("fillNAMethod must be one of {Previous, Subsequent, Linear, Zero, Fixed}.")
    }
  }

  def getFillNAMethod: String = $(fillNAMethod)

  val paddingValue = new IntParam(this, "paddingValue", "optional field, is only useful" +
    " if FillNAMethod is set to Fixed.")

  def setPaddingValue(v: Int): this.type = set(paddingValue, v)

  def getPaddingValue: Int = $(paddingValue)

  val displayName = new Param[String](this, "displayName", "optional field," +
    " name of the model")

  def setDisplayName(v: String): this.type = set(displayName, v)

  def getDisplayName: String = $(displayName)

  protected def prepareEntity(source: String): Option[AbstractHttpEntity] = {
    Some(new StringEntity(
      MAERequest(
        source,
        getStartTime,
        getEndTime,
        get(slidingWindow),
        Option(AlignPolicy(
          get(alignMode),
          get(fillNAMethod),
          get(paddingValue))),
        get(displayName)
      ).toJson.compactPrint, ContentType.APPLICATION_JSON))
  }

  protected def prepareUrl: String = getUrl

  //noinspection ScalaStyle
  override protected def timeoutResult(key: Option[String], client: CloseableHttpClient,
                                       location: URI, maxTries: Int): HTTPResponseData = {
    // if no response after max retries, return the response containing modelId directly
    queryForResult(key, client, location).get
  }

  override def fit(dataset: Dataset[_]): DetectMultivariateAnomaly = {
    logFit({
      val response = submitDatasetAndJob(dataset)

      val modelInfo = response("modelInfo").asJsObject.fields
      val modelId = response("modelId").convertTo[String]

      if (modelInfo("status").asInstanceOf[JsString].value.toLowerCase() == "failed") {
        val errors = modelInfo("errors").convertTo[Seq[DMAError]].toJson.compactPrint
        throw new RuntimeException(s"Caught errors during fitting: $errors")
      }

      MADUtils.CreatedModels += modelId

      new DetectMultivariateAnomaly()
        .setSubscriptionKey(getSubscriptionKey)
        .setLocation(getUrl.split("/".toCharArray)(2).split(".".toCharArray).head)
        .setModelId(modelId)
        .setIntermediateSaveDir(getIntermediateSaveDir)
        .setDiagnosticsInfo(modelInfo("diagnosticsInfo").convertTo[DiagnosticsInfo])
    })
  }

  override def copy(extra: ParamMap): FitMultivariateAnomaly = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    schema.add(getErrorCol, DMAError.schema)
      .add(getOutputCol, DMAResponse.schema)
      .add("isAnomaly", BooleanType)
  }

}

object DetectMultivariateAnomaly extends ComplexParamsReadable[DetectMultivariateAnomaly] with Serializable

class DetectMultivariateAnomaly(override val uid: String) extends Model[DetectMultivariateAnomaly]
  with MADBase {
  logClass()

  def this() = this(Identifiable.randomUID("DetectMultivariateAnomaly"))

  def urlPath: String = "anomalydetector/v1.1-preview/multivariate/models/"

  val modelId = new Param[String](this, "modelId", "Format - uuid. Model identifier.")

  def setModelId(v: String): this.type = set(modelId, v)

  def getModelId: String = $(modelId)

  val diagnosticsInfo = new CognitiveServiceStructParam[DiagnosticsInfo](this, "diagnosticsInfo",
    "diagnosticsInfo for training a multivariate anomaly detection model")

  def setDiagnosticsInfo(v: DiagnosticsInfo): this.type = set(diagnosticsInfo, v)

  def getDiagnosticsInfo: DiagnosticsInfo = $(diagnosticsInfo)

  protected def prepareEntity(source: String): Option[AbstractHttpEntity] = {
    Some(new StringEntity(
      DMARequest(source, getStartTime, getEndTime)
        .toJson.compactPrint))
  }

  protected def prepareUrl: String = getUrl + s"$getModelId/detect"

  //noinspection ScalaStyle
  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame] {

      // check model status first
      try {
        val response = MADUtils.madGetModel(getUrl, getModelId, getSubscriptionKey)
          .parseJson.asJsObject.fields

        val modelInfo = response("modelInfo").asJsObject.fields
        val modelStatus = modelInfo("status").asInstanceOf[JsString].value.toLowerCase
        modelStatus match {
          case "failed" =>
            val errors = modelInfo("errors").convertTo[Seq[DMAError]].toJson.compactPrint
            throw new RuntimeException(s"Caught errors during fitting: $errors")
          case "created" | "running" =>
            throw new RuntimeException(s"model $getModelId is not ready yet")
          case "ready" =>
            logInfo("model is ready for inference")
        }
      } catch {
        case e: RuntimeException =>
          throw new RuntimeException(s"Encounter error while fetching model $getModelId, " +
            s"please double check the modelId is correct: ${e.getMessage}")
      }

      val spark = dataset.sparkSession
      val responseJson = submitDatasetAndJob(dataset)

      val summary = responseJson("summary").convertTo[DMASummary]

      if (summary.status.toLowerCase() == "failed") {
        val errors = summary.errors.get.toJson.compactPrint
        throw new RuntimeException(s"Failure during inference: $errors")
      }

      val jsonDF = spark.createDataset(Seq(responseJson("results").toString()))(Encoders.STRING)

      val parsedJsonDF = spark.read.json(jsonDF).toDF()
        .sort(col("timestamp").asc)
        .withColumnRenamed("timestamp", "resultTimestamp")

      val simplifiedDF = if (parsedJsonDF.columns.contains("value")) {
        parsedJsonDF.withColumn("isAnomaly", col("value.isAnomaly"))
          .withColumnRenamed("value", getOutputCol)
      } else {
        parsedJsonDF.withColumn(getOutputCol, lit(None))
          .withColumn("isAnomaly", lit(None))
      }

      val finalDF = if (simplifiedDF.columns.contains("errors")) {
        simplifiedDF.withColumnRenamed("errors", getErrorCol)
      } else {
        simplifiedDF.withColumn(getErrorCol, lit(None))
      }

      val df = dataset.toDF()
      df.join(finalDF, df(getTimestampCol) === finalDF("resultTimestamp"), "left")
        .drop("resultTimestamp")
        .sort(col(getTimestampCol).asc)
    }
  }

  override def copy(extra: ParamMap): DetectMultivariateAnomaly = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    schema.add(getErrorCol, DMAError.schema)
      .add(getOutputCol, DMAResponse.schema)
      .add("isAnomaly", BooleanType)
  }

}
