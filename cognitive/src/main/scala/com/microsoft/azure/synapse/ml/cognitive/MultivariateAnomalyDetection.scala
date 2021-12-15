// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cognitive

import com.microsoft.azure.synapse.ml.build.BuildInfo
import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.cognitive.MADJsonProtocol._
import com.microsoft.azure.synapse.ml.core.contracts.HasOutputCol
import com.microsoft.azure.synapse.ml.core.schema.DatasetExtensions
import com.microsoft.azure.synapse.ml.io.http.HandlingUtils.{convertAndClose, sendWithRetries}
import com.microsoft.azure.synapse.ml.io.http._
import com.microsoft.azure.synapse.ml.logging.BasicLogging
import com.microsoft.azure.synapse.ml.stages.{DropColumns, Lambda}
import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.HttpGet
import org.apache.http.entity.{AbstractHttpEntity, ContentType, StringEntity}
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.spark.ml._
import org.apache.spark.ml.param.{Param, ParamMap, ServiceParam}
import org.apache.spark.ml.util._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.{col, lit, struct}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import spray.json._

import java.net.URI
import java.util.concurrent.TimeoutException
import scala.concurrent.blocking
import scala.language.existentials


trait HasMADSource extends HasServiceParams {
  val source = new ServiceParam[String](this, "source", "The blob link to the input data. " +
    "It should be a zipped folder containing csv files. Each csv file should has two columns with header 'timestamp'" +
    " and 'value' (case sensitive). The file name will be used as the variable name. The variables used for" +
    " detection should be exactly the same as for training. Please refer to the sample data to prepare your" +
    " own data accordingly.", isRequired = true)

  def setSource(v: String): this.type = setScalarParam(source, v)

  def setSourceCol(v: String): this.type = setVectorParam(source, v)

  def getSource: String = getScalarParam(source)

  def getSourceCol: String = getVectorParam(source)
}

trait HasMADStartTime extends HasServiceParams {
  val startTime = new ServiceParam[String](this, "startTime", "A required field, start time" +
    " of data to be used for detection/generating multivariate anomaly detection model, should be date-time.",
    isRequired = true)

  def setStartTime(v: String): this.type = setScalarParam(startTime, v)

  def setStartTimeCol(v: String): this.type = setVectorParam(startTime, v)

  def getStartTime: String = getScalarParam(startTime)

  def getStartTimeCol: String = getVectorParam(startTime)

}

trait HasMADEndTime extends HasServiceParams {
  val endTime = new ServiceParam[String](this, "endTime", "A required field, end time of data" +
    " to be used for detection/generating multivariate anomaly detection model, should be date-time.",
    isRequired = true)

  def setEndTime(v: String): this.type = setScalarParam(endTime, v)

  def setEndTimeCol(v: String): this.type = setVectorParam(endTime, v)

  def getEndTime: String = getScalarParam(endTime)

  def getEndTimeCol: String = getVectorParam(endTime)
}

trait MADBase extends HasAsyncReply with HasMADSource with HasMADStartTime with HasMADEndTime
  with HasCognitiveServiceInput with HasInternalJsonOutputParser with HasSetLocation with Wrappable
  with HTTPParams with HasOutputCol with HasURL with ComplexParamsWritable
  with HasSubscriptionKey with HasErrorCol with BasicLogging {

  setDefault(
    outputCol -> (this.uid + "_output"),
    errorCol -> (this.uid + "_error"))

  protected def queryForResult(key: Option[String],
                               client: CloseableHttpClient,
                               location: URI): Option[HTTPResponseData] = {
    val get = new HttpGet()
    get.setURI(location)
    key.foreach(get.setHeader("Ocp-Apim-Subscription-Key", _))
    get.setHeader("User-Agent", s"synapseml/${BuildInfo.version}${HeaderValues.PlatformInfo}")
    val resp = convertAndClose(sendWithRetries(client, get, getBackoffs))
    get.releaseConnection()
    val fields = IOUtils.toString(resp.entity.get.content, "UTF-8").parseJson.asJsObject.fields
    val status = if (fields.keySet.contains("modelInfo")) {
      fields("modelInfo").asInstanceOf[JsObject].fields
        .get("status").map(_.convertTo[String]).get.toLowerCase()
    } else if (fields.keySet.contains("summary")) {
      fields("summary").asInstanceOf[JsObject]
        .fields.get("status").map(_.convertTo[String]).get.toLowerCase()
    } else {
      "None"
    }
    status match {
      case "ready" | "failed" => Some(resp)
      case "created" | "running" => None
      case s => throw new RuntimeException(s"Received unknown status code: $s")
    }
  }

  protected def handlingFunc(client: CloseableHttpClient,
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
}

object MultivariateAnomalyEstimator extends ComplexParamsReadable[MultivariateAnomalyEstimator] with Serializable

class MultivariateAnomalyEstimator(override val uid: String) extends Estimator[DetectMultivariateAnomaly]
  with MADBase {
  logClass()

  def this() = this(Identifiable.randomUID("MultivariateAnomalyModel"))

  def urlPath: String = "anomalydetector/v1.1-preview/multivariate/models"

  val slidingWindow = new ServiceParam[Int](this, "slidingWindow", "An optional field, indicates" +
    " how many history points will be used to determine the anomaly score of one subsequent point.", {
    case Left(x) => (x >= 28) && (x <= 2880)
    case Right(_) => true
  }, isRequired = true)

  def setSlidingWindow(v: Int): this.type = setScalarParam(slidingWindow, v)

  def setSlidingWindowCol(v: String): this.type = setVectorParam(slidingWindow, v)

  def getSlidingWindow: Int = getScalarParam(slidingWindow)

  def getSlidingWindowCol: String = getVectorParam(slidingWindow)

  val alignMode = new ServiceParam[String](this, "alignMode", "An optional field, indicates how " +
    "we align different variables into the same time-range which is required by the model.{Inner, Outer}", {
    case Left(s) => Set("inner", "outer")(s.toLowerCase)
    case Right(_) => true
  })

  def setAlignMode(v: String): this.type = setScalarParam(alignMode, v.toLowerCase.capitalize)

  def setAlignModeCol(v: String): this.type = setVectorParam(alignMode, v)

  def getAlignMode: String = getScalarParam(alignMode)

  def getAlignModeCol: String = getVectorParam(alignMode)

  val fillNAMethod = new ServiceParam[String](this, "fillNAMethod", "An optional field, indicates how missed " +
    "values will be filled with. Can not be set to NotFill, when alignMode is Outer.{Previous, Subsequent," +
    " Linear, Zero, Fixed}", {
    case Left(s) => Set("previous", "subsequent", "linear", "zero", "fixed")(s.toLowerCase)
    case Right(_) => true
  })

  def setFillNAMethod(v: String): this.type = setScalarParam(fillNAMethod, v.toLowerCase.capitalize)

  def setFillNAMethodCol(v: String): this.type = setVectorParam(fillNAMethod, v)

  def getFillNAMethod: String = getScalarParam(fillNAMethod)

  def getFillNAMethodCol: String = getVectorParam(fillNAMethod)

  val paddingValue = new ServiceParam[Int](this, "paddingValue", "optional field, is only useful" +
    " if FillNAMethod is set to Fixed.")

  def setPaddingValue(v: Int): this.type = setScalarParam(paddingValue, v)

  def setPaddingValueCol(v: String): this.type = setVectorParam(paddingValue, v)

  def getPaddingValue: Int = getScalarParam(paddingValue)

  def getPaddingValueCol: String = getVectorParam(paddingValue)

  val displayName = new ServiceParam[String](this, "displayName", "optional field," +
    " name of the model")

  def setDisplayName(v: String): this.type = setScalarParam(displayName, v)

  def setDisplayNameCol(v: String): this.type = setVectorParam(displayName, v)

  def getDisplayName: String = getScalarParam(displayName)

  def getDisplayNameCol: String = getVectorParam(displayName)

  val diagnosticsInfo = new ServiceParam[DiagnosticsInfo](this, "diagnosticsInfo",
    "diagnosticsInfo for training a multivariate anomaly detection model")

  def setDiagnosticsInfo(v: DiagnosticsInfo): this.type = setScalarParam(diagnosticsInfo, v)

  def getDiagnosticsInfo: DiagnosticsInfo = getScalarParam(diagnosticsInfo)

  override protected def prepareEntity: Row => Option[AbstractHttpEntity] = {
    r =>
      Some(new StringEntity(Map("source" -> getValue(r, source).toJson,
        "startTime" -> getValue(r, startTime).toJson,
        "endTime" -> getValue(r, endTime).toJson,
        "slidingWindow" -> getValue(r, slidingWindow).toJson,
        "alignPolicy" -> Map("alignMode" -> getValueOpt(r, alignMode).toJson,
          "fillNAMethod" -> getValueOpt(r, fillNAMethod).toJson,
          "paddingValue" -> getValueOpt(r, paddingValue).toJson).toJson,
        "displayName" -> getValueOpt(r, displayName).toJson)
        .toJson.compactPrint, ContentType.APPLICATION_JSON))
  }

  override def responseDataType: DataType = MAEResponse.schema

  protected def getInternalTransformer(schema: StructType): PipelineModel = {
    val dynamicParamColName = DatasetExtensions.findUnusedColumnName("dynamic", schema)
    val badColumns = getVectorParamMap.values.toSet.diff(schema.fieldNames.toSet)
    assert(badColumns.isEmpty,
      s"Could not find dynamic columns: $badColumns in columns: ${schema.fieldNames.toSet}")

    val missingRequiredParams = this.getRequiredParams.filter {
      p => this.get(p).isEmpty && this.getDefault(p).isEmpty
    }
    assert(missingRequiredParams.isEmpty,
      s"Missing required params: ${missingRequiredParams.map(s => s.name).mkString("(", ", ", ")")}")

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

  override def fit(dataset: Dataset[_]): DetectMultivariateAnomaly = {
    logFit({
      import MADJsonProtocol._

      val df = getInternalTransformer(dataset.schema)
        .transform(dataset)
        .withColumn("diagnosticsInfo", col(getOutputCol)
          .getField("modelInfo").getField("diagnosticsInfo"))
        .withColumn("modelId", col(getOutputCol).getField("modelId"))
        .select(getOutputCol, "modelId", "diagnosticsInfo")
        .collect()
      this.setDiagnosticsInfo(df.head.get(2).asInstanceOf[GenericRowWithSchema]
        .json.parseJson.convertTo[DiagnosticsInfo])
      val modelId = df.head.getString(1)
      new DetectMultivariateAnomaly()
        .setSubscriptionKey(getSubscriptionKey)
        .setLocation(getUrl.split("/".toCharArray)(2).split(".".toCharArray).head)
        .setModelId(modelId)
        .setSourceCol(getSourceCol)
        .setStartTime(getStartTime)
        .setEndTime(getEndTime)
    })
  }

  override def copy(extra: ParamMap): Estimator[DetectMultivariateAnomaly] = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    getInternalTransformer(schema).transformSchema(schema)
  }
}

object DetectMultivariateAnomaly extends ComplexParamsReadable[DetectMultivariateAnomaly] with Serializable

class DetectMultivariateAnomaly(override val uid: String) extends Model[DetectMultivariateAnomaly]
  with MADBase {
  logClass()

  def this() = this(Identifiable.randomUID("DetectMultivariateAnomaly"))

  def urlPath: String = "anomalydetector/v1.1-preview/multivariate/models/"

  val modelId = new ServiceParam[String](this, "modelId", "Format - uuid. Model identifier.",
    isRequired = true)

  def setModelId(v: String): this.type = setScalarParam(modelId, v)

  def setModelIdCol(v: String): this.type = setVectorParam(modelId, v)

  def getModelId: String = getScalarParam(modelId)

  def getModelIdCol: String = getVectorParam(modelId)

  override protected def prepareEntity: Row => Option[AbstractHttpEntity] = { row =>
    Some(new StringEntity(DMARequest(
      getValue(row, source),
      getValue(row, startTime),
      getValue(row, endTime)
    ).toJson.compactPrint))
  }

  override protected def prepareUrl: Row => String = {
    val urlParams: Array[ServiceParam[Any]] =
      getUrlParams.asInstanceOf[Array[ServiceParam[Any]]];
    // This semicolon is needed to avoid argument confusion
    { row: Row =>
      val base = getUrl + s"${getValue(row, modelId)}/detect"
      val appended = if (!urlParams.isEmpty) {
        "?" + URLEncodingUtils.format(urlParams.flatMap(p =>
          getValueOpt(row, p).map(v => p.name -> p.toValueString(v))
        ).toMap)
      } else {
        ""
      }
      base + appended
    }
  }

  override def responseDataType: DataType = DMAResponse.schema

  protected def getInternalTransformer(schema: StructType): PipelineModel = {
    val dynamicParamColName = DatasetExtensions.findUnusedColumnName("dynamic", schema)
    val badColumns = getVectorParamMap.values.toSet.diff(schema.fieldNames.toSet)
    assert(badColumns.isEmpty,
      s"Could not find dynamic columns: $badColumns in columns: ${schema.fieldNames.toSet}")

    val missingRequiredParams = this.getRequiredParams.filter {
      p => this.get(p).isEmpty && this.getDefault(p).isEmpty
    }
    assert(missingRequiredParams.isEmpty,
      s"Missing required params: ${missingRequiredParams.map(s => s.name).mkString("(", ", ", ")")}")

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

  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame](
      getInternalTransformer(dataset.schema).transform(dataset)
    )
  }

  override def copy(extra: ParamMap): DetectMultivariateAnomaly = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    getInternalTransformer(schema).transformSchema(schema)
  }

}
