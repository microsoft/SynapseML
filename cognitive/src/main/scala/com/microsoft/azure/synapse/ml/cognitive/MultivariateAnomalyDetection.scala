// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cognitive

import com.microsoft.azure.synapse.ml.build.BuildInfo
import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.cognitive.MADJsonProtocol._
import com.microsoft.azure.synapse.ml.io.http.HandlingUtils.{convertAndClose, sendWithRetries}
import com.microsoft.azure.synapse.ml.io.http.{HTTPRequestData, HTTPResponseData, HandlingUtils, HeaderValues}
import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.HttpGet
import org.apache.http.entity.{AbstractHttpEntity, ContentType, StringEntity}
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.spark.ml._
import org.apache.spark.ml.param.ServiceParam
import org.apache.spark.ml.util._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
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

trait MADAsyncReply extends HasAsyncReply {

  protected def queryForResult(key: Option[String],
                               client: CloseableHttpClient,
                               location: URI): Option[HTTPResponseData] = {
    val get = new HttpGet()
    get.setURI(location)
    key.foreach(get.setHeader("Ocp-Apim-Subscription-Key", _))
    get.setHeader("User-Agent", s"synapseml/${BuildInfo.version}${HeaderValues.PlatformInfo}")
    val resp = convertAndClose(sendWithRetries(client, get, getBackoffs))
    get.releaseConnection()
    val status = IOUtils.toString(resp.entity.get.content, "UTF-8").parseJson.asJsObject.fields("modelInfo")
      .asInstanceOf[JsObject].fields.get("status").map(_.convertTo[String])
    status.map(_.toLowerCase()).flatMap {
      case "ready" | "failed" | "created" => Some(resp)
      case "running" => None
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

object MultivariateAnomalyModel extends ComplexParamsReadable[MultivariateAnomalyModel] with Serializable

class MultivariateAnomalyModel(override val uid: String) extends CognitiveServicesBaseNoHandler(uid)
  with MADAsyncReply with HasMADSource with HasMADStartTime with HasMADEndTime with Wrappable
  with HasCognitiveServiceInput with HasInternalJsonOutputParser with HasSetLocation  {
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
    case Left(s) => Set("Inner", "Outer")(s)
    case Right(_) => true
  })

  def setAlignMode(v: String): this.type = setScalarParam(alignMode, v)

  def setAlignModeCol(v: String): this.type = setVectorParam(alignMode, v)

  def getAlignMode: String = getScalarParam(alignMode)

  def getAlignModeCol: String = getVectorParam(alignMode)

  val fillNAMethod = new ServiceParam[String](this, "fillNAMethod", "An optional field, indicates how missed " +
    "values will be filled with. Can not be set to NotFill, when alignMode is Outer.{Previous, Subsequent," +
    " Linear, Zero, Pad, NotFill}", {
    case Left(s) => Set("Previous", "Subsequent", "Linear", "Zero", "Pad", "NotFill")(s)
    case Right(_) => true
  })

  def setFillNAMethod(v: String): this.type = setScalarParam(fillNAMethod, v)

  def setFillNAMethodCol(v: String): this.type = setVectorParam(fillNAMethod, v)

  def getFillNAMethod: String = getScalarParam(fillNAMethod)

  def getFillNAMethodCol: String = getVectorParam(fillNAMethod)

  val paddingValue = new ServiceParam[Int](this, "paddingValue", "optional field, only be useful" +
    " if FillNAMethod is set to Pad.")

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

  override def responseDataType: DataType = MAMResponse.schema
}

object DetectMultivariateAnomaly extends ComplexParamsReadable[DetectMultivariateAnomaly] with Serializable

class DetectMultivariateAnomaly(override val uid: String) extends CognitiveServicesBaseNoHandler(uid)
  with HasMADSource with HasMADStartTime with HasMADEndTime with HasCognitiveServiceInput with Wrappable
  with HasInternalJsonOutputParser with HasSetLocation with BasicAsyncReply {
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

}
