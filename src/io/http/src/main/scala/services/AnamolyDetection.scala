// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import com.microsoft.ml.spark.cognitive._
import org.apache.http.entity.{AbstractHttpEntity, StringEntity}
import org.apache.spark.ml.param.ServiceParam
import org.apache.spark.ml.util._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import AnomalyDetectorProtocol._
import org.apache.spark.sql.functions.udf
import spray.json._
import spray.json.DefaultJsonProtocol._

import scala.language.existentials

abstract class AnomalyDetectorBase(override val uid: String) extends CognitiveServicesBase(uid)
with HasCognitiveServiceInput with HasInternalJsonOutputParser {
  val series = new ServiceParam[Seq[TimeSeriesPoint]](this, "series",
    """
      |Time series data points. Points should be sorted by timestamp in ascending order
      |to match the anomaly detection result. If the data is not sorted correctly or
      |there is duplicated timestamp, the API will not work.
      |In such case, an error message will be returned.
    """.stripMargin.replace("\n", ""),
    { _ => true },
    isRequired = true
  )

  def setSeries(v: Seq[TimeSeriesPoint]): this.type = setScalarParam(series, v)

  def setSeriesCol(v: String): this.type = setVectorParam(series, v)

  val granularity = new ServiceParam[String](this, "granularity",
    """
      |Can only be one of yearly, monthly, weekly, daily, hourly or minutely.
      |Granularity is used for verify whether input series is valid.
    """.stripMargin.replace("\n", ""),
    { _ => true },
    isRequired = true
  )

  def setGranularity(v: String): this.type = setScalarParam(granularity, v)

  def setGranularityCol(v: String): this.type = setVectorParam(granularity, v)

  val maxAnomalyRatio = new ServiceParam[Double](this, "maxAnomalyRatio",
    """
      |Optional argument, advanced model parameter, max anomaly ratio in a time series.
    """.stripMargin.replace("\n", ""),
    { _ => true },
    isRequired = false
  )

  def setMaxAnomalyRatio(v: Double): this.type = setScalarParam(maxAnomalyRatio, v)

  def setMaxAnomalyRatioCol(v: String): this.type = setVectorParam(maxAnomalyRatio, v)

  val sensitivity = new ServiceParam[Int](this, "sensitivity",
    """
      |Optional argument, advanced model parameter, between 0-99,
      |the lower the value is, the larger the margin value will be which means less anomalies will be accepted
    """.stripMargin.replace("\n", ""),
    { _ => true },
    isRequired = false
  )

  def setSensitivity(v: Int): this.type = setScalarParam(sensitivity, v)

  def setSensitivityCol(v: String): this.type = setVectorParam(sensitivity, v)

  val customInterval = new ServiceParam[Int](this, "customInterval",
    """
      |Custom Interval is used to set non-standard time interval, for example, if the series is 5 minutes,
      | request can be set as granularity=minutely, customInterval=5.
    """.stripMargin.replace("\n", ""),
    { _ => true },
    isRequired = false
  )

  def setCustomInterval(v: Int): this.type = setScalarParam(customInterval, v)

  def setCustomIntervalCol(v: String): this.type = setVectorParam(customInterval, v)

  val period = new ServiceParam[Int](this, "period",
    """
      |Optional argument, periodic value of a time series.
      |If the value is null or does not present, the API will determine the period automatically.
    """.stripMargin.replace("\n", ""),
    { _ => true },
    isRequired = false
  )

  def setPeriod(v: Int): this.type = setScalarParam(period, v)

  def setPeriodCol(v: String): this.type = setVectorParam(period, v)

  override protected def prepareEntity: Row => Option[AbstractHttpEntity] = { row =>
    Some(new StringEntity(ADRequest(
      getValueAny(row, series).asInstanceOf[Seq[Any]].map {
        case tsp: TimeSeriesPoint => tsp
        case r: Row => TimeSeriesPoint(r.getString(0), r.getDouble(1))
      },
      getValue(row, granularity),
      getValueOpt(row, maxAnomalyRatio),
      getValueOpt(row, sensitivity),
      getValueOpt(row, customInterval),
      getValueOpt(row, period)
    ).toJson.compactPrint))
  }

}

object DetectLastAnomaly extends ComplexParamsReadable[DetectLastAnomaly] with Serializable

class DetectLastAnomaly(override val uid: String) extends AnomalyDetectorBase(uid) {

  def this() = this(Identifiable.randomUID("DetectLastAnomoly"))

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/anomalydetector/v1.0/timeseries/last/detect")

  override def responseDataType: DataType = ADLastResponse.schema

}

object DetectAnomalies extends ComplexParamsReadable[DetectAnomalies] with Serializable

class DetectAnomalies(override val uid: String) extends AnomalyDetectorBase(uid) {

  def this() = this(Identifiable.randomUID("DetectAnomalies"))

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/anomalydetector/v1.0/timeseries/entire/detect")

  override def responseDataType: DataType = ADEntireResponse.schema

}
