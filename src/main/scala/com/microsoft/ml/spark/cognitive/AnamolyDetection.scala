// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cognitive

import com.microsoft.ml.spark.cognitive._
import org.apache.http.entity.{AbstractHttpEntity, StringEntity}
import org.apache.spark.ml.param.{Param, ServiceParam}
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types._
import AnomalyDetectorProtocol._
import com.microsoft.ml.spark.core.contracts.HasOutputCol
import com.microsoft.ml.spark.core.schema.DatasetExtensions
import com.microsoft.ml.spark.io.http.ErrorUtils
import com.microsoft.ml.spark.logging.BasicLogging
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.ComplexParamsReadable
import org.apache.spark.sql.functions.{arrays_zip, col, collect_list, explode, size, struct, udf}
import spray.json._
import spray.json.DefaultJsonProtocol._

import scala.language.existentials

abstract class AnomalyDetectorBase(override val uid: String) extends CognitiveServicesBase(uid)
  with HasCognitiveServiceInput with HasInternalJsonOutputParser with HasSetLocation {

  val granularity = new ServiceParam[String](this, "granularity",
    """
      |Can only be one of yearly, monthly, weekly, daily, hourly or minutely.
      |Granularity is used for verify whether input series is valid.
    """.stripMargin.replace("\n", " ").replace("\r", " "),
    { _ => true },
    isRequired = true
  )

  def setGranularity(v: String): this.type = setScalarParam(granularity, v)

  def setGranularityCol(v: String): this.type = setVectorParam(granularity, v)

  val maxAnomalyRatio = new ServiceParam[Double](this, "maxAnomalyRatio",
    """
      |Optional argument, advanced model parameter, max anomaly ratio in a time series.
    """.stripMargin.replace("\n", " ").replace("\r", " "),
    { _ => true },
    isRequired = false
  )

  def setMaxAnomalyRatio(v: Double): this.type = setScalarParam(maxAnomalyRatio, v)

  def setMaxAnomalyRatioCol(v: String): this.type = setVectorParam(maxAnomalyRatio, v)

  val sensitivity = new ServiceParam[Int](this, "sensitivity",
    """
      |Optional argument, advanced model parameter, between 0-99,
      |the lower the value is, the larger the margin value will be which means less anomalies will be accepted
    """.stripMargin.replace("\n", " ").replace("\r", " "),
    { _ => true },
    isRequired = false
  )

  def setSensitivity(v: Int): this.type = setScalarParam(sensitivity, v)

  def setSensitivityCol(v: String): this.type = setVectorParam(sensitivity, v)

  val customInterval = new ServiceParam[Int](this, "customInterval",
    """
      |Custom Interval is used to set non-standard time interval, for example, if the series is 5 minutes,
      | request can be set as granularity=minutely, customInterval=5.
    """.stripMargin.replace("\n", " ").replace("\r", " "),
    { _ => true },
    isRequired = false
  )

  def setCustomInterval(v: Int): this.type = setScalarParam(customInterval, v)

  def setCustomIntervalCol(v: String): this.type = setVectorParam(customInterval, v)

  val period = new ServiceParam[Int](this, "period",
    """
      |Optional argument, periodic value of a time series.
      |If the value is null or does not present, the API will determine the period automatically.
    """.stripMargin.replace("\n", " ").replace("\r", " "),
    { _ => true },
    isRequired = false
  )

  def setPeriod(v: Int): this.type = setScalarParam(period, v)

  def setPeriodCol(v: String): this.type = setVectorParam(period, v)

  val series = new ServiceParam[Seq[TimeSeriesPoint]](this, "series",
    """
      |Time series data points. Points should be sorted by timestamp in ascending order
      |to match the anomaly detection result. If the data is not sorted correctly or
      |there is duplicated timestamp, the API will not work.
      |In such case, an error message will be returned.
    """.stripMargin.replace("\n", " ").replace("\r", " "),
    { _ => true },
    isRequired = true
  )

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

class DetectLastAnomaly(override val uid: String) extends AnomalyDetectorBase(uid) with BasicLogging {
  logClass(uid)

  def this() = this(Identifiable.randomUID("DetectLastAnomaly"))

  def setSeries(v: Seq[TimeSeriesPoint]): this.type = setScalarParam(series, v)

  def setSeriesCol(v: String): this.type = setVectorParam(series, v)

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/anomalydetector/v1.0/timeseries/last/detect")

  override def responseDataType: DataType = ADLastResponse.schema

}

object DetectAnomalies extends ComplexParamsReadable[DetectAnomalies] with Serializable

class DetectAnomalies(override val uid: String) extends AnomalyDetectorBase(uid) with BasicLogging {
  logClass(uid)

  def this() = this(Identifiable.randomUID("DetectAnomalies"))

  def setSeries(v: Seq[TimeSeriesPoint]): this.type = setScalarParam(series, v)

  def setSeriesCol(v: String): this.type = setVectorParam(series, v)

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/anomalydetector/v1.0/timeseries/entire/detect")

  override def responseDataType: DataType = ADEntireResponse.schema

}

object SimpleDetectAnomalies extends ComplexParamsReadable[SimpleDetectAnomalies] with Serializable

class SimpleDetectAnomalies(override val uid: String) extends AnomalyDetectorBase(uid)
  with HasOutputCol with BasicLogging {
  logClass(uid)

  def this() = this(Identifiable.randomUID("SimpleDetectAnomalies"))

  val timestampCol = new Param[String](this, "timestampCol", "column representing the time of the series")

  def setTimestampCol(v: String): this.type = set(timestampCol, v)

  def getTimestampCol: String = $(timestampCol)

  val valueCol = new Param[String](this, "valueCol", "column representing the value of the series")

  def setValueCol(v: String): this.type = set(valueCol, v)

  def getValueCol: String = $(valueCol)

  val groupbyCol = new Param[String](this, "groupbyCol", "column that groups the series")

  def setGroupbyCol(v: String): this.type = set(groupbyCol, v)

  def getGroupbyCol: String = $(groupbyCol)

  setDefault(
    timestampCol -> "timestamp",
    valueCol -> "value",
    outputCol -> s"${uid}_output")

  private def sortWithContext(timeSeries: Seq[Row], context: Seq[Row]): Row = {
    val s1 = timeSeries.zipWithIndex.sortBy(r => r._1.getString(0))
    val s2 = s1.map(s => context(s._2))
    Row(s1.map(_._1), s2)
  }

  private def formatResultsFunc(): (Row, Int) => Seq[Row] = {
    val fromRow = ADEntireResponse.makeFromRowConverter
    val toRow = ADSingleResponse.makeToRowConverter;
    { case (result, count) =>
      Option(result)
        .map(res => fromRow(res).explode.map(toRow))
        .getOrElse(Seq.fill[Row](count)(null)) // scalastyle:ignore null
    }
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform(uid, dataset)
    val contextCol = DatasetExtensions.findUnusedColumnName("context", dataset.schema)
    val inputsCol = DatasetExtensions.findUnusedColumnName("inputs", dataset.schema)
    setVectorParam(series, inputsCol)

    val inputDF = dataset.toDF()
      .withColumn(contextCol, struct("*"))
      .withColumn(inputsCol, struct(
        col(getTimestampCol).alias("timestamp"),
        col(getValueCol).alias("value")))
    val sortUDF = UDFUtils.oldUdf(sortWithContext _,
      new StructType()
        .add(inputsCol, ArrayType(TimeSeriesPoint.schema))
        .add(contextCol, ArrayType(inputDF.schema(contextCol).dataType))
    )

    val groupedDF = inputDF
      .groupBy(getGroupbyCol)
      .agg(
        collect_list(inputsCol).alias(inputsCol),
        collect_list(contextCol).alias(contextCol))
      .select(sortUDF(col(inputsCol), col(contextCol)).alias("sorted"))
      .select("sorted.*")

    val outputDF = super.transform(groupedDF)

    outputDF.select(
      col(getErrorCol),
      explode(arrays_zip(
        col(contextCol),
        UDFUtils.oldUdf(formatResultsFunc(), ArrayType(ADSingleResponse.schema))(
          col(getOutputCol), size(col(contextCol))).alias(getOutputCol)
      )).alias(getOutputCol)
    ).select(
      s"$getOutputCol.$contextCol.*",
      getErrorCol,
      s"$getOutputCol.1"
    ).withColumnRenamed("1", getOutputCol)

  }

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/anomalydetector/v1.0/timeseries/entire/detect")

  override def responseDataType: DataType = ADEntireResponse.schema

  override def transformSchema(schema: StructType): StructType = {
    schema.add(getErrorCol, ErrorUtils.ErrorSchema)
      .add(getOutputCol, ADSingleResponse.schema)
  }
}
