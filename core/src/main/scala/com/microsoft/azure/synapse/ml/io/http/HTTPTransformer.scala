// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.io.http

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.contracts.{HasInputCol, HasOutputCol}
import com.microsoft.azure.synapse.ml.io.http.HandlingUtils.HandlerFunc
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import com.microsoft.azure.synapse.ml.param.{GlobalKey, GlobalParams, UDFParam}
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, Transformer}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration

trait HasHandler extends Params {
  val handler: UDFParam = new UDFParam(
    this, "handler", "Which strategy to use when handling requests")

  /** @group getParam */
  def getHandler: HandlerFunc = UDFUtils.unpackUdf($(handler))._1.asInstanceOf[HandlerFunc]

  def setHandler(v: HandlerFunc): HasHandler.this.type = {
    set(handler, UDFUtils.oldUdf(v, StringType))
  }

  def setHandler(v: UserDefinedFunction): HasHandler.this.type = set(handler, v)

  setDefault(handler -> HandlingUtils.advancedUDF(100)) //scalastyle:ignore magic.number

  def handlingFunc(client: CloseableHttpClient,
                   request: HTTPRequestData): HTTPResponseData =
    getHandler(client, request)
}

trait ConcurrencyParams extends Wrappable {
  val concurrency: Param[Int] = new IntParam(
    this, "concurrency", "max number of concurrent calls")

  /** @group getParam */
  def getConcurrency: Int = $(concurrency)

  /** @group setParam */
  def setConcurrency(value: Int): this.type = set(concurrency, value)

  val apiTimeout: Param[Double] = new DoubleParam(
    this, "apiTimeout", "number of seconds to wait for a response from the API")

  /** @group getParam */
  def getApiTimeout: Double = $(apiTimeout)

  /** @group setParam */
  def setApiTimeout(value: Double): this.type = set(apiTimeout, value)

  val connectionTimeout: Param[Double] = new DoubleParam(
    this, "connectionTimeout", "number of seconds to wait for establishing a connection")

  /** @group getParam */
  def getConnectionTimeout: Double = $(connectionTimeout)

  /** @group setParam */
  def setConnectionTimeout(value: Double): this.type = set(connectionTimeout, value)

  val timeout: Param[Double] = new DoubleParam(
    this, "timeout",
    "number of seconds for the entire DataFrame transformation to complete, measured from the start of the transform operation; " +
      "rows processed after this timeout will receive HTTP 408 responses without making API calls")

  /** @group getParam */
  def getTimeout: Double = $(timeout)

  /** @group setParam */
  def setTimeout(value: Double): this.type = set(timeout, value)

  val concurrentTimeout: Param[Double] = new DoubleParam(
    this, "concurrentTimeout", "max number seconds to wait on futures if concurrency >= 1")

  /** @group getParam */
  def getConcurrentTimeout: Double = $(concurrentTimeout)

  /** @group setParam */
  def setConcurrentTimeout(value: Double): this.type = set(concurrentTimeout, value)

  def setConcurrentTimeout(value: Option[Double]): this.type = value match {
    case Some(v) => setConcurrentTimeout(v)
    case None => clear(concurrentTimeout)
  }
  setDefault(concurrency -> 1, apiTimeout -> 60.0, connectionTimeout -> 5.0)
}

case object URLKey extends GlobalKey[String]

trait HasURL extends Params {

  val url: Param[String] = new Param[String](this, "url", "Url of the service")

  GlobalParams.registerParam(url, URLKey)

  /** @group getParam */
  def getUrl: String = $(url)

  /** @group setParam */
  def setUrl(value: String): this.type = set(url, value)

}

object HTTPTransformer extends ComplexParamsReadable[HTTPTransformer]

class HTTPTransformer(val uid: String)
  extends Transformer with ConcurrencyParams with HasInputCol
    with HasOutputCol with HasHandler
    with ComplexParamsWritable with SynapseMLLogging {
  logClass(FeatureNames.Core)

  setDefault(handler -> HandlingUtils.advancedUDF(100, 500, 1000)) //scalastyle:ignore magic.number

  def this() = this(Identifiable.randomUID("HTTPTransformer"))

  val clientHolder = SharedVariable {
    getConcurrency match {
      case 1 => new SingleThreadedHTTPClient(
        getHandler,
        (getApiTimeout * 1000).toInt,
        (getConnectionTimeout * 1000).toInt)
      case n if n > 1 =>
        val dur = get(concurrentTimeout)
          .map(ct => Duration.fromNanos((ct * math.pow(10, 9)).toLong)) //scalastyle:ignore magic.number
          .getOrElse(Duration.Inf)
        val ec = ExecutionContext.global
        new AsyncHTTPClient(
          getHandler,
          n,
          dur,
          (getApiTimeout * 1000).toInt,
          (getConnectionTimeout * 1000).toInt)(ec)
    }
  }

  private def createTimeoutResponse(timeoutSeconds: Double): HTTPResponseData = {
    HTTPSchema.stringToResponse(
      f"The operation exceeded the time limit of $timeoutSeconds%.1f seconds. " +
        "Fix: increase value of timeout or reset it for no limit.",
      408, //scalastyle:ignore magic.number HTTP_REQUEST_TIMEOUT
      "Request Timeout")
  }

  /** @param dataset - The input dataset, to be transformed
    * @return The DataFrame that results from column selection
    */
  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame]({
      val df = dataset.toDF()
      val enc = ExpressionEncoder(transformSchema(df.schema))
      val colIndex = df.schema.fieldNames.indexOf(getInputCol)
      val fromRow = HTTPRequestData.makeFromRowConverter
      val toRow = HTTPResponseData.makeToRowConverter

      val timeoutMs = get(timeout).map(t => (t * 1000).toLong)
      val startTime = timeoutMs.map(_ => System.currentTimeMillis())
      val startTimeBroadcast = startTime.map(df.sparkSession.sparkContext.broadcast(_))

      val timeoutSec = get(timeout)

      df.mapPartitions { it =>
        if (!it.hasNext) {
          Iterator()
        } else {
          // Early timeout check before creating HTTP client to avoid network errors
          val isAlreadyTimedOut = (timeoutMs, startTimeBroadcast) match {
            case (Some(tm), Some(startBroadcast)) =>
              System.currentTimeMillis() - startBroadcast.value > tm
            case _ => false
          }

          if (isAlreadyTimedOut) {
            // Return timeout responses for all rows without creating HTTP client
            it.map { row =>
              Row.fromSeq(row.toSeq :+ toRow(createTimeoutResponse(timeoutSec.get)))
            }
          } else {
            val c = clientHolder.get

            val responsesWithContext = c.sendRequestsWithContext(it.map { row =>
              // Check if timeout has been exceeded
              val isTimedOut = (timeoutMs, startTimeBroadcast) match {
                case (Some(tm), Some(startBroadcast)) =>
                  System.currentTimeMillis() - startBroadcast.value > tm
                case _ => false
              }

              if (isTimedOut) {
                // Return a timeout response without making the API call
                c.RequestWithContext(None, Some(row), Some(createTimeoutResponse(timeoutSec.get)))
              } else {
                c.RequestWithContext(Option(row.getStruct(colIndex)).map(fromRow), Some(row))
              }
            })
            responsesWithContext.map { rwc =>
              Row.fromSeq(rwc.context.get.asInstanceOf[Row].toSeq :+
                rwc.response.flatMap(Option.apply).map(toRow).orNull)
            }
          }
        }
      }(enc)
    }, dataset.columns.length)
  }

  def copy(extra: ParamMap): HTTPTransformer = defaultCopy(extra)

  def transformSchema(schema: StructType): StructType = {
    assert(schema(getInputCol).dataType == HTTPSchema.Request)
    schema.add(getOutputCol, HTTPSchema.Response, nullable = true)
  }

}
