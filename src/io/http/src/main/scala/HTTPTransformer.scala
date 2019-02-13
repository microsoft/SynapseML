// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import com.microsoft.ml.spark.HandlingUtils.HandlerFunc
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.{ComplexParamsReadable, ComplexParamsWritable, Identifiable}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration

trait HasHandler extends Params {
  val handler: UDFParam = new UDFParam(
    this, "handler", "Which strategy to use when handling requests")

  /** @group getParam */
  def getHandler: HandlerFunc = $(handler).f.asInstanceOf[HandlerFunc]

  def setHandler(v: HandlerFunc): HasHandler.this.type = {
    set(handler, udf(v, StringType))
  }
}

trait HTTPParams extends Wrappable {
  val concurrency: Param[Int] = new IntParam(
    this, "concurrency", "max number of concurrent calls")

  /** @group getParam */
  def getConcurrency: Int = $(concurrency)

  /** @group setParam */
  def setConcurrency(value: Int): this.type = set(concurrency, value)

  val timeout: Param[Double] = new DoubleParam(
    this, "timeout", "number of seconds to wait before closing the connection")

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

  setDefault(concurrency -> 1,
    timeout -> 60.0,
    concurrentTimeout -> 100.0)

}

trait HasURL extends Wrappable {

  val url: Param[String] = new Param[String](this, "url", "Url of the service")

  /** @group getParam */
  def getUrl: String = $(url)

  /** @group setParam */
  def setUrl(value: String): this.type = set(url, value)

}

object HTTPTransformer extends ComplexParamsReadable[HTTPTransformer]

class HTTPTransformer(val uid: String)
  extends Transformer with HTTPParams with HasInputCol
    with HasOutputCol with HasHandler
    with ComplexParamsWritable {

  setDefault(handler -> HandlingUtils.advancedUDF(100,500,1000))

  def this() = this(Identifiable.randomUID("HTTPTransformer"))

  val clientHolder = SharedVariable {
    getConcurrency match {
      case 1 => new SingleThreadedHTTPClient(getHandler, (getTimeout*1000).toInt)
      case n if n > 1 =>
        val dur = Duration.fromNanos((getConcurrentTimeout * math.pow(10, 9)).toLong)
        val ec = ExecutionContext.global
        new AsyncHTTPClient(getHandler,n, dur, (getTimeout*1000).toInt)(ec)
    }
  }

  /** @param dataset - The input dataset, to be transformed
    * @return The DataFrame that results from column selection
    */
  override def transform(dataset: Dataset[_]): DataFrame = {
    val df = dataset.toDF()
    val enc = RowEncoder(transformSchema(df.schema))
    val colIndex = df.schema.fieldNames.indexOf(getInputCol)
    val fromRow = HTTPRequestData.makeFromRowConverter
    val toRow = HTTPResponseData.makeToRowConverter
    df.mapPartitions { it =>
      if (!it.hasNext) {
        Iterator()
      }else{
        val c = clientHolder.get
        val responsesWithContext = c.sendRequestsWithContext(it.map{row =>
          c.RequestWithContext(Option(row.getStruct(colIndex)).map(fromRow), Some(row))
        })
        responsesWithContext.map { rwc =>
          Row.merge(rwc.context.get.asInstanceOf[Row], Row(rwc.response.map(toRow).orNull))
        }
      }
    }(enc)
  }

  def copy(extra: ParamMap): HTTPTransformer = defaultCopy(extra)

  def transformSchema(schema: StructType): StructType = {
    assert(schema(getInputCol).dataType == HTTPSchema.request)
    schema.add(getOutputCol, HTTPSchema.response, nullable=true)
  }

}
