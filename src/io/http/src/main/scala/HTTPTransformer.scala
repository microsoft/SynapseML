// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.{ComplexParamsReadable, ComplexParamsWritable, Identifiable}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration

trait HTTPParams extends Wrappable {
  val concurrency: Param[Int] = new IntParam(
    this, "concurrency", "max number of concurrent calls")

  /** @group getParam */
  def getConcurrency: Int = $(concurrency)

  /** @group setParam */
  def setConcurrency(value: Int): this.type = set(concurrency, value)

  val concurrentTimeout: Param[Double] = new DoubleParam(
    this, "concurrentTimeout", "max number seconds to wait on futures if concurrency >=1")

  /** @group getParam */
  def getConcurrentTimeout: Double = $(concurrentTimeout)

  /** @group setParam */
  def setConcurrentTimeout(value: Double): this.type = set(concurrentTimeout, value)

  val advancedHandling: Param[Boolean] = new BooleanParam(
    this, "advancedHandling", "whether to be robust to failures")

  /** @group getParam */
  def getAdvancedHandling: Boolean = $(advancedHandling)

  /** @group setParam */
  def setAdvancedHandling(value: Boolean): this.type = set(advancedHandling, value)

  setDefault(concurrency -> 1, concurrentTimeout -> 100, advancedHandling -> true)
}

trait HasURL extends Wrappable {
  val url: Param[String] = new Param[String](
    this, "url", "Url of the service")

  /** @group getParam */
  def getUrl: String = $(url)

  /** @group setParam */
  def setUrl(value: String): this.type = set(url, value)
}

object HTTPTransformer extends ComplexParamsReadable[HTTPTransformer]

class HTTPTransformer(val uid: String)
  extends Transformer with HTTPParams with HasInputCol with HasOutputCol
  with ComplexParamsWritable {
  def this() = this(Identifiable.randomUID("HTTPTransformer"))

  val clientHolder = SharedVariable {
    getConcurrency match {
      case 1 if getAdvancedHandling => new SingleThreadedHTTPClient with AdvancedHTTPHandling
      case 1 => new SingleThreadedHTTPClient with BasicHTTPHandling
      case n if n > 1 =>
        val dur = Duration.fromNanos((getConcurrentTimeout * math.pow(10, 9)).toLong)
        val ec = ExecutionContext.global
        if (getAdvancedHandling) {
          new AsyncHTTPClient(n, dur)(ec) with AdvancedHTTPHandling
        } else {
          new AsyncHTTPClient(n, dur)(ec) with BasicHTTPHandling
        }
    }
  }

  /** @param dataset - The input dataset, to be transformed
    * @return The DataFrame that results from column selection
    */
  override def transform(dataset: Dataset[_]): DataFrame = {
    val df = dataset.toDF()
    val enc = RowEncoder(transformSchema(df.schema))
    val colIndex = df.schema.fieldNames.indexOf(getInputCol)
    df.mapPartitions { it =>
      val c = clientHolder.get
      c.send(it.map(row =>
        c.ContextIn(HTTPRequestData.fromRow(row.getStruct(colIndex)), Some(row))
      )).map(cout => Row.merge(
        cout.context.get.asInstanceOf[Row],
        Row(cout.out.toRow)
      ))
    }(enc)
  }

  def copy(extra: ParamMap): HTTPTransformer = defaultCopy(extra)

  def transformSchema(schema: StructType): StructType = {
    assert(schema(getInputCol).dataType == HTTPSchema.request)
    schema.add(getOutputCol, HTTPSchema.response)
  }

}
