// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.util.concurrent.{BlockingQueue, CountDownLatch, LinkedBlockingQueue}

import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet, HttpPost, HttpUriRequest}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import org.apache.http.message.BufferedHeader
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

private[ml] trait DataFrameClient extends BaseClient[Row, Row] {
  def transformSchema(schema: StructType): StructType
}

private[ml] trait UnaryTransformerClient extends DataFrameClient with ColumnSchema {

  private var inputIndex: Option[Int] = None

  def setInputIndex(value: Int): Unit = {
    inputIndex = Some(value)
  }

  def getInputIndex: Int = inputIndex.get

  private var outputCol: Option[String] = None

  def setOutputCol(value: String): Unit = {
    outputCol = Some(value)
  }

  def getOutputCol: String = outputCol.get

  def transformSchema(struct: StructType): StructType = {
    assert(inputIndex.isDefined)
    assert(outputCol.isDefined)
    struct.add(getOutputCol, transformDataType(struct(getInputIndex).dataType))
  }

}

abstract class UnaryTransformerWrapper[In, Out]
  (val baseClient: BaseClient[In, Out] with ColumnSchema)
    extends UnaryTransformerClient {

  override def setUrl(value: String): Unit = {
    baseClient.setUrl(value)
    super.setUrl(value)
  }

  override def setMethod(value: String): Unit = {
    baseClient.setMethod(value)
    super.setMethod(value)
  }

  override def verifyInput(input: DataType): Unit = {
    baseClient.verifyInput(input)
  }

  override def transformDataType(dataType: DataType): DataType = {
    baseClient.transformDataType(dataType)
  }

  def toBaseRequest(r: Request): baseClient.Request = {
    baseClient.Request(r.request, r.context)
  }

  def fromBaseResponse(r: baseClient.Response): Response = {
    Response(r.response, r.context)
  }

  def fromBaseRequest(r: baseClient.Request): Request = {
    Request(r.request, r.context)
  }

  def toBaseResponse(r: Response): baseClient.Response = {
    baseClient.Response(r.response, r.context)
  }

  override private[ml] def sendRequests(requests: Iterator[Request]): Iterator[Response] = {
    baseClient
      .sendRequests(requests.map(toBaseRequest))
      .map(fromBaseResponse)
  }

}

class UnbufferedUnaryTransformerWrapper[In,Out]
  (override val baseClient: BaseClient[In, Out]
       with Unbuffered[In, Out] with ColumnSchema)
    extends UnaryTransformerWrapper(baseClient) with Unbuffered[Row,Row] {

  private[ml] def toRequest(row: Row): Request = {
    val request = baseClient.toRequest(row.getAs[In](getInputIndex))
    request.context.put("row", row)
    fromBaseRequest(request)
  }

  private[ml] def fromResponse(response: Response): Row = {
    val initialRow = response.context("row").asInstanceOf[Row]
    Row.merge(initialRow,
              Row(baseClient.fromResponse(toBaseResponse(response))))
  }

}

class BatchedUnaryTransformerWrapper[In,Out]
  (override val baseClient: BaseClient[In, Out]
       with BufferedBatching[In, Out] with ColumnSchema)
    extends UnaryTransformerWrapper(baseClient) with BufferedBatching[Row, Row] {

  private[ml] def toBatchRequest(rows: Seq[Row]): Request = {
    val request = baseClient.toBatchRequest(rows.map(row => row.getAs[In](getInputIndex)))
    request.context.put("rows", rows)
    fromBaseRequest(request)
  }

  private[ml] def fromBatchResponse(response: Response): Seq[Row] = {
    val initialRows = response.context("rows").asInstanceOf[Seq[Row]]
    val baseOutputs = baseClient.fromBatchResponse(toBaseResponse(response))
    initialRows.zip(baseOutputs).map {
      case (row, baseOutput) => Row.merge(row, Row(baseOutput))
    }
  }

}

object ClientTools {

  def toUnaryTransformerClient[In, Out](baseClient: BaseClient[In, Out]
                                            with ColumnSchema)
      : UnaryTransformerWrapper[In, Out] = {
    baseClient match {
      case c: Unbuffered[_, _] =>
        val coerced =
          c.asInstanceOf[BaseClient[In, Out] with ColumnSchema with Unbuffered[In, Out]]
        new UnbufferedUnaryTransformerWrapper(coerced)
      case c: BufferedBatching[_, _] =>
        val coerced =
          c.asInstanceOf[BaseClient[In, Out] with ColumnSchema with BufferedBatching[In, Out]]
        new BatchedUnaryTransformerWrapper(coerced)
    }
  }

}
