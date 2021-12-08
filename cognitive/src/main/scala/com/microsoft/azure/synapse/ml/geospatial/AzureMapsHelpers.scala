// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.geospatial

import com.microsoft.azure.synapse.ml.cognitive.{HasAsyncReply, HasServiceParams, HasSubscriptionKey, URLEncodingUtils}
import com.microsoft.azure.synapse.ml.io.http.{CustomInputParser, HTTPInputParser, HasURL}
import com.microsoft.azure.synapse.ml.io.http._
import com.microsoft.azure.synapse.ml.io.http.HandlingUtils._
import org.apache.http.client.methods.{HttpGet, HttpPost, HttpRequestBase}
import org.apache.http.entity.{AbstractHttpEntity, StringEntity}
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.spark.ml.param._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

import java.net.{URI, URLEncoder}
import java.util.concurrent.TimeoutException
import scala.concurrent.blocking
import scala.language.{existentials, postfixOps}
import spray.json.DefaultJsonProtocol.{StringJsonFormat, seqFormat}

trait HasAddressInput extends HasServiceParams with HasSubscriptionKey with HasURL {
  val address = new ServiceParam[Seq[String]](
    this, "address", "the address to geocode")

  def getAddress: Seq[String] = getScalarParam(address)

  def setAddress(v: Seq[String]): this.type = setScalarParam(address, v)

  def setAddress(v: String): this.type = setScalarParam(address, Seq(v))

  def getAddressCol: String = getVectorParam(address)

  def setAddressCol(v: String): this.type = setVectorParam(address, v)

  protected def prepareEntity: Row => Option[AbstractHttpEntity]

  protected def prepareMethod(): HttpRequestBase = new HttpGet()

  protected def contentType: Row => String = { _ => "application/json" }

  protected def inputFunc(schema: StructType): Row => Option[HttpRequestBase] = {
    { row: Row =>
      if (shouldSkip(row)) {
        None
      } else {
        val req = prepareMethod()
        val addressValue =  getValueOpt(row, address).mkString("")
        val queryParams = "?" + URLEncodingUtils.format(Map("api-version"-> "1.0",
          "subscription-key" -> getSubscriptionKey,
          "query" -> addressValue))
        req.setURI(new URI(getUrl + queryParams))

        Some(req)
      }
    }
  }

  protected def getInternalInputParser(schema: StructType): HTTPInputParser = {
    new CustomInputParser().setNullableUDF(inputFunc(schema))
  }
}

trait HasBatchAddressInput extends HasServiceParams with HasSubscriptionKey with HasURL {
  val addresses = new ServiceParam[Seq[String]](
    this, "address", "the address to geocode")

  def getAddresses: Seq[String] = getScalarParam(addresses)

  def setAddresses(v: Seq[String]): this.type = setScalarParam(addresses, v)

  def setAddresses(v: String): this.type = setScalarParam(addresses, Seq(v))

  def getAddressesCol: String = getVectorParam(addresses)

  def setAddressesCol(v: String): this.type = setVectorParam(addresses, v)

  protected def prepareEntity: Row => Option[AbstractHttpEntity]

  protected def contentType: Row => String = { _ => "application/json" }

  protected def inputFunc(schema: StructType): Row => Option[HttpRequestBase] = {
    { row: Row =>
      if (shouldSkip(row)) {
        None
      } else {

        val queryParams = "?" + URLEncodingUtils.format(Map("api-version"-> "1.0",
          "subscription-key" -> getSubscriptionKey))
        val post = new HttpPost(new URI(getUrl + queryParams))
        post.setHeader("Content-Type", "application/json")
        val addressesCol = getValueOpt(row, addresses)
        val encodedAddresses = addressesCol.get.map(x => URLEncoder.encode(x, "UTF-8")).toList
        val payloadItems = encodedAddresses.map(x => s"""{ "query": "?query=$x&limit=1" }""").mkString(",")
        val payload = s"""{ "batchItems": [ $payloadItems ] }"""
        post.setEntity(new StringEntity(payload))
        Some(post)
      }
    }
  }

  protected def getInternalInputParser(schema: StructType): HTTPInputParser = {
    new CustomInputParser().setNullableUDF(inputFunc(schema))
  }
}

trait BatchAsyncReply extends HasAsyncReply with HasSubscriptionKey {

  protected def queryForResult(key: Option[String], client: CloseableHttpClient,
                               location: URI): Option[HTTPResponseData] = {
    val statusRequest = new HttpGet()
    statusRequest.setURI(location)
    val resp = convertAndClose(sendWithRetries(client, statusRequest, getBackoffs))
    statusRequest.releaseConnection()
    val status = resp.statusLine.statusCode
    if (status == 202){
      None
    } else if (status == 200){
      Some(resp)
    } else {
      throw new RuntimeException(s"Received unknown status code: $status")
    }
  }

  protected def handlingFunc(client: CloseableHttpClient,
                             request: HTTPRequestData): HTTPResponseData = {
    val response = HandlingUtils.advanced(getBackoffs: _*)(client, request)
    if (response.statusLine.statusCode == 202) {

      val maxTries = getMaxPollingRetries
      val locationHeaderValue = response.headers.filter(_.name.toLowerCase() == "location").head.value
      val location = new URI(locationHeaderValue + "&subscription-key=" + getSubscriptionKey)
      val it = (0 to maxTries).toIterator.flatMap { _ =>
        queryForResult(None, client, location).orElse({
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

