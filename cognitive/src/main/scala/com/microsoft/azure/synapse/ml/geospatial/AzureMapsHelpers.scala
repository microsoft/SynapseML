// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.geospatial

import com.microsoft.azure.synapse.ml.build.BuildInfo
import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.cognitive.{HasAsyncReply, HasServiceParams, HasSubscriptionKey,
  HasUrlPath, URLEncodingUtils}
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
import spray.json.DefaultJsonProtocol.{DoubleJsonFormat, StringJsonFormat, seqFormat}

trait HasSetGeography extends Wrappable with HasURL with HasUrlPath {
  override def pyAdditionalMethods: String = super.pyAdditionalMethods + {
    """
      |def setGeography(self, value):
      |    self._java_obj = self._java_obj.setGeography(value)
      |    return self
      |""".stripMargin
  }

  def setGeography(v: String): this.type = {
    setUrl(s"https://$v.atlas.microsoft.com/" + urlPath)
  }
}

trait HasUserDataIdInput extends HasServiceParams {
  val udid = new ServiceParam[String](
    this, "udid", "the API key to use")

  def getUserDataIdentifier: String = getScalarParam(udid)

  def setUserDataIdentifier(v: String): this.type = setScalarParam(udid, v)

  def getUserDataIdentifierCol: String = getVectorParam(udid)

  def setUserDataIdentifierCol(v: String): this.type = setVectorParam(udid, v)
}

trait HasLatLonPairInput extends HasServiceParams{
  val latitude = new ServiceParam[Seq[Double]](
    this, "lat", "the latitude of location")
  val longitude = new ServiceParam[Seq[Double]](
    this, "lon", "the longitude of location")

  def getLatitude: Seq[Double] = getScalarParam(latitude)

  def setLatitude(v: Seq[Double]): this.type = setScalarParam(latitude, v)

  def setLatitude(v: Double): this.type = setScalarParam(latitude, Seq(v))

  def getLatitudeCol: String = getVectorParam(latitude)

  def setLatitudeCol(v: String): this.type = setVectorParam(latitude, v)

  def getLongitude: Seq[Double] = getScalarParam(longitude)

  def setLongitude(v: Seq[Double]): this.type = setScalarParam(longitude, v)

  def setLongitude(v: Double): this.type = setScalarParam(longitude, Seq(v))

  def getLongitudeCol: String = getVectorParam(longitude)

  def setLongitudeCol(v: String): this.type = setVectorParam(longitude, v)
}

trait HasAddressInput extends HasServiceParams{
  val address = new ServiceParam[Seq[String]](
  this, "address", "the address to geocode")

  def getAddress: Seq[String] = getScalarParam(address)

  def setAddress(v: Seq[String]): this.type = setScalarParam(address, v)

  def setAddress(v: String): this.type = setScalarParam(address, Seq(v))

  def getAddressCol: String = getVectorParam(address)

  def setAddressCol(v: String): this.type = setVectorParam(address, v)
}

trait BatchAddressGeocoding extends HasServiceParams
  with HasSubscriptionKey with HasURL with HasAddressInput {

  protected def inputFunc(schema: StructType): Row => Option[HttpRequestBase] = {
    { row: Row =>
      if (shouldSkip(row)) {
        None
      } else {

        val queryParams = "?" + URLEncodingUtils.format(Map("api-version" -> "1.0",
          "subscription-key" -> getSubscriptionKey))
        val post = new HttpPost(new URI(getUrl + queryParams))
        post.setHeader("Content-Type", "application/json")
        post.setHeader("User-Agent", s"synapseml/${BuildInfo.version}${HeaderValues.PlatformInfo}")
        val encodedAddresses = getValue(row, address).map(x => URLEncoder.encode(x, "UTF-8")).toList
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

trait BatchReverseAddressGeocoding extends HasServiceParams
  with HasSubscriptionKey with HasURL with HasLatLonPairInput{

  protected def inputFunc(schema: StructType): Row => Option[HttpRequestBase] = {
    { row: Row =>
      if (shouldSkip(row)) {
        None
      } else {

        val queryParams = "?" + URLEncodingUtils.format(Map("api-version" -> "1.0",
          "subscription-key" -> getSubscriptionKey))
        val post = new HttpPost(new URI(getUrl + queryParams))
        post.setHeader("Content-Type", "application/json")
        post.setHeader("User-Agent", s"synapseml/${BuildInfo.version}${HeaderValues.PlatformInfo}")
        val latitudes = getValue(row, latitude).toList
        val longitudes = getValue(row, longitude).toList
        val coordinatePairs = (latitudes,longitudes).zipped.toList
        val payloadItems = coordinatePairs.map(x => s"""{ "query": "?query=${x._1},${x._2}&limit=1" }""").mkString(",")
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

trait GetPointInPolygon extends HasServiceParams
  with HasSubscriptionKey with HasSetGeography with HasLatLonPairInput with HasUserDataIdInput {

  protected def inputFunc(schema: StructType): Row => Option[HttpRequestBase] = {
    { row: Row =>
      if (shouldSkip(row)) {
        None
      } else {
        val userDataIdentifier = getValue(row, udid).mkString
        val lat = String.valueOf(getValue(row, latitude))
        val lon = String.valueOf(getValue(row, longitude))

        val queryParams = "?" + URLEncodingUtils.format(Map("api-version" -> "1.0",
          "subscription-key" -> getSubscriptionKey,
          "udid" -> userDataIdentifier,
          "lat"-> lat,
          "lon"-> lon))
        val get = new HttpGet()
        get.setURI(new URI(getUrl + queryParams))
        get.setHeader("User-Agent", s"synapseml/${BuildInfo.version}${HeaderValues.PlatformInfo}")
        Some(get)
      }
    }
  }

  protected def getInternalInputParser(schema: StructType): HTTPInputParser = {
    new CustomInputParser().setNullableUDF(inputFunc(schema))
  }
}

trait MapsAsyncReply extends HasAsyncReply {

  protected def queryForResult(key: Option[String], client: CloseableHttpClient,
                               location: URI): Option[HTTPResponseData] = {
    val statusRequest = new HttpGet()
    statusRequest.setURI(location)
    statusRequest.setHeader("User-Agent", s"synapseml/${BuildInfo.version}${HeaderValues.PlatformInfo}")
    val resp = convertAndClose(sendWithRetries(client, statusRequest, getBackoffs))
    statusRequest.releaseConnection()
    val status = resp.statusLine.statusCode
    if (status == 202) {
      None
    } else if (status == 200) {
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
      val location = new URI(response.headers.filter(_.name.toLowerCase() == "location").head.value)
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
