// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.geospatial

import com.microsoft.azure.synapse.ml.build.BuildInfo
import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.services.vision.HasAsyncReply
import com.microsoft.azure.synapse.ml.services.{HasServiceParams, HasUrlPath}
import com.microsoft.azure.synapse.ml.io.http.HandlingUtils._
import com.microsoft.azure.synapse.ml.io.http._
import com.microsoft.azure.synapse.ml.param.ServiceParam
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.CloseableHttpClient
import spray.json.DefaultJsonProtocol.{DoubleJsonFormat, StringJsonFormat, seqFormat}

import java.net.URI
import java.util.concurrent.TimeoutException
import scala.concurrent.blocking
import scala.language.{existentials, postfixOps}

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
  val userDataIdentifier = new ServiceParam[String](
    this, "userDataIdentifier", "the identifier for the user uploaded data")

  def getUserDataIdentifier: String = getScalarParam(userDataIdentifier)

  def setUserDataIdentifier(v: String): this.type = setScalarParam(userDataIdentifier, v)

  def getUserDataIdentifierCol: String = getVectorParam(userDataIdentifier)

  def setUserDataIdentifierCol(v: String): this.type = setVectorParam(userDataIdentifier, v)
}

trait HasLatLonPairInput extends HasServiceParams {
  val latitude = new ServiceParam[Seq[Double]](
    this, "latitude", "the latitude of location")
  val longitude = new ServiceParam[Seq[Double]](
    this, "longitude", "the longitude of location")

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

trait HasAddressInput extends HasServiceParams {
  val address = new ServiceParam[Seq[String]](
    this, "address", "the address to geocode")

  def getAddress: Seq[String] = getScalarParam(address)

  def setAddress(v: Seq[String]): this.type = setScalarParam(address, v)

  def setAddress(v: String): this.type = setScalarParam(address, Seq(v))

  def getAddressCol: String = getVectorParam(address)

  def setAddressCol(v: String): this.type = setVectorParam(address, v)
}

trait MapsAsyncReply extends HasAsyncReply {

  override protected def extractHeaderValuesForPolling(request: HTTPRequestData): Map[String, String] = {
    // Extract query parameters instead of headers for Azure Maps authentication
    // Azure Maps uses subscription-key as a URL query parameter, not an HTTP header.
    Option(request.requestLine.uri).flatMap { uriString =>
      val uri = new URI(uriString)
      Option(uri.getQuery).map { queryParams =>
        queryParams.split("&").flatMap { param =>
          val parts = param.split("=", 2)
          if (parts.length == 2 && (parts(0) == "subscription-key" || parts(0) == "api-version")) {
            // Decode the value in case it contains URL-encoded characters
            Some(parts(0) -> java.net.URLDecoder.decode(parts(1), "UTF-8"))
          } else {
            None
          }
        }.toMap
      }
    }.getOrElse(Map.empty)
  }

  protected def queryForResult(headers: Map[String, String], client: CloseableHttpClient,
                               location: URI): Option[HTTPResponseData] = {
    val statusRequest = new HttpGet()
    val locationWithAuth = if (headers.nonEmpty) {
      val separator = if (location.getQuery == null) "?" else "&"
      val authParams = headers.map { case (k, v) =>
        s"$k=${java.net.URLEncoder.encode(v, "UTF-8")}"
      }.mkString("&")
      new URI(location.toString + separator + authParams)
    } else {
      location
    }
    statusRequest.setURI(locationWithAuth)
    statusRequest.setHeader("User-Agent", s"synapseml/${BuildInfo.version}${HeaderValues.PlatformInfo}")
    val resp = convertAndClose(sendWithRetries(
      client, statusRequest, getBackoffs, extraCodesToRetry = Set(404))) // scalastyle:off magic.number
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
      val headers = extractHeaderValuesForPolling(request)
      val it = (0 to maxTries).toIterator.flatMap { _ =>
        queryForResult(headers, client, location).orElse({
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
