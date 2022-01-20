// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.geospatial

import com.microsoft.azure.synapse.ml.build.BuildInfo
import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.cognitive.{HasAsyncReply, HasServiceParams, HasUrlPath}
import com.microsoft.azure.synapse.ml.io.http.HandlingUtils._
import com.microsoft.azure.synapse.ml.io.http.{HasURL, _}
import org.apache.http.client.methods.HttpGet
import com.microsoft.azure.synapse.ml.logging.BasicLogging
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.spark.ml.param._
import spray.json.DefaultJsonProtocol.{DoubleJsonFormat, StringJsonFormat, seqFormat}

import java.net.URI
import java.util.concurrent.TimeoutException
import scala.concurrent.blocking
import scala.language.{existentials, postfixOps}
import com.microsoft.azure.synapse.ml.cognitive.BasicAsyncReply

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

trait MapsAsyncReply extends BasicAsyncReply with BasicLogging {

  protected override def queryForResult(key: Option[String], client: CloseableHttpClient,
                               location: URI): Either[String, HTTPResponseData] = {
    val statusRequest = new HttpGet()
    statusRequest.setURI(location)
    statusRequest.setHeader("User-Agent", s"synapseml/${BuildInfo.version}${HeaderValues.PlatformInfo}")
    val resp = convertAndClose(sendWithRetries(client, statusRequest, getBackoffs))
    statusRequest.releaseConnection()
    val status = resp.statusLine.statusCode
    if (status == 202) {
      Left("202")
    } else if (status == 200) {
      Right(resp)
    } else {
      throw new RuntimeException(s"Received unknown status code: $status")
    }
  }

  protected override def handlingFunc(client: CloseableHttpClient,
                             request: HTTPRequestData): HTTPResponseData = {
    val response = HandlingUtils.advanced(getBackoffs: _*)(client, request)
    if (response.statusLine.statusCode == 202) {
      val startedTime = System.nanoTime
      val maxTries = getMaxPollingRetries
      val location = new URI(response.headers.filter(_.name.toLowerCase() == "location").head.value)
      val it = (0 to maxTries).toIterator.foldLeft[Either[String, HTTPResponseData]](Left(""))( (prevResponse, i) => {
        prevResponse match {
          case Right(value) => Right(value) // have value don't want to requery
          case Left(value) => {
            if (i > 0){
              blocking {
                Thread.sleep(getPollingDelay.toLong)
              }
            }
            queryForResult(None, client, location)
          }
        }
      })
      val completedTime = System.nanoTime
      val durationMs = (completedTime - startedTime)/1000000 // seconds -> milli -> micro -> nano
      it match {
        case Right(value) =>{
          logInfo(s"Async operation:succeeded=true:${durationMs}")
          value
        }
        case Left(lastStatus) => {
          logInfo(s"Async operation:succeeded=false:${durationMs}")
          if (getSuppressMaxRetriesExceededException){
            getRetriesExceededHTTPResponseData(maxTries, lastStatus)
          } else {
            throw new TimeoutException(
              s"Querying for results did not complete within $maxTries tries. Last status: $lastStatus")
          }
        }
      }
    } else {
      response
    }
  }
}
