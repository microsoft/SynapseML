// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.geospatial

import com.microsoft.azure.synapse.ml.build.BuildInfo
import com.microsoft.azure.synapse.ml.services._
import com.microsoft.azure.synapse.ml.io.http.{CustomInputParser, HTTPInputParser, HeaderValues}
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import org.apache.http.client.methods.{HttpGet, HttpRequestBase}
import org.apache.spark.ml.ComplexParamsReadable
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, StructType}

import java.net.URI

object CheckPointInPolygon extends ComplexParamsReadable[CheckPointInPolygon]

class CheckPointInPolygon(override val uid: String)
  extends CognitiveServicesBase(uid)
    with HasInternalJsonOutputParser with SynapseMLLogging with HasServiceParams
    with HasSubscriptionKey with HasSetGeography with HasLatLonPairInput with HasUserDataIdInput {
  logClass(FeatureNames.Geospatial)

  import org.apache.spark.sql.{DataFrame, Dataset}

  // Azure Maps Spatial service was retired on September 30, 2025.
  // See: https://azure.microsoft.com/en-us/updates/v2/azure-maps-creator-services-retirement-on-30-september-2025
  private def retiredServiceError(): Nothing = {
    throw new UnsupportedOperationException(
      "CheckPointInPolygon is no longer functional. " +
      "The Azure Maps Spatial service was retired on September 30, 2025. " +
      "See: https://azure.microsoft.com/en-us/updates/v2/azure-maps-creator-services-retirement-on-30-september-2025"
    )
  }

  override def transform(dataset: Dataset[_]): DataFrame = retiredServiceError()

  protected def inputFunc: Row => Option[HttpRequestBase] = {
    { row: Row =>
      if (shouldSkip(row)) {
        None
      } else {
        val udid = getValue(row, userDataIdentifier).mkString
        val lat = String.valueOf(getValue(row, latitude))
        val lon = String.valueOf(getValue(row, longitude))

        val queryParams = "?" + URLEncodingUtils.format(Map("api-version" -> "1.0",
          "subscription-key" -> getSubscriptionKey,
          "udid" -> udid,
          "lat" -> lat,
          "lon" -> lon))
        val get = new HttpGet()
        get.setURI(new URI(getUrl + queryParams))
        get.setHeader("User-Agent", s"synapseml/${BuildInfo.version}${HeaderValues.PlatformInfo}")
        Some(get)
      }
    }
  }

  protected def getInternalInputParser(schema: StructType): HTTPInputParser = {
    new CustomInputParser().setNullableUDF(inputFunc)
  }

  def this() = this(Identifiable.randomUID("CheckPointInPolygon"))

  setDefault(
    url -> "https://atlas.microsoft.com/")

  override protected def responseDataType: DataType = PointInPolygonProcessResult.schema

  override def urlPath: String = "spatial/pointInPolygon/json"
}
