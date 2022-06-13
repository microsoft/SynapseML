// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.geospatial

import com.microsoft.azure.synapse.ml.build.BuildInfo
import com.microsoft.azure.synapse.ml.cognitive._
import com.microsoft.azure.synapse.ml.io.http.{CustomInputParser, HTTPInputParser, HasURL, HeaderValues}
import com.microsoft.azure.synapse.ml.logging.BasicLogging
import com.microsoft.azure.synapse.ml.stages.Lambda
import org.apache.http.client.methods.{HttpPost, HttpRequestBase}
import org.apache.http.entity.StringEntity
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{ComplexParamsReadable, NamespaceInjections, PipelineModel}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DataType, StructType}

import java.net.{URI, URLEncoder}

object AzureMapsAPIConstants {
  val DefaultAPIVersion = "1.0"
}

object AddressGeocoder extends ComplexParamsReadable[AddressGeocoder]

class AddressGeocoder(override val uid: String)
  extends CognitiveServicesBaseNoHandler(uid) with HasServiceParams
    with HasSubscriptionKey with HasURL with HasAddressInput
    with HasInternalJsonOutputParser with MapsAsyncReply with BasicLogging {

  protected def inputFunc: Row => Option[HttpRequestBase] = {
    { row: Row =>
      if (shouldSkip(row)) {
        None
      } else {
        val queryParams = "?" + URLEncodingUtils.format(Map(
          "api-version" -> "1.0",
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
    new CustomInputParser().setNullableUDF(inputFunc)
  }

  def this() = this(Identifiable.randomUID("AddressGeocoder"))

  setDefault(
    url -> "https://atlas.microsoft.com/search/address/batch/json")

  override protected def responseDataType: DataType = SearchAddressBatchProcessResult.schema

  override protected def getInternalTransformer(schema: StructType): PipelineModel = {
    val basePipeline = super.getInternalTransformer(schema)
    val stages = Array(
      basePipeline,
      Lambda(_.withColumn(getOutputCol, col("output.batchItems")))
    )
    NamespaceInjections.pipelineModel(stages)
  }
}

object ReverseAddressGeocoder extends ComplexParamsReadable[ReverseAddressGeocoder]

class ReverseAddressGeocoder(override val uid: String)
  extends CognitiveServicesBaseNoHandler(uid)
    with HasInternalJsonOutputParser with MapsAsyncReply with BasicLogging with HasServiceParams
    with HasSubscriptionKey with HasURL with HasLatLonPairInput {

  protected def inputFunc: Row => Option[HttpRequestBase] = {
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
        val coordinatePairs = (latitudes, longitudes).zipped.toList
        val payloadItems = coordinatePairs.map(x => s"""{ "query": "?query=${x._1},${x._2}&limit=1" }""").mkString(",")
        val payload = s"""{ "batchItems": [ $payloadItems ] }"""
        post.setEntity(new StringEntity(payload))
        Some(post)
      }
    }
  }

  protected def getInternalInputParser(schema: StructType): HTTPInputParser = {
    new CustomInputParser().setNullableUDF(inputFunc)
  }

  def this() = this(Identifiable.randomUID("ReverseAddressGeocoder"))

  setDefault(
    url -> "https://atlas.microsoft.com/search/address/reverse/batch/json")

  override protected def responseDataType: DataType = ReverseSearchAddressBatchResult.schema

  override protected def getInternalTransformer(schema: StructType): PipelineModel = {
    val basePipeline = super.getInternalTransformer(schema)
    val stages = Array(
      basePipeline,
      Lambda(_.withColumn(getOutputCol, col("output.batchItems")))
    )
    NamespaceInjections.pipelineModel(stages)
  }
}
