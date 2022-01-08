// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.geospatial

import com.microsoft.azure.synapse.ml.cognitive.{CognitiveServicesBaseNoHandler, HasInternalJsonOutputParser}
import com.microsoft.azure.synapse.ml.logging.BasicLogging
import com.microsoft.azure.synapse.ml.stages.Lambda
import org.apache.http.entity.AbstractHttpEntity
import org.apache.spark.ml.{ComplexParamsReadable, NamespaceInjections, PipelineModel}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DataType, StructType}

object AzureMapsAPIConstants {
  val DefaultAPIVersion = "1.0"
}

object AddressGeocoder extends ComplexParamsReadable[AddressGeocoder]

class AddressGeocoder(override val uid: String)
  extends CognitiveServicesBaseNoHandler(uid) with BatchAddressGeocoding
    with HasInternalJsonOutputParser
    with MapsAsyncReply
    with BasicLogging {

  def this() = this(Identifiable.randomUID("AddressGeocoder"))

  setDefault(
    url -> "https://atlas.microsoft.com/search/address/batch/json")

  override protected def prepareEntity: Row => Option[AbstractHttpEntity] = { _ => None }
  override protected def responseDataType: DataType = SearchAddressBatchProcessResult.schema

  override protected def getInternalTransformer(schema: StructType): PipelineModel = {
    val basePipeline =  super.getInternalTransformer(schema)
    val stages = Array(
      basePipeline,
      Lambda(_.withColumn(getOutputCol, col("output.batchItems")))
    )
    NamespaceInjections.pipelineModel(stages)
  }
}

object ReverseAddressGeocoder extends ComplexParamsReadable[ReverseAddressGeocoder]
class ReverseAddressGeocoder(override  val uid: String)
  extends CognitiveServicesBaseNoHandler(uid) with BatchReverseAddressGeocoding
    with HasInternalJsonOutputParser
    with MapsAsyncReply
    with BasicLogging {

  def this() = this(Identifiable.randomUID("ReverseGeocoder"))

  setDefault(
    url -> "https://atlas.microsoft.com/search/address/reverse/batch/json")

  override protected def prepareEntity: Row => Option[AbstractHttpEntity] = { _ => None }
  override protected def responseDataType: DataType = ReverseSearchAddressBatchResult.schema

  override protected def getInternalTransformer(schema: StructType): PipelineModel = {
    val basePipeline =  super.getInternalTransformer(schema)
    val stages = Array(
      basePipeline,
      Lambda(_.withColumn(getOutputCol, col("output.batchItems")))
    )
    NamespaceInjections.pipelineModel(stages)
  }
}
