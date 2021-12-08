// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.


package com.microsoft.azure.synapse.ml.geospatial

import com.microsoft.azure.synapse.ml.cognitive.HasInternalJsonOutputParser
import com.microsoft.azure.synapse.ml.logging.BasicLogging
import org.apache.http.entity.AbstractHttpEntity
import org.apache.spark.ml.ComplexParamsReadable
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DataType


object AzureMapsAPIConstants {
  val DefaultAPIVersion = "1.0"
}

object SearchAddress extends ComplexParamsReadable[SearchAddress]

class SearchAddress(override val uid: String)
  extends AzureMapsBase(uid) with HasAddressInput
    with HasInternalJsonOutputParser
    with BasicLogging {
  logClass()

  def this() = this(Identifiable.randomUID("GetSearchAddress"))

  setDefault(
    url -> "https://atlas.microsoft.com/search/address/json")

  override protected def prepareEntity: Row => Option[AbstractHttpEntity] = { _ => None }
  override protected def responseDataType: DataType = SearchAddressResult.schema

}

object BatchSearchAddress extends ComplexParamsReadable[BatchSearchAddress]

class BatchSearchAddress(override val uid: String)
  extends AzureMapsBaseNoHandler(uid) with HasBatchAddressInput
    with HasInternalJsonOutputParser
    with BatchAsyncReply
    with BasicLogging {

  def this() = this(Identifiable.randomUID("BatchSearchAddress"))

  setDefault(
    url -> "https://atlas.microsoft.com/search/address/batch/json")

  override protected def prepareEntity: Row => Option[AbstractHttpEntity] = { _ => None }
  override protected def responseDataType: DataType = SearchAddressBatchProcessResult.schema

}