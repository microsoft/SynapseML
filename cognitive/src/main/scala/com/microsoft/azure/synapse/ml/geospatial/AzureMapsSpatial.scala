// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.geospatial

import com.microsoft.azure.synapse.ml.cognitive.{CognitiveServicesBase, HasInternalJsonOutputParser}
import com.microsoft.azure.synapse.ml.logging.BasicLogging
import com.microsoft.azure.synapse.ml.stages.Lambda
import org.apache.http.entity.AbstractHttpEntity
import org.apache.spark.ml.{ComplexParamsReadable, NamespaceInjections, PipelineModel}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DataType


object CheckPointInPolygon extends ComplexParamsReadable[CheckPointInPolygon]
class CheckPointInPolygon(override val uid: String)
  extends CognitiveServicesBase(uid) with GetPointInPolygon
    with HasInternalJsonOutputParser
    with BasicLogging {

  def this() = this(Identifiable.randomUID("CheckPointInPolygon"))

  setDefault(
    url -> "https://atlas.microsoft.com/")

  override protected def responseDataType: DataType = PointInPolygonProcessResult.schema

  override def urlPath: String = "spatial/pointInPolygon/json"
}
