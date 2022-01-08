// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.geospatial

import com.microsoft.azure.synapse.ml.core.schema.SparkBindings

case class PointInPolygonSummary (
 sourcePoint: Option[LatLongPairAbbreviated],
 // A unique data id (udid) for the uploaded content.
 // Udid is not applicable for POST spatial operations(set to null)
 udid: Option[String],
 // Processing information
 information: Option[String]
)

case class PointInPolygonResult(
  pointInPolygons: Option[Boolean],
  intersectingGeometries: Option[Seq[String]]
)

object PointInPolygonProcessResult extends SparkBindings[PointInPolygonProcessResult]
case class PointInPolygonProcessResult (
  summary: Option[PointInPolygonSummary],
  // Point In Polygon Result Object
  result: Option[PointInPolygonResult]
)

case class MapDataMetadataResult(
  udid: Option[String],
  location: Option[String],
  created: Option[String],
  updated: Option[String],
  sizeInBytes: Option[String],
  uploadStatus: Option[String]
)
