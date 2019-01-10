// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import spray.json._

case class IndexSchema(name: String, fields: Seq[Field])

case class Field(name: String,
                 `type`: String,
                 searchable: Boolean,
                 filterable: Boolean,
                 sortable: Boolean,
                 facetable: Boolean,
                 key: Boolean,
                 retrievable: Boolean,
                 analyzer: Option[String],
                 searchAnalyzer: Option[String],
                 indexAnalyzer: Option[String],
                 synonymMaps: Option[String])

object IndexJsonProtocol extends DefaultJsonProtocol {
  implicit val fieldFormat = jsonFormat12(Field)
  implicit val indexFormat = jsonFormat2(IndexSchema)
}
