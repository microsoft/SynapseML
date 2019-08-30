// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cognitive

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
  implicit val FieldFormat: RootJsonFormat[Field] = jsonFormat12(Field)
  implicit val IndexFormat: RootJsonFormat[IndexSchema] = jsonFormat2(IndexSchema)
}
