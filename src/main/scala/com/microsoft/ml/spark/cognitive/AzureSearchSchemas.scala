// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cognitive

import com.microsoft.ml.spark.core.schema.SparkBindings
import spray.json.DefaultJsonProtocol._
import spray.json.RootJsonFormat

object ASResponses extends SparkBindings[ASResponses]

case class ASResponses(value: Seq[ASResponse])

case class ASResponse(key: String, status: Boolean, errorMessage: Option[String], statusCode: Int)

case class IndexInfo(
                    name: Option[String],
                    fields: Array[IndexField],
                    suggesters: Option[Array[String]],
                    scoringProfiles: Option[Array[String]],
                    analyzers: Option[Array[String]],
                    charFilters: Option[Array[String]],
                    tokenizers: Option[Array[String]],
                    tokenFilters: Option[Array[String]],
                    defaultScoringProfile: Option[Array[String]],
                    corsOptions: Option[Array[String]]
                    )

case class IndexField(
                     name: String,
                     `type`: String,
                     searchable: Option[Boolean],
                     filterable: Option[Boolean],
                     sortable: Option[Boolean],
                     facetable: Option[Boolean],
                     key: Option[Boolean],
                     retrievable: Option[Boolean],
                     analyzer: Option[String],
                     searchAnalyzer: Option[String],
                     indexAnalyzer: Option[String],
                     synonymMap: Option[String]
                     )

case class IndexStats(documentCount: Int, storageSize: Int)

case class IndexList(`@odata.context`: String, value: Seq[IndexName])
case class IndexName(name: String)

object AzureSearchProtocol {
  implicit val IfEnc: RootJsonFormat[IndexField] = jsonFormat12(IndexField.apply)
  implicit val IiEnc: RootJsonFormat[IndexInfo] = jsonFormat10(IndexInfo.apply)
  implicit val IsEnc: RootJsonFormat[IndexStats] = jsonFormat2(IndexStats.apply)
  implicit val InEnc: RootJsonFormat[IndexName] = jsonFormat1(IndexName.apply)
  implicit val IlEnc: RootJsonFormat[IndexList] = jsonFormat2(IndexList.apply)
}
