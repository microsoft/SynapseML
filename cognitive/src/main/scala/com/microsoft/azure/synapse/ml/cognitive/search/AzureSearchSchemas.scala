// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cognitive.search

import com.microsoft.azure.synapse.ml.core.schema.SparkBindings
import spray.json.DefaultJsonProtocol._
import spray.json.{DefaultJsonProtocol, JsonFormat, RootJsonFormat}

object ASResponses extends SparkBindings[ASResponses]

case class ASResponses(value: Seq[ASResponse])

case class ASResponse(key: String, status: Boolean, errorMessage: Option[String], statusCode: Int)

case class IndexInfo(
                    name: Option[String],
                    fields: Seq[IndexField],
                    suggesters: Option[Seq[String]],
                    scoringProfiles: Option[Seq[String]],
                    analyzers: Option[Seq[String]],
                    charFilters: Option[Seq[String]],
                    tokenizers: Option[Seq[String]],
                    tokenFilters: Option[Seq[String]],
                    defaultScoringProfile: Option[Seq[String]],
                    corsOptions: Option[Seq[String]]
                    )

case class IndexField(
                     name: String,
                     `type`: String,
                     searchable: Option[Boolean],
                     filterable: Option[Boolean],
                     sortable: Option[Boolean],
                     facetable: Option[Boolean],
                     retrievable: Option[Boolean],
                     key: Option[Boolean],
                     analyzer: Option[String],
                     searchAnalyzer: Option[String],
                     indexAnalyzer: Option[String],
                     synonymMap: Option[String],
                     fields: Option[Seq[IndexField]]
                     )

case class VectorColParams(
                          name: String,
                          dimension: Int
                          )

case class IndexStats(documentCount: Int, storageSize: Int)

case class IndexList(`@odata.context`: String, value: Seq[IndexName])
case class IndexName(name: String)

object AzureSearchProtocol extends DefaultJsonProtocol {
  implicit val IfEnc: JsonFormat[IndexField] = lazyFormat(jsonFormat(
    IndexField,"name","type","searchable","filterable","sortable",
    "facetable","retrievable", "key","analyzer","searchAnalyzer", "indexAnalyzer", "synonymMaps", "fields"))
  implicit val IiEnc: RootJsonFormat[IndexInfo] = jsonFormat10(IndexInfo.apply)
  implicit val IsEnc: RootJsonFormat[IndexStats] = jsonFormat2(IndexStats.apply)
  implicit val InEnc: RootJsonFormat[IndexName] = jsonFormat1(IndexName.apply)
  implicit val IlEnc: RootJsonFormat[IndexList] = jsonFormat2(IndexList.apply)
  implicit val IvEnc: RootJsonFormat[VectorColParams] = jsonFormat2(VectorColParams.apply)
//  implicit object IvsEnc extends RootJsonFormat[SeqVectorColParams] {
//    def read(value: JsValue): SeqVectorColParams = SeqVectorColParams(value.convertTo[Seq[VectorColParams]])
//    def write(obj: SeqVectorColParams): JsValue = obj.items.toJson
//  }
}
