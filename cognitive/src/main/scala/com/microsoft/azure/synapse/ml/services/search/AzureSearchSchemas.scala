// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.search

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
                    scoringProfiles: Option[Seq[ScoringProfile]],
                    analyzers: Option[Seq[String]],
                    charFilters: Option[Seq[String]],
                    tokenizers: Option[Seq[String]],
                    tokenFilters: Option[Seq[String]],
                    defaultScoringProfile: Option[String],
                    corsOptions: Option[Seq[String]],
                    vectorSearch: Option[VectorSearch]
                    )

case class AlgorithmConfigs(
                           name: String,
                           kind: String
                           )

case class VectorProfile(
                        name: String,
                        algorithm: String
                        )

/** Vector search configuration.
  *
  * Azure AI Search renamed `algorithmConfigurations` to `algorithms` and introduced `profiles` in the
  * `2023-11-01` stable API. Both shapes are modeled here so index definitions authored against either
  * generation of the API can be parsed, and so a definition can be emitted in whichever shape the
  * requested `api-version` expects.
  */
case class VectorSearch(
                       algorithms: Option[Seq[AlgorithmConfigs]] = None,
                       profiles: Option[Seq[VectorProfile]] = None,
                       algorithmConfigurations: Option[Seq[AlgorithmConfigs]] = None
                       ) {
  def effectiveAlgorithms: Seq[AlgorithmConfigs] =
    algorithms.orElse(algorithmConfigurations).getOrElse(Seq.empty)
}

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
                     synonymMap: Option[Seq[String]],
                     fields: Option[Seq[IndexField]],
                     dimensions: Option[Int],
                     vectorSearchConfiguration: Option[String],
                     vectorSearchProfile: Option[String] = None
                     ) {
  /** Name of the vector configuration this field points at, under either API generation.
    *
    * `vectorSearchProfile` is the `2023-11-01`+ spelling; `vectorSearchConfiguration` is the legacy
    * `2023-07-01-Preview` spelling.
    */
  def vectorReference: Option[String] = vectorSearchProfile.orElse(vectorSearchConfiguration)

  def isVectorField: Boolean = dimensions.nonEmpty && vectorReference.nonEmpty
}

case class VectorColParams(
                          name: String,
                          dimension: Int
                          )

case class ScoringFunction(
                          `type`: String,
                          boost: Option[Double],
                          fieldName: String,
                          interpolation: Option[String],
                          freshness: Option[FreshnessFunction],
                          magnitude: Option[MagnitudeFunction],
                          distance: Option[DistanceFunction],
                          tag: Option[TagFunction]
                          )

case class FreshnessFunction(boostingDuration: String)

case class MagnitudeFunction(
                            boostingRangeStart: Double,
                            boostingRangeEnd: Double,
                            constantBoostBeyondRange: Option[Boolean]
                            )

case class DistanceFunction(
                           referencePointParameter: String,
                           boostingDistance: Double
                           )

case class TagFunction(tagsParameter: String)

case class ScoringProfile(
                         name: String,
                         text: Option[TextWeights],
                         functions: Option[Seq[ScoringFunction]],
                         functionAggregation: Option[String]
                         )

case class TextWeights(weights: Map[String, Double])

case class IndexStats(documentCount: Int, storageSize: Int)

case class IndexList(`@odata.context`: String, value: Seq[IndexName])
case class IndexName(name: String)

object AzureSearchProtocol extends DefaultJsonProtocol {
  implicit val IfEnc: JsonFormat[IndexField] = lazyFormat(jsonFormat(
    IndexField,"name","type","searchable","filterable","sortable",
    "facetable","retrievable", "key","analyzer","searchAnalyzer", "indexAnalyzer", "synonymMaps", "fields",
    "dimensions", "vectorSearchConfiguration", "vectorSearchProfile"))
  implicit val AcEnc: RootJsonFormat[AlgorithmConfigs] = jsonFormat2(AlgorithmConfigs.apply)
  implicit val VpEnc: RootJsonFormat[VectorProfile] = jsonFormat2(VectorProfile.apply)
  implicit val VsEnc: RootJsonFormat[VectorSearch] = jsonFormat(
    VectorSearch.apply, "algorithms", "profiles", "algorithmConfigurations")
  implicit val FfEnc: RootJsonFormat[FreshnessFunction] = jsonFormat1(FreshnessFunction.apply)
  implicit val MfEnc: RootJsonFormat[MagnitudeFunction] = jsonFormat3(MagnitudeFunction.apply)
  implicit val DfEnc: RootJsonFormat[DistanceFunction] = jsonFormat2(DistanceFunction.apply)
  implicit val TfEnc: RootJsonFormat[TagFunction] = jsonFormat1(TagFunction.apply)
  implicit val SfEnc: RootJsonFormat[ScoringFunction] = jsonFormat8(ScoringFunction.apply)
  implicit val TwEnc: RootJsonFormat[TextWeights] = jsonFormat1(TextWeights.apply)
  implicit val SpEnc: RootJsonFormat[ScoringProfile] = jsonFormat4(ScoringProfile.apply)
  implicit val IiEnc: RootJsonFormat[IndexInfo] = jsonFormat11(IndexInfo.apply)
  implicit val IsEnc: RootJsonFormat[IndexStats] = jsonFormat2(IndexStats.apply)
  implicit val InEnc: RootJsonFormat[IndexName] = jsonFormat1(IndexName.apply)
  implicit val IlEnc: RootJsonFormat[IndexList] = jsonFormat2(IndexList.apply)
  implicit val VcpEnc: RootJsonFormat[VectorColParams] = jsonFormat2(VectorColParams.apply)
}

/** Translates an index definition between the two generations of the Azure AI Search vector schema.
  *
  * `2023-11-01` renamed `vectorSearch.algorithmConfigurations` to `vectorSearch.algorithms`, added
  * `vectorSearch.profiles`, and replaced the field-level `vectorSearchConfiguration` with
  * `vectorSearchProfile`. Index definitions supplied by users (via the `indexJson` option) may still
  * use either spelling, so definitions are rewritten to match the requested `api-version` rather than
  * being rejected.
  */
private[search] object VectorSchema {

  def align(index: IndexInfo, apiVersion: String): IndexInfo = {
    if (AzureSearchAPIConstants.supportsVectorProfiles(apiVersion)) toProfileSchema(index)
    else toLegacySchema(index)
  }

  private def toProfileSchema(index: IndexInfo): IndexInfo = {
    val algorithms = index.vectorSearch.map(_.effectiveAlgorithms).getOrElse(Seq.empty)
    val declaredProfiles = index.vectorSearch.flatMap(_.profiles).getOrElse(Seq.empty)
    // A legacy field points at an algorithm name. Mirroring each algorithm as a same-named profile
    // keeps those references resolvable without renaming anything the user already wrote.
    val profiles = if (declaredProfiles.nonEmpty) declaredProfiles
    else algorithms.map(algorithm => VectorProfile(algorithm.name, algorithm.name))
    val profileNames = profiles.map(_.name).toSet
    val profileByAlgorithm = profiles.map(profile => profile.algorithm -> profile.name).toMap

    def rewrite(field: IndexField): IndexField = field.copy(
      // Vector fields must be searchable in 2023-11-01 and later.
      searchable = if (field.isVectorField) Some(true) else field.searchable,
      fields = field.fields.map(_.map(rewrite)),
      vectorSearchConfiguration = None,
      vectorSearchProfile = field.vectorReference.map { reference =>
        if (profileNames.contains(reference)) reference
        else profileByAlgorithm.getOrElse(reference, reference)
      })

    index.copy(
      fields = index.fields.map(rewrite),
      vectorSearch = index.vectorSearch.map(_ =>
        VectorSearch(algorithms = Some(algorithms), profiles = Some(profiles))))
  }

  private def toLegacySchema(index: IndexInfo): IndexInfo = {
    val algorithms = index.vectorSearch.map(_.effectiveAlgorithms).getOrElse(Seq.empty)
    val algorithmByProfile = index.vectorSearch.flatMap(_.profiles).getOrElse(Seq.empty)
      .map(profile => profile.name -> profile.algorithm).toMap

    def rewrite(field: IndexField): IndexField = field.copy(
      // Pre-2023-11-01 rejects searchable vector fields.
      searchable = if (field.isVectorField) None else field.searchable,
      fields = field.fields.map(_.map(rewrite)),
      vectorSearchConfiguration = field.vectorReference.map(r => algorithmByProfile.getOrElse(r, r)),
      vectorSearchProfile = None)

    index.copy(
      fields = index.fields.map(rewrite),
      vectorSearch = index.vectorSearch.map(_ =>
        VectorSearch(algorithmConfigurations = Some(algorithms))))
  }
}
