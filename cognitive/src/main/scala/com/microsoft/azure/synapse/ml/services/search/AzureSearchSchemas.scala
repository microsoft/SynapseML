// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.search

import com.microsoft.azure.synapse.ml.core.schema.SparkBindings
import spray.json.DefaultJsonProtocol._
import spray.json._

object ASResponses extends SparkBindings[ASResponses]

case class ASResponses(value: Seq[ASResponse])

case class ASResponse(key: String, status: Boolean, errorMessage: Option[String], statusCode: Int)

// These six fields are pure pass-through: the writer round-trips them to the service and never
// inspects their contents. The Search REST API returns objects for all of them (a custom analyzer
// is {"@odata.type":"#Microsoft.Azure.Search.CustomAnalyzer",...}, corsOptions is a single object),
// so typing them as strings made any index using these features fail to deserialize. Keeping them
// as opaque JsValue also stops the library from breaking when the service adds a new analyzer kind.
// spray.json.JsValue is written out in full because the release branches this change is replayed
// onto import spray.json explicitly rather than by wildcard.
case class IndexInfo(
                    name: Option[String],
                    fields: Seq[IndexField],
                    suggesters: Option[Seq[spray.json.JsValue]],
                    scoringProfiles: Option[Seq[ScoringProfile]],
                    analyzers: Option[Seq[spray.json.JsValue]],
                    charFilters: Option[Seq[spray.json.JsValue]],
                    tokenizers: Option[Seq[spray.json.JsValue]],
                    tokenFilters: Option[Seq[spray.json.JsValue]],
                    defaultScoringProfile: Option[String],
                    corsOptions: Option[spray.json.JsValue],
                    vectorSearch: Option[VectorSearch]
                    )

case class AlgorithmConfigs(
                           name: String,
                           kind: String
                           )

case class VectorSearch(
                       algorithmConfigurations: Seq[AlgorithmConfigs]
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
                     synonymMap: Option[Seq[String]],
                     fields: Option[Seq[IndexField]],
                     dimensions: Option[Int],
                     vectorSearchConfiguration: Option[String]
                     ) {
  def vectorReference: Option[String] = vectorSearchConfiguration

  // A field is a vector field if it declares dimensions. The algorithm/profile reference
  // describes *how* the vectors are indexed, not *whether* the field holds vectors, and the
  // service does not always surface it: reading a legacy index under a modern api-version
  // returns `dimensions` with a null `vectorSearchProfile` and an empty `profiles` list.
  // Requiring a reference here would silently stop treating those fields as vectors.
  def isVectorField: Boolean = dimensions.nonEmpty
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
  implicit val AcEnc: RootJsonFormat[AlgorithmConfigs] = jsonFormat2(AlgorithmConfigs.apply)
  implicit val VsEnc: RootJsonFormat[VectorSearch] = {
    val legacyFormat = jsonFormat1(VectorSearch.apply)
    new RootJsonFormat[VectorSearch] {
      override def read(json: JsValue): VectorSearch =
        legacyFormat.read(VectorSchema.legacyVectorSearchView(json))

      override def write(vectorSearch: VectorSearch): JsValue = legacyFormat.write(vectorSearch)
    }
  }
  implicit val IfEnc: JsonFormat[IndexField] = {
    val legacyFormat = lazyFormat(jsonFormat(
      IndexField, "name", "type", "searchable", "filterable", "sortable",
      "facetable", "retrievable", "key", "analyzer", "searchAnalyzer", "indexAnalyzer", "synonymMaps", "fields",
      "dimensions", "vectorSearchConfiguration"))
    new JsonFormat[IndexField] {
      override def read(json: JsValue): IndexField =
        legacyFormat.read(VectorSchema.legacyIndexFieldView(json))

      override def write(field: IndexField): JsValue = legacyFormat.write(field)
    }
  }
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

/** Translates index JSON between the two generations of the Azure AI Search vector schema.
  *
  * `2023-10-01-Preview` renamed `vectorSearch.algorithmConfigurations` to `vectorSearch.algorithms`, added
  * `vectorSearch.profiles`, and replaced the field-level `vectorSearchConfiguration` with
  * `vectorSearchProfile`. The public Scala case classes retain their original constructor and extractor
  * shapes; REST payload modernization happens on the JSON AST so parameters, vectorizers, compressions,
  * and fields introduced by future service versions are not discarded.
  */
private[search] object VectorSchema {

  private val LegacyAlgorithmsKey = "algorithmConfigurations"
  private val ModernAlgorithmsKey = "algorithms"
  private val ProfilesKey = "profiles"
  private val LegacyFieldReferenceKey = "vectorSearchConfiguration"
  private val ModernFieldReferenceKey = "vectorSearchProfile"

  def align(index: JsValue, apiVersion: String): JsValue = {
    if (AzureSearchAPIConstants.supportsVectorProfiles(apiVersion)) toProfileSchema(index)
    else toLegacySchema(index)
  }

  def requireCompatibleExistingIndex(index: JsValue, apiVersion: String): Unit = {
    val modern = AzureSearchAPIConstants.supportsVectorProfiles(apiVersion)
    val legacyMarkers = vectorSearchContainsKey(index, LegacyAlgorithmsKey) ||
      containsFieldKey(index, LegacyFieldReferenceKey)
    val modernMarkers = vectorSearchContainsKey(index, ModernAlgorithmsKey) ||
      vectorSearchContainsKey(index, ProfilesKey) ||
      containsFieldKey(index, ModernFieldReferenceKey)

    if (modern && legacyMarkers) {
      throw new IllegalArgumentException(
        "The index already exists with the legacy vector schema. createIfNoneExists does not update or migrate " +
          "existing indexes. Migrate it with Azure AI Search Create or Update Index after reviewing the schema " +
          "changes, or set an apiVersion earlier than 2023-10-01-Preview to keep using it as-is.")
    } else if (!modern && modernMarkers) {
      throw new IllegalArgumentException(
        s"The index already exists with the profile-based vector schema, which is not supported by api-version " +
          s"$apiVersion. Use apiVersion=2023-10-01-Preview or later.")
    }
  }

  private[search] def legacyVectorSearchView(json: JsValue): JsValue =
    aliasForPublicModel(json, LegacyAlgorithmsKey, ModernAlgorithmsKey)

  private[search] def legacyIndexFieldView(json: JsValue): JsValue =
    aliasForPublicModel(json, LegacyFieldReferenceKey, ModernFieldReferenceKey)

  private def aliasForPublicModel(json: JsValue, legacyKey: String, modernKey: String): JsValue = json match {
    case JsObject(fields) =>
      (fields.get(legacyKey), fields.get(modernKey)) match {
        case (Some(legacy), Some(modern)) if legacy != modern =>
          deserializationError(s"Conflicting $legacyKey and $modernKey values")
        case (None, Some(modern)) => JsObject(fields - modernKey + (legacyKey -> modern))
        case _ => json
      }
    case _ => json
  }

  private def toProfileSchema(index: JsValue): JsValue = index match {
    case JsObject(indexFields) =>
      val (rewrittenVectorSearch, references) = indexFields.get("vectorSearch") match {
        case Some(vectorSearch: JsObject) => modernizeVectorSearch(vectorSearch)
        case Some(JsNull) | None => (indexFields.get("vectorSearch"), Map.empty[String, String])
        case Some(_) => throw new IllegalArgumentException("vectorSearch must be a JSON object")
      }
      val rewrittenFields = indexFields.get("fields").map(rewriteFields(_, references))
      JsObject(indexFields ++ rewrittenVectorSearch.map("vectorSearch" -> _) ++ rewrittenFields.map("fields" -> _))
    case _ => throw new IllegalArgumentException("Azure AI Search index JSON must be an object")
  }

  private def modernizeVectorSearch(vectorSearch: JsObject): (Option[JsValue], Map[String, String]) = {
    val fields = vectorSearch.fields
    val upgradingLegacy = !fields.contains(ModernAlgorithmsKey) && fields.contains(LegacyAlgorithmsKey)
    val algorithms = selectAlias(fields, ModernAlgorithmsKey, LegacyAlgorithmsKey)
    val existingProfiles = fields.get(ProfilesKey).map(asArray(_, ProfilesKey)).getOrElse(Vector.empty)
    val profileNames = existingProfiles.flatMap(profileValue(_, "name")).toSet
    val profileByAlgorithm = existingProfiles.flatMap { profile =>
      for {
        name <- profileValue(profile, "name")
        algorithm <- profileValue(profile, "algorithm")
      } yield algorithm -> name
    }.toMap

    val generatedProfiles = if (upgradingLegacy) {
      algorithms.toSeq.flatMap(asArray(_, ModernAlgorithmsKey)).flatMap { algorithm =>
        val algorithmName = objectString(algorithm, "name", ModernAlgorithmsKey)
        if (profileByAlgorithm.contains(algorithmName)) {
          None
        } else if (profileNames.contains(algorithmName)) {
          throw new IllegalArgumentException(
            s"Cannot generate a vector profile for algorithm '$algorithmName' because that profile name is in use")
        } else {
          Some(JsObject("name" -> JsString(algorithmName), "algorithm" -> JsString(algorithmName)))
        }
      }
    } else {
      Seq.empty
    }
    val profiles = existingProfiles ++ generatedProfiles
    val allProfileByAlgorithm = profiles.flatMap { profile =>
      for {
        name <- profileValue(profile, "name")
        algorithm <- profileValue(profile, "algorithm")
      } yield algorithm -> name
    }.toMap
    val allProfileNames = profiles.flatMap(profileValue(_, "name")).toSet
    val references = allProfileByAlgorithm ++ allProfileNames.map(name => name -> name)

    val rewritten = fields - LegacyAlgorithmsKey ++ algorithms.map(ModernAlgorithmsKey -> _) ++
      (if (profiles.nonEmpty) Some(ProfilesKey -> JsArray(profiles)) else None)
    (Some(JsObject(rewritten)), references)
  }

  private def toLegacySchema(index: JsValue): JsValue = {
    if (vectorSearchContainsKey(index, ModernAlgorithmsKey) || vectorSearchContainsKey(index, ProfilesKey) ||
      containsFieldKey(index, ModernFieldReferenceKey)) {
      throw new IllegalArgumentException(
        "Profile-based vector JSON cannot be losslessly sent to a legacy Azure AI Search api-version. " +
          "Use apiVersion=2023-10-01-Preview or later, or provide an explicit legacy definition using " +
          "algorithmConfigurations and vectorSearchConfiguration.")
    }
    index
  }

  private def rewriteFields(value: JsValue,
                            references: Map[String, String]): JsValue = value match {
    case JsArray(fields) => JsArray(fields.map(rewriteField(_, references)))
    case _ => throw new IllegalArgumentException("fields must be a JSON array")
  }

  private def hasVectorDimensions(fields: Map[String, JsValue]): Boolean =
    fields.get("dimensions").exists(_ != JsNull)

  private def rewriteField(value: JsValue,
                           references: Map[String, String]): JsValue = value match {
    case JsObject(fields) =>
      val nested = fields.get("fields").map(rewriteFields(_, references))
      val reference = selectAlias(fields, ModernFieldReferenceKey, LegacyFieldReferenceKey).map {
        case JsString(name) =>
          JsString(references.getOrElse(name, throw new IllegalArgumentException(
            s"Cannot find or create a vector profile for algorithm '$name'")))
        case _ => throw new IllegalArgumentException(
          s"$LegacyFieldReferenceKey and $ModernFieldReferenceKey must be strings")
      }
      val vectorField = hasVectorDimensions(fields) && reference.nonEmpty
      JsObject(fields - LegacyFieldReferenceKey ++ reference.map(ModernFieldReferenceKey -> _) ++
        nested.map("fields" -> _) ++ (if (vectorField) Some("searchable" -> JsBoolean(true)) else None))
    case _ => throw new IllegalArgumentException("Each field definition must be a JSON object")
  }

  private def selectAlias(fields: Map[String, JsValue],
                          preferredKey: String,
                          alternateKey: String): Option[JsValue] = {
    (fields.get(preferredKey), fields.get(alternateKey)) match {
      case (Some(preferred), Some(alternate)) if preferred != alternate =>
        throw new IllegalArgumentException(s"Conflicting $preferredKey and $alternateKey values")
      case (Some(preferred), _) => Some(preferred)
      case (_, alternate) => alternate
    }
  }

  private def asArray(value: JsValue, key: String): Vector[JsValue] = value match {
    case JsArray(elements) => elements
    case _ => throw new IllegalArgumentException(s"$key must be a JSON array")
  }

  private def objectString(value: JsValue, key: String, owner: String): String = value match {
    case JsObject(fields) => fields.get(key) match {
      case Some(JsString(text)) => text
      case _ => throw new IllegalArgumentException(s"Each $owner entry must contain a string $key")
    }
    case _ => throw new IllegalArgumentException(s"Each $owner entry must be a JSON object")
  }

  private def profileValue(value: JsValue, key: String): Option[String] = value match {
    case JsObject(fields) => fields.get(key).collect { case JsString(text) => text }
    case _ => None
  }

  private def vectorSearchContainsKey(index: JsValue, key: String): Boolean = index match {
    case JsObject(fields) => fields.get("vectorSearch").exists {
      case JsObject(vectorSearchFields) => vectorSearchFields.contains(key)
      case _ => false
    }
    case _ => false
  }

  private def containsFieldKey(index: JsValue, key: String): Boolean = index match {
    case JsObject(fields) => fields.get("fields").exists(fieldArrayContainsKey(_, key))
    case _ => false
  }

  private def fieldArrayContainsKey(value: JsValue, key: String): Boolean = value match {
    case JsArray(elements) => elements.exists {
      case JsObject(fields) =>
        fields.contains(key) || fields.get("fields").exists(fieldArrayContainsKey(_, key))
      case _ => false
    }
    case _ => false
  }
}
