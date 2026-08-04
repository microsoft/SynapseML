// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.search

import com.microsoft.azure.synapse.ml.services.search.AzureSearchProtocol._
import com.microsoft.azure.synapse.ml.io.http.RESTHelpers._
import org.apache.commons.io.IOUtils
import org.apache.log4j.{LogManager, Logger}
import spray.json._

import scala.util.{Failure, Success, Try}

object AzureSearchAPIConstants {
  /** Latest generally available Azure AI Search (formerly Azure Cognitive Search) data plane API version.
    *
    * The previous default, `2023-07-01-Preview`, was deprecated on 2024-04-08 and has been out of
    * support since 2024-07-08.
    */
  val DefaultAPIVersion = "2026-04-01"
  val VectorConfigName = "vectorConfig"
  val VectorProfileName = "vectorProfile"
  val VectorSearchAlgorithm = "hnsw"

  /** First API version that replaced `vectorSearch.algorithmConfigurations` with
    * `vectorSearch.algorithms` + `vectorSearch.profiles`, and field-level
    * `vectorSearchConfiguration` with `vectorSearchProfile`.
    */
  val VectorProfileMinAPIVersion = "2023-11-01"

  /** True when the api-version expects the profile-based vector schema.
    *
    * API versions are `yyyy-MM-dd` optionally followed by a preview suffix, so an ISO date prefix
    * comparison orders them correctly. Unrecognized values are treated as current.
    */
  def supportsVectorProfiles(apiVersion: String): Boolean = {
    val datePrefixLength = VectorProfileMinAPIVersion.length
    Option(apiVersion).map(_.trim).forall { version =>
      version.length < datePrefixLength || version.take(datePrefixLength) >= VectorProfileMinAPIVersion
    }
  }
}
import com.microsoft.azure.synapse.ml.services.search.AzureSearchAPIConstants._

trait IndexParser {
  def parseIndexJson(str: String): IndexInfo = {
    str.parseJson.convertTo[IndexInfo]
  }
}

trait IndexLister {
  def getExisting(key: String,
                  serviceName: String,
                  apiVersion: String = DefaultAPIVersion): Seq[String] = {
    getExisting(AzureSearchAuth.fromSubscriptionKey(key), serviceName, apiVersion)
  }

  def getExisting(auth: AzureSearchAuth,
                  serviceName: String): Seq[String] = {
    getExisting(auth, serviceName, DefaultAPIVersion)
  }

  def getExisting(auth: AzureSearchAuth,
                  serviceName: String,
                  apiVersion: String): Seq[String] = {
    val response = safeSend(AzureSearchRequests.listIndexes(auth, serviceName, apiVersion), close = false)
    try {
      val indexList = IOUtils.toString(response.getEntity.getContent, "utf-8").parseJson.convertTo[IndexList]
      indexList.value.map(_.name)
    } finally {
      response.close()
    }
  }
}

trait IndexJsonGetter extends IndexLister {
  def getIndexJsonFromExistingIndex(key: String,
                                    serviceName: String,
                                    indexName: String,
                                    apiVersion: String = DefaultAPIVersion): String = {
    getIndexJsonFromExistingIndex(AzureSearchAuth.fromSubscriptionKey(key), serviceName, indexName, apiVersion)
  }

  def getIndexJsonFromExistingIndex(auth: AzureSearchAuth,
                                    serviceName: String,
                                    indexName: String): String = {
    getIndexJsonFromExistingIndex(auth, serviceName, indexName, DefaultAPIVersion)
  }

  def getIndexJsonFromExistingIndex(auth: AzureSearchAuth,
                                    serviceName: String,
                                    indexName: String,
                                    apiVersion: String): String = {
    val existingIndexNames = getExisting(auth, serviceName, apiVersion)
    assert(existingIndexNames.contains(indexName), s"Cannot find an existing index name with $indexName")

    val response = safeSend(
      AzureSearchRequests.getIndex(auth, serviceName, indexName, apiVersion), close = false)
    try {
      IOUtils.toString(response.getEntity.getContent, "utf-8")
    } finally {
      response.close()
    }
  }
}

object SearchIndex extends IndexParser with IndexLister {

  import AzureSearchProtocol._

  val Logger: Logger = LogManager.getRootLogger

  def createIfNoneExists(key: String,
                         serviceName: String,
                         indexJson: String,
                         apiVersion: String = DefaultAPIVersion): Unit = {
    createIfNoneExists(AzureSearchAuth.fromSubscriptionKey(key), serviceName, indexJson, apiVersion)
  }

  def createIfNoneExists(auth: AzureSearchAuth,
                         serviceName: String,
                         indexJson: String): Unit = {
    createIfNoneExists(auth, serviceName, indexJson, DefaultAPIVersion)
  }

  def createIfNoneExists(auth: AzureSearchAuth,
                         serviceName: String,
                         indexJson: String,
                         apiVersion: String): Unit = {
    val indexName = parseIndexJson(indexJson).name.get
    val existingIndexNames = getExisting(auth, serviceName, apiVersion)

    if (!existingIndexNames.contains(indexName)) {
      val request = AzureSearchRequests.createIndex(auth, serviceName, prepareEntity(indexJson, apiVersion), apiVersion)
      val response = safeSend(request, close = false)
      try {
        assert(response.getStatusLine.getStatusCode == 201)
      } finally {
        response.close()
      }
    }
  }

  private def prepareEntity(indexJson: String, apiVersion: String): String = {
    validIndexJson(indexJson, apiVersion).get
  }

  // validate schema
  private def validIndexJson(indexJson: String, apiVersion: String): Try[String] = {
    validateIndexInfo(indexJson, apiVersion).map(_.toJson.compactPrint)
  }

  private def validateIndexInfo(indexJson: String, apiVersion: String): Try[IndexInfo] = {
    // Align first so the definition that is validated is exactly the one that gets sent.
    val schema = VectorSchema.align(parseIndexJson(indexJson), apiVersion)
    for {
      _ <- validName(schema.name.get)
      _ <- validIndexFields(schema.fields)
    } yield schema
  }

  private def validIndexField(field: IndexField): Try[IndexField] = {
    for {
      _ <- validName(field.name)
      _ <- validType(field.`type`, field.fields)
      _ <- validSearchable(field.`type`, field.searchable, field.dimensions)
      _ <- validSortable(field.`type`, field.sortable)
      _ <- validFacetable(field.`type`, field.facetable)
      _ <- validKey(field.`type`, field.key)
      _ <- validAnalyzer(field.analyzer, field.searchAnalyzer, field.indexAnalyzer)
      _ <- validSearchAnalyzer(field.analyzer, field.searchAnalyzer, field.indexAnalyzer)
      _ <- validIndexAnalyzer(field.analyzer, field.searchAnalyzer, field.indexAnalyzer)
      _ <- validVectorField(field.dimensions, field.vectorReference)
      // TODO: Fix and add back validSynonymMaps check. SynonymMaps needs to be Option[Seq[String]] type
      //_ <- validSynonymMaps(field.synonymMap)
    } yield field
  }

  private def validIndexFields(fields: Seq[IndexField]): Try[Seq[IndexField]] = {
    Try(fields.map(f => validIndexField(f).get))
  }

  private def validName(n: String): Try[String] = {
    if (n.isEmpty) {
      Failure(new IllegalArgumentException("Empty name"))
    } else Success(n)
  }

  private def validType(t: String, fields: Option[Seq[IndexField]]): Try[String] = {
    val tdt = Try(AzureSearchWriter.edmTypeToSparkType(t,fields))
    tdt.map(_ => t)
  }

  private def validSearchable(t: String, s: Option[Boolean], dimensions: Option[Int]): Try[Option[Boolean]] = {
    if (Set("Edm.String", "Collection(Edm.String)")(t)) {
      Success(s)
    } else if (dimensions.nonEmpty) {
      // Vector fields are not Edm.String but must be searchable in 2023-11-01 and later.
      Success(s)
    } else if (s.contains(true)) {
      Failure(new IllegalArgumentException("Only Edm.String and Collection(Edm.String) fields can be searchable"))
    } else {
      Success(s)
    }
  }

  private def validSortable(t: String, s: Option[Boolean]): Try[Option[Boolean]] = {
    if (t == "Collection(Edm.String)" & s.contains(true)) {
      Failure(new IllegalArgumentException("Collection(Edm.String) fields cannot be sortable"))
    } else {
      Success(s)
    }
  }

  private def validFacetable(t: String, s: Option[Boolean]): Try[Option[Boolean]] = {
    if (t == "Edm.GeographyPoint" & s.contains(true)) {
      Failure(new IllegalArgumentException("Edm.GeographyPoint fields cannot be facetable"))
    } else {
      Success(s)
    }
  }

  private def validKey(t: String, s: Option[Boolean]): Try[Option[Boolean]] = {
    if (t != "Edm.String" & s.contains(true)) {
      Failure(new IllegalArgumentException("Only Edm.String fields can be keys"))
    } else {
      Success(s)
    }
  }

  private def validAnalyzer(a: Option[String], sa: Option[String], ia: Option[String]): Try[Option[String]] = {
    if (a.isDefined && (sa.isDefined || ia.isDefined)) {
      Failure(new IllegalArgumentException("Max of 1 analyzer can be defined"))
    } else {
      Success(a)
    }
  }

  private def validSearchAnalyzer(a: Option[String], sa: Option[String], ia: Option[String]): Try[Option[String]] = {
    if (sa.isDefined && (a.isDefined || ia.isDefined)) {
      Failure(new IllegalArgumentException("Max of 1 analyzer can be defined"))
    } else {
      Success(sa)
    }
  }

  private def validIndexAnalyzer(a: Option[String], sa: Option[String], ia: Option[String]): Try[Option[String]] = {
    if (ia.isDefined && (sa.isDefined || a.isDefined)) {
      Failure(new IllegalArgumentException("Max of 1 analyzer can be defined"))
    } else {
      Success(ia)
    }
  }

  private def validSynonymMaps(sm: Option[String]): Try[Option[String]] = {
    val regexExtractor = "\"([^, ]+)\"".r
    val extractList =
      regexExtractor.findAllMatchIn(sm.getOrElse("")).map(_ group 1).toList
    if (extractList.length > 1) {
      Failure(new IllegalArgumentException("Only one synonym map per field is supported"))
    } else {
      Success(sm)
    }
  }

  private def validVectorField(d: Option[Int], v: Option[String]): Try[Option[String]] = {
    if ((d.isDefined && v.isEmpty) || (v.isDefined && d.isEmpty)) {
      Failure(new IllegalArgumentException("Both dimensions and vectorSearchProfile (or the legacy " +
        "vectorSearchConfiguration) fields need to be defined for vector search"))
    } else {
      Success(v)
    }
  }

  def getStatistics(indexName: String,
                    key: String,
                    serviceName: String,
                    apiVersion: String = DefaultAPIVersion): (Int, Int) = {
    getStatistics(indexName, AzureSearchAuth.fromSubscriptionKey(key), serviceName, apiVersion)
  }

  def getStatistics(indexName: String,
                    auth: AzureSearchAuth,
                    serviceName: String): (Int, Int) = {
    getStatistics(indexName, auth, serviceName, DefaultAPIVersion)
  }

  def getStatistics(indexName: String,
                    auth: AzureSearchAuth,
                    serviceName: String,
                    apiVersion: String): (Int, Int) = {
    val response = safeSend(
      AzureSearchRequests.getStatistics(auth, serviceName, indexName, apiVersion), close = false)
    try {
      val stats = IOUtils.toString(response.getEntity.getContent, "utf-8").parseJson.convertTo[IndexStats]
      (stats.documentCount, stats.storageSize)
    } finally {
      response.close()
    }
  }

}
