// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.search

import com.microsoft.azure.synapse.ml.services.search.AzureSearchProtocol._
import com.microsoft.azure.synapse.ml.io.http.RESTHelpers._
import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.{CloseableHttpResponse, HttpRequestBase}
import org.apache.log4j.{LogManager, Logger}
import spray.json._

import java.time.LocalDate
import java.time.format.DateTimeParseException
import scala.util.{Failure, Success, Try}

object AzureSearchAPIConstants {
  /** Latest generally available Azure AI Search (formerly Azure Cognitive Search) data plane API version.
    *
    * The previous default, `2023-07-01-Preview`, was deprecated on 2024-04-08 and has been out of
    * support since 2024-07-08.
    */
  val DefaultAPIVersion = "2026-04-01"
  val VectorConfigName = "vectorConfig"
  val VectorSearchAlgorithm = "hnsw"

  /** First API version that replaced `vectorSearch.algorithmConfigurations` with
    * `vectorSearch.algorithms` + `vectorSearch.profiles`, and field-level
    * `vectorSearchConfiguration` with `vectorSearchProfile`.
    */
  val VectorProfileMinAPIVersion = "2023-10-01-Preview"
  private val VectorProfileMinAPIDate = LocalDate.parse("2023-10-01")
  private val APIVersion = """^(\d{4}-\d{2}-\d{2})(?:-[A-Za-z0-9.-]+)?$""".r

  /** True when the api-version expects the profile-based vector schema.
    *
    * API versions are parsed as `yyyy-MM-dd` optionally followed by a preview suffix. Invalid values
    * fail before a request is sent rather than being assigned a schema generation by string ordering.
    */
  def supportsVectorProfiles(apiVersion: String): Boolean = {
    val version = Option(apiVersion).map(_.trim).filter(_.nonEmpty).getOrElse {
      throw new IllegalArgumentException("Azure AI Search apiVersion must be non-empty")
    }
    val date = version match {
      case APIVersion(dateText) =>
        try {
          LocalDate.parse(dateText)
        } catch {
          case _: DateTimeParseException =>
            throw new IllegalArgumentException(s"Invalid Azure AI Search apiVersion: $apiVersion")
        }
      case _ => throw new IllegalArgumentException(s"Invalid Azure AI Search apiVersion: $apiVersion")
    }
    !date.isBefore(VectorProfileMinAPIDate)
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

private[search] object IndexJsonReader {
  def get(auth: AzureSearchAuth,
          serviceName: String,
          indexName: String,
          apiVersion: String): String = {
    read(
      AzureSearchRequests.getIndex(auth, serviceName, indexName, apiVersion),
      request => safeSend(request, close = false))
  }

  private[search] def read(request: HttpRequestBase,
                           send: HttpRequestBase => CloseableHttpResponse): String = {
    val response = send(request)
    try {
      IOUtils.toString(response.getEntity.getContent, "utf-8")
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

    IndexJsonReader.get(auth, serviceName, indexName, apiVersion)
  }
}

private[search] trait SearchIndexClient {
  def getExisting(auth: AzureSearchAuth, serviceName: String, apiVersion: String): Seq[String]

  def getIndexJson(auth: AzureSearchAuth,
                   serviceName: String,
                   indexName: String,
                   apiVersion: String): String

  def createIndex(auth: AzureSearchAuth,
                  serviceName: String,
                  indexJson: String,
                  apiVersion: String): Int
}

private object DefaultSearchIndexClient extends SearchIndexClient {
  override def getExisting(auth: AzureSearchAuth,
                           serviceName: String,
                           apiVersion: String): Seq[String] =
    SearchIndex.getExisting(auth, serviceName, apiVersion)

  override def getIndexJson(auth: AzureSearchAuth,
                            serviceName: String,
                            indexName: String,
                            apiVersion: String): String =
    IndexJsonReader.get(auth, serviceName, indexName, apiVersion)

  override def createIndex(auth: AzureSearchAuth,
                           serviceName: String,
                           indexJson: String,
                           apiVersion: String): Int = {
    val request = AzureSearchRequests.createIndex(auth, serviceName, indexJson, apiVersion)
    val response = safeSend(request, close = false)
    try {
      response.getStatusLine.getStatusCode
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
    createIfNoneExists(auth, serviceName, indexJson, apiVersion, DefaultSearchIndexClient)
  }

  private[search] def createIfNoneExists(auth: AzureSearchAuth,
                                         serviceName: String,
                                         indexJson: String,
                                         apiVersion: String,
                                         client: SearchIndexClient): Unit = {
    AzureSearchAPIConstants.supportsVectorProfiles(apiVersion)
    val indexName = parseIndexJson(indexJson).name.get
    val existingIndexNames = client.getExisting(auth, serviceName, apiVersion)

    if (!existingIndexNames.contains(indexName)) {
      val statusCode = client.createIndex(auth, serviceName, prepareEntity(indexJson, apiVersion), apiVersion)
      assert(statusCode == 201)
    } else {
      val existingIndexJson = client.getIndexJson(auth, serviceName, indexName, apiVersion)
      VectorSchema.requireCompatibleExistingIndex(existingIndexJson.parseJson, apiVersion)
    }
  }

  private[search] def prepareEntity(indexJson: String, apiVersion: String): String = {
    validIndexJson(indexJson, apiVersion).get
  }

  // validate schema
  private def validIndexJson(indexJson: String, apiVersion: String): Try[String] = {
    Try(VectorSchema.align(indexJson.parseJson, apiVersion)).flatMap { aligned =>
      validateIndexInfo(aligned, apiVersion).map(_ => aligned.compactPrint)
    }
  }

  private def validateIndexInfo(indexJson: JsValue, apiVersion: String): Try[IndexInfo] = {
    Try(indexJson.convertTo[IndexInfo]).flatMap { schema =>
      for {
        _ <- validName(schema.name.get)
        _ <- validIndexFields(schema.fields, AzureSearchAPIConstants.supportsVectorProfiles(apiVersion))
      } yield schema
    }
  }

  private def validIndexField(field: IndexField, supportsVectorProfiles: Boolean): Try[IndexField] = {
    for {
      _ <- validName(field.name)
      _ <- validType(field.`type`, field.fields)
      _ <- validSearchable(field.`type`, field.searchable, field.dimensions, supportsVectorProfiles)
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

  private def validIndexFields(fields: Seq[IndexField],
                               supportsVectorProfiles: Boolean): Try[Seq[IndexField]] = {
    Try(fields.map(f => validIndexField(f, supportsVectorProfiles).get))
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

  private def validSearchable(t: String,
                              s: Option[Boolean],
                              dimensions: Option[Int],
                              supportsVectorProfiles: Boolean): Try[Option[Boolean]] = {
    if (dimensions.nonEmpty) {
      validVectorSearchable(s, supportsVectorProfiles)
    } else if (Set("Edm.String", "Collection(Edm.String)")(t)) {
      Success(s)
    } else if (s.contains(true)) {
      Failure(new IllegalArgumentException("Only Edm.String and Collection(Edm.String) fields can be searchable"))
    } else {
      Success(s)
    }
  }

  private def validVectorSearchable(s: Option[Boolean],
                                    supportsVectorProfiles: Boolean): Try[Option[Boolean]] = {
    if (supportsVectorProfiles) {
      if (s.contains(true)) {
        Success(s)
      } else {
        Failure(new IllegalArgumentException(
          "Vector fields must set searchable=true for api-version 2023-10-01-Preview and later"))
      }
    } else if (s.contains(true)) {
      Failure(new IllegalArgumentException(
        "Legacy vector fields cannot set searchable=true; use api-version 2023-10-01-Preview or later"))
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
