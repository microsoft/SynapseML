// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.search

import com.microsoft.azure.synapse.ml.services.search.AzureSearchProtocol._
import com.microsoft.azure.synapse.ml.io.http.RESTHelpers._
import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.log4j.{LogManager, Logger}
import spray.json._

import java.util.UUID
import scala.util.{Failure, Success, Try}

object AzureSearchAPIConstants {
  val DefaultAPIVersion = "2023-07-01-Preview"
  val VectorConfigName = "vectorConfig"
  val VectorSearchAlgorithm = "hnsw"
  val AADHeaderName = "Authorization"
}
import com.microsoft.azure.synapse.ml.services.search.AzureSearchAPIConstants._

trait IndexParser {
  def parseIndexJson(str: String): IndexInfo = {
    str.parseJson.convertTo[IndexInfo]
  }
}

trait IndexLister {

  def getExisting(key: Option[String],
                  AADToken: Option[String],
                  serviceName: String,
                  apiVersion: String = DefaultAPIVersion): Seq[String] = {
    val req = new HttpGet(
      s"https://$serviceName.search.windows.net/indexes?api-version=$apiVersion&$$select=name"
    )
    key.foreach(k => req.setHeader("api-key", k))
    AADToken.foreach { token =>
      req.setHeader(AADHeaderName, "Bearer " + token)
    }

    val response = safeSend(req, close = false)
    val indexList = IOUtils.toString(response.getEntity.getContent, "utf-8").parseJson.convertTo[IndexList]
    response.close()
    for (i <- indexList.value.seq) yield i.name
  }
}

trait IndexJsonGetter extends IndexLister {
  def getIndexJsonFromExistingIndex(key: Option[String],
                                    AADToken: Option[String],
                                    serviceName: String,
                                    indexName: String,
                                    apiVersion: String = DefaultAPIVersion): String = {
    val existingIndexNames = getExisting(key, AADToken, serviceName, apiVersion)
    assert(existingIndexNames.contains(indexName), s"Cannot find an existing index name with $indexName")

    val req = new HttpGet(
      s"https://$serviceName.search.windows.net/indexes/$indexName?api-version=$apiVersion"
    )
    key.foreach(k => req.setHeader("api-key", k))
    AADToken.foreach { token =>
      req.setHeader(AADHeaderName, "Bearer " + token)
    }
    req.setHeader("Content-Type", "application/json")
    val indexJsonResponse = safeSend(req, close = false)
    val indexJson = IOUtils.toString(indexJsonResponse.getEntity.getContent, "utf-8")
    indexJsonResponse.close()
    indexJson
  }
}

object SearchIndex extends IndexParser with IndexLister {

  import AzureSearchProtocol._

  val Logger: Logger = LogManager.getRootLogger

  def createIfNoneExists(key: Option[String],
                         AADToken: Option[String],
                         serviceName: String,
                         indexJson: String,
                         apiVersion: String = DefaultAPIVersion): Unit = {
    val indexName = parseIndexJson(indexJson).name.get

    val existingIndexNames = getExisting(key, AADToken, serviceName, apiVersion)

    if (!existingIndexNames.contains(indexName)) {
      val req = new HttpPost(s"https://$serviceName.search.windows.net/indexes?api-version=$apiVersion")
      req.setHeader("Content-Type", "application/json")
      key.foreach(k => req.setHeader("api-key", k))
      AADToken.foreach { token =>
        req.setHeader(AADHeaderName, "Bearer " + token)
      }
      req.setEntity(prepareEntity(indexJson))
      val response = safeSend(req)
      val status = response.getStatusLine.getStatusCode
      assert(status == 201)
      ()
    }

  }

  private def prepareEntity(indexJson: String): StringEntity = {
    new StringEntity(validIndexJson(indexJson).get)
  }

  // validate schema
  private def validIndexJson(indexJson: String): Try[String] = {
    validateIndexInfo(indexJson).map(_.toJson.compactPrint)
  }

  private def validateIndexInfo(indexJson: String): Try[IndexInfo] = {
    val schema = parseIndexJson(indexJson)
    for {
      _ <- validName(schema.name.get)
      _ <- validIndexFields(schema.fields)
    } yield schema
  }

  private def validIndexField(field: IndexField): Try[IndexField] = {
    for {
      _ <- validName(field.name)
      _ <- validType(field.`type`, field.fields)
      _ <- validSearchable(field.`type`, field.searchable)
      _ <- validSortable(field.`type`, field.sortable)
      _ <- validFacetable(field.`type`, field.facetable)
      _ <- validKey(field.`type`, field.key)
      _ <- validAnalyzer(field.analyzer, field.searchAnalyzer, field.indexAnalyzer)
      _ <- validSearchAnalyzer(field.analyzer, field.searchAnalyzer, field.indexAnalyzer)
      _ <- validIndexAnalyzer(field.analyzer, field.searchAnalyzer, field.indexAnalyzer)
      _ <- validVectorField(field.dimensions, field.vectorSearchConfiguration)
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
    val tdt = Try(AzureSearchWriter.edmTypeToSparkType(t, fields))
    tdt.map(_ => t)
  }

  private def validSearchable(t: String, s: Option[Boolean]): Try[Option[Boolean]] = {
    if (Set("Edm.String", "Collection(Edm.String)")(t)) {
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
      Failure(new IllegalArgumentException("Both dimensions and vectorSearchConfig fields need to be defined for " +
        "vector search"))
    } else {
      Success(v)
    }
  }

  def getStatistics(indexName: String,
                    key: String,
                    serviceName: String,
                    apiVersion: String = DefaultAPIVersion): (Int, Int) = {
    val getStatsRequest = new HttpGet(
      s"https://$serviceName.search.windows.net/indexes/$indexName/stats?api-version=$apiVersion")
    getStatsRequest.setHeader("api-key", key)
    val statsResponse = safeSend(getStatsRequest, close = false)
    val stats = IOUtils.toString(statsResponse.getEntity.getContent, "utf-8").parseJson.convertTo[IndexStats]
    statsResponse.close()

    (stats.documentCount, stats.storageSize)
  }

}
