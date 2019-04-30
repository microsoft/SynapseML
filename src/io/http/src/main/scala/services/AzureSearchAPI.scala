// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import com.microsoft.ml.spark.cognitive._
import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.types._
import spray.json._

import scala.util.{Failure, Success, Try}
import AzureSearchProtocol._

import RESTHelpers._

trait IndexParser {
  def parseIndexJson(str: String): IndexInfo = {
    str.parseJson.convertTo[IndexInfo]
  }
}

trait IndexLister {
  def getExisting(key: String,
                  serviceName: String,
                  apiVersion: String = "2017-11-11"): Seq[String] = {
    val indexListRequest = new HttpGet(
      s"https://$serviceName.search.windows.net/indexes?api-version=$apiVersion&$$select=name"
    )
    indexListRequest.setHeader("api-key", key)
    val indexListResponse = safeSend(indexListRequest, close = false)
    val indexList = IOUtils.toString(indexListResponse.getEntity.getContent, "utf-8").parseJson.convertTo[IndexList]
    indexListResponse.close()
    for (i <- indexList.value.seq) yield i.name
  }
}

object SearchIndex extends IndexParser with IndexLister {

  import AzureSearchProtocol._

  val logger: Logger = LogManager.getRootLogger

  def createIfNoneExists(key: String,
                         serviceName: String,
                         indexJson: String,
                         apiVersion: String = "2017-11-11"): Unit = {
    val indexName = parseIndexJson(indexJson).name.get

    val existingIndexNames = getExisting(key, serviceName, apiVersion)

    if (!existingIndexNames.contains(indexName)) {
      val createRequest = new HttpPost(s"https://$serviceName.search.windows.net/indexes?api-version=$apiVersion")
      createRequest.setHeader("Content-Type", "application/json")
      createRequest.setHeader("api-key", key)
      createRequest.setEntity(prepareEntity(indexJson))
      val response = safeSend(createRequest)
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
      _ <- validType(field.`type`)
      _ <- validSearchable(field.`type`, field.searchable)
      _ <- validSortable(field.`type`, field.sortable)
      _ <- validFacetable(field.`type`, field.facetable)
      _ <- validKey(field.`type`, field.key)
      _ <- validAnalyzer(field.analyzer, field.searchAnalyzer, field.indexAnalyzer)
      _ <- validSearchAnalyzer(field.analyzer, field.searchAnalyzer, field.indexAnalyzer)
      _ <- validIndexAnalyzer(field.analyzer, field.searchAnalyzer, field.indexAnalyzer)
      _ <- validSynonymMaps(field.synonymMap)
    } yield field
  }

  private def validIndexFields(fields: Seq[IndexField]): Try[Seq[IndexField]] = {
    val x = fields.map(f => validIndexField(f))
    val y = x.collect { case Success(f) => f }
    if (y.length == x.length) {
      Success(y)
    } else Failure(new IllegalArgumentException("Invalid fields"))
  }

  private val validFieldTypes = Seq("Edm.String",
    "Collection(Edm.String)",
    "Edm.Int32",
    "Edm.Int64",
    "Edm.Double",
    "Edm.Boolean",
    "Edm.DateTimeOffset",
    "Edm.GeographyPoint")

  private def validName(n: String): Try[String] = {
    if (n.isEmpty) {
      Failure(new IllegalArgumentException("Empty name"))
    } else Success(n)
  }

  private def validType(t: String): Try[String] = {
    if (validFieldTypes.contains(t)) {
      Success(t)
    } else Failure(new IllegalArgumentException("Invalid field type"))
  }

  private def validSearchable(t: String, s: Option[Boolean]): Try[Option[Boolean]] = {
    if (Seq("Edm.String", "Collection(Edm.String)").contains(t)) {
      Success(s)
    } else if (s.contains(true)) {
      Failure(new IllegalArgumentException("Only Edm.String and Collection(Edm.String) fields can be searchable"))
    } else {
      Success(s)
    }
  }

  private def validSortable(t: String, s: Option[Boolean]): Try[Option[Boolean]] = {
    if (t == "Collection(Edm.String)" && s.contains(true)) {
      Failure(new IllegalArgumentException("Collection(Edm.String) fields cannot be sortable"))
    } else {
      Success(s)
    }
  }

  private def validFacetable(t: String, s: Option[Boolean]): Try[Option[Boolean]] = {
    if (t == "Edm.GeographyPoint" && s.contains(true)) {
      Failure(new IllegalArgumentException("Edm.GeographyPoint fields cannot be facetable"))
    } else {
      Success(s)
    }
  }

  private def validKey(t: String, s: Option[Boolean]): Try[Option[Boolean]] = {
    if (t != "Edm.String" && s.contains(true)) {
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

  def getStatistics(indexName: String,
                    key: String,
                    serviceName: String,
                    apiVersion: String = "2017-11-11"): (Int, Int) = {
    val getStatsRequest = new HttpGet(
      s"https://$serviceName.search.windows.net/indexes/$indexName/stats?api-version=$apiVersion")
    getStatsRequest.setHeader("api-key", key)
    val statsResponse = safeSend(getStatsRequest, close = false)
    val stats = IOUtils.toString(statsResponse.getEntity.getContent, "utf-8").parseJson.convertTo[IndexStats]
    statsResponse.close()

    (stats.documentCount, stats.storageSize)
  }

}
