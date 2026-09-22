// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.nbtest

import spray.json._

import java.net.{URI, URLEncoder}
import java.time.{Instant, LocalDateTime, OffsetDateTime, ZoneOffset}
import java.util.concurrent.TimeUnit
import scala.annotation.tailrec
import scala.util.Try
import scala.util.control.NonFatal

private[ml] object FabricArtifactCleanup {
  val Owner = "SynapseML OSS Fabric E2E"
  private val RetentionSeconds = TimeUnit.HOURS.toSeconds(24)
  private val ConfirmationAttempts = 11
  private val ConfirmationDelayMillis = TimeUnit.SECONDS.toMillis(30)
  private val Guid = "[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}"
  private val UniqueStore = "(Lakehouse|Warehouse)[0-9]{14}[0-9a-fA-F]{32}".r
  private val RelationFields = Seq("artifactRelations", "datasetRelations", "dataflowRelations", "datamartRelations")
  private val TerminalStates = Set("Completed", "Failed", "Cancelled", "Canceled", "Deduped")

  case class Item(id: String, name: String, kind: String, description: String,
                  created: Option[Instant], updated: Option[Instant], state: String,
                  references: Set[String]) {
    def expired(cutoff: Instant): Boolean =
      created.exists(_.isBefore(cutoff)) && updated.exists(_.isBefore(cutoff)) && state == "Active"
  }

  trait Client {
    def inventory(): Vector[Item]
    def jobs(id: String): Vector[JsValue]
    def schedules(id: String): Vector[JsValue]
    def delete(id: String): Unit
  }

  private def text(value: JsValue, field: String): Option[String] =
    value.asJsObject.fields.get(field).collect { case JsString(s) if s.nonEmpty => s }

  private def timestamp(value: JsValue, field: String): Option[Instant] =
    text(value, field).map { s =>
      Try(OffsetDateTime.parse(s).toInstant).getOrElse(LocalDateTime.parse(s).toInstant(ZoneOffset.UTC))
    }

  private def references(value: JsValue, field: String): Set[String] = value match {
    case JsString(s) if s.matches(Guid) => Set(java.util.UUID.fromString(s).toString)
    case JsObject(fields) if fields.nonEmpty => fields.values.flatMap(references(_, field)).toSet
    case JsArray(values) if values.nonEmpty => values.flatMap(references(_, field)).toSet
    case _ => throw new IllegalArgumentException(s"Invalid or unknown $field relation metadata")
  }

  private def reference(value: JsValue, field: String): Set[String] = value.asJsObject.fields.get(field) match {
    case None | Some(JsNull) => Set.empty
    case Some(JsString(id)) if id.matches(Guid) => Set(java.util.UUID.fromString(id).toString)
    case _ => throw new IllegalArgumentException(s"Invalid artifact reference in $field")
  }

  def item(value: JsValue): Item = {
    val fields = value.asJsObject.fields
    val id = text(value, "objectId").getOrElse(throw new IllegalArgumentException("Missing artifact ID"))
    require(id.matches(Guid), "Invalid artifact ID")
    val relations = RelationFields.flatMap { field =>
      fields.get(field) match {
        case Some(JsNull) => Set.empty[String]
        case Some(JsArray(values)) =>
          values.flatMap(references(_, field)).toSet
        case _ => throw new IllegalArgumentException(s"Missing or invalid $field metadata for $id")
      }
    }.toSet
    val parent = reference(value, "parentArtifactObjectId")
    val store = fields.get("extendedProperties") match {
      case Some(properties: JsObject) =>
        Seq("DefaultLakehouseArtifactId", "DefaultWarehouseArtifactId").flatMap(reference(properties, _)).toSet
      case None | Some(JsNull) => Set.empty[String]
      case _ => throw new IllegalArgumentException(s"Invalid extended properties for $id")
    }
    require((parent ++ store).forall(_.matches(Guid)), s"Invalid artifact references for $id")
    val canonicalId = java.util.UUID.fromString(id).toString
    val canonicalReferences = (relations ++ parent ++ store).map(java.util.UUID.fromString(_).toString)
    Item(canonicalId, text(value, "displayName").getOrElse(""), text(value, "artifactType").getOrElse(""),
      text(value, "description").getOrElse(""), timestamp(value, "createdDate"),
      timestamp(value, "lastUpdatedDate"), text(value, "provisionState").getOrElse("Unknown"),
      canonicalReferences - canonicalId)
  }

  private def cursor(obj: JsObject, key: String): Option[String] = obj.fields.get(key) match {
    case None | Some(JsNull) | Some(JsString("")) => None
    case Some(JsString(value)) => Some(value)
    case _ => throw new IllegalArgumentException(s"Invalid Fabric pagination field $key")
  }

  private def validatePage(origin: URI, current: String, visited: Set[String]): Unit = {
    val uri = new URI(current)
    require(uri.getScheme == "https" && uri.getRawAuthority == origin.getRawAuthority &&
      uri.getPath == origin.getPath && uri.getFragment == null && !visited(current),
      "Unsafe or repeated Fabric pagination URL")
  }

  def pages(url: String, get: String => JsValue): Vector[JsValue] = {
    val origin = new URI(url)
    @tailrec
    def read(current: String, visited: Set[String], result: Vector[JsValue]): Vector[JsValue] = {
      validatePage(origin, current, visited)
      get(current) match {
        case JsArray(values) => result ++ values
        case obj: JsObject =>
          val values = obj.fields.get("value").orElse(obj.fields.get("artifacts"))
          val items = values.collect { case JsArray(entries) => entries }.getOrElse {
            throw new IllegalArgumentException("Unrecognized Fabric inventory page")
          }
          val next = cursor(obj, "continuationUri").orElse(cursor(obj, "@odata.nextLink")).orElse {
            cursor(obj, "continuationToken").map(t =>
              s"$url?continuationToken=${URLEncoder.encode(t, "UTF-8")}")
          }
          next match {
            case Some(page) => read(page, visited + current, result ++ items)
            case None => result ++ items
          }
        case _ => throw new IllegalArgumentException("Unrecognized Fabric inventory response")
      }
    }
    read(url, Set.empty, Vector.empty)
  }

  private def ownedJob(i: Item): Boolean =
    i.kind == "SparkJobDefinition" && FabricNotebookTests.isTestArtifactName(i.name) &&
      (i.description == Owner || i.description == "Synapse Spark Job Definition SparkJobDefinition")

  private def ownedStore(i: Item, initial: Map[String, Item]): Boolean = {
    val storeName = i.kind == "Lakehouse" || i.kind == "Warehouse"
    val legacyUnique = UniqueStore.pattern.matcher(i.name).matches() &&
      i.description == s"SynapseML Test Infra ${i.kind}"
    val linked = neighbors(i.id, initial).exists(id => initial.get(id).exists(ownedJob))
    storeName && FabricNotebookTests.isTestArtifactName(i.name) &&
      (i.description == Owner || legacyUnique ||
        (i.description == s"SynapseML Test Infra ${i.kind}" && linked))
  }

  private def index(items: Vector[Item]): Map[String, Item] = {
    val distinct = items.distinct
    require(distinct.map(_.id).distinct.size == distinct.size, "Conflicting Fabric inventory IDs")
    distinct.map(i => i.id -> i).toMap
  }

  private def neighbors(id: String, items: Map[String, Item]): Set[String] =
    items(id).references ++ items.values.filter(_.references(id)).map(_.id)

  private def idle(client: Client, id: String, cutoff: Instant): Boolean = {
    val history = client.jobs(id)
    val schedules = client.schedules(id)
    val noSchedule = schedules.forall(_.asJsObject.fields.get("enabled").contains(JsBoolean(false)))
    noSchedule && history.forall(j => text(j, "status").exists(TerminalStates) &&
      timestamp(j, "endTimeUtc").exists(_.isBefore(cutoff)))
  }

  private def managedEndpoint(i: Item, store: Item, cutoff: Instant): Boolean =
    store.kind == "Lakehouse" && i.kind == "SQLEndpoint" &&
      i.references == Set(store.id) && i.expired(cutoff)

  private def confirmAbsent(client: Client, id: String, pause: Long => Unit): Unit = {
    @tailrec
    def check(remaining: Int): Unit = {
      if (index(client.inventory()).contains(id)) {
        require(remaining > 1,
          s"Fabric cleanup could not confirm deletion of $id after $ConfirmationAttempts reads")
        pause(ConfirmationDelayMillis)
        check(remaining - 1)
      }
    }
    check(ConfirmationAttempts)
  }

  private def safeJob(candidate: Item, current: Map[String, Item], initial: Map[String, Item],
                      client: Client, cutoff: Instant): Boolean = {
    idle(client, candidate.id, cutoff) && neighbors(candidate.id, current).forall { id =>
      current.get(id).exists(i => initial.contains(id) && ownedStore(i, initial) &&
        i.expired(cutoff) && neighbors(i.id, current).forall { other =>
          other == candidate.id || current.get(other).exists(j => ownedJob(j) || managedEndpoint(j, i, cutoff))
        })
    }
  }

  private def safeStore(candidate: Item, current: Map[String, Item], cutoff: Instant): Boolean = {
    !current.values.exists(ownedJob) &&
      !current.values.exists(i => Set("SparkJobDefinition", "Notebook")(i.kind) && i.references.isEmpty) &&
      neighbors(candidate.id, current).forall(id => current.get(id).exists(i =>
      managedEndpoint(i, candidate, cutoff) && neighbors(i.id, current) == Set(candidate.id)))
  }

  private def tryDeleteItem(client: Client, id: String, log: String => Unit): Option[Throwable] = {
    try {
      client.delete(id)
      None
    } catch {
      case e: RuntimeException if Option(e.getMessage).exists(_.contains("PowerBIEntityNotFound")) =>
        log(s"Fabric cleanup item $id was concurrently deleted; confirming absence")
        None
      case NonFatal(e) => Some(e)
    }
  }

  def run(client: Client, now: Instant, dryRun: Boolean = false,
          pause: Long => Unit = millis => Thread.sleep(millis),
          log: String => Unit = println): Vector[String] = {
    val cutoff = now.minusSeconds(RetentionSeconds)
    val initial = index(client.inventory())
    val jobs = initial.values.filter(ownedJob).toVector.sortBy(_.id)
    val stores = initial.values.filter(i => ownedStore(i, initial)).toVector.sortBy(_.id)
    var deleted = Vector.empty[String]
    var failures = Vector.empty[Throwable]
    try {
      (jobs ++ stores).filter(_.expired(cutoff)).foreach { candidate =>
        val current = index(client.inventory())
        val expected = candidate.copy(references = candidate.references -- deleted)
        val unchanged = current.get(candidate.id).contains(expected)
        val safe = unchanged && (if (ownedJob(candidate)) {
          safeJob(candidate, current, initial, client, cutoff)
        } else {
          failures.isEmpty && safeStore(candidate, current, cutoff)
        })
        if (safe) {
          log(s"Fabric cleanup ${if (dryRun) "would delete" else "deleting"} ${candidate.kind} " +
            s"${candidate.id} (${candidate.name}); created=${candidate.created}, updated=${candidate.updated}")
          if (!dryRun) {
            tryDeleteItem(client, candidate.id, log) match {
              case Some(e) =>
                failures :+= e
                log(s"Fabric cleanup failed for ${candidate.id}: ${e.getClass.getSimpleName}; retaining stores")
              case None =>
                confirmAbsent(client, candidate.id, pause)
                deleted :+= candidate.id
                log(s"Fabric cleanup confirmed deletion of ${candidate.id}")
            }
          }
        } else {
          log(s"Fabric cleanup retains ${candidate.id}: changed metadata, active jobs, schedules, or dependencies")
        }
      }
    } catch {
      case NonFatal(e) =>
        failures.filterNot(_ eq e).foreach(e.addSuppressed)
        throw e
    }
    failures.headOption.foreach { first =>
      failures.tail.filterNot(_ eq first).foreach(first.addSuppressed)
      throw first
    }
    log(s"Fabric cleanup examined ${initial.size} items, " +
      s"found ${jobs.size} owned jobs and ${stores.size} owned stores, " +
      s"and confirmed ${deleted.size} deletions; dryRun=$dryRun")
    deleted
  }
}
