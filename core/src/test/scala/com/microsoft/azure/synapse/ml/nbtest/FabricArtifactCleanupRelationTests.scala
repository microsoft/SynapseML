// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.nbtest

import org.scalatest.funsuite.AnyFunSuite
import spray.json._

import java.time.Instant

private[nbtest] trait FabricArtifactCleanupRelationTests extends AnyFunSuite {
  private val now = Instant.parse("2026-10-13T12:00:00Z")
  private val expired = now.minusSeconds(25 * 60 * 60).toString
  private val storeId = "00000000-0000-0000-0000-000000000001"
  private val jobId = "00000000-0000-0000-0000-000000000002"
  private val foreignId = "00000000-0000-0000-0000-000000000003"

  private def artifact(id: String, name: String, kind: String, relations: JsValue): JsObject =
    JsObject("objectId" -> JsString(id), "displayName" -> JsString(name), "artifactType" -> JsString(kind),
      "description" -> JsString(FabricArtifactCleanup.Owner),
      "createdDate" -> JsString(expired), "lastUpdatedDate" -> JsString(expired),
      "provisionState" -> JsString("Active"), "artifactRelations" -> relations,
      "datasetRelations" -> JsNull, "dataflowRelations" -> JsNull, "datamartRelations" -> JsNull)

  private val store = artifact(storeId,
    "Lakehouse202610100000000123456789abcdef0123456789abcdef", "Lakehouse", JsNull)

  private def job(relation: JsValue): JsObject = artifact(jobId,
    "OnePlusOne-20261010-00-00-00-0123456789abcdef0123456789abcdef", "SparkJobDefinition", JsArray(relation))

  private def relation(settings: String, settingsList: String, usage: String): JsObject =
    JsObject("dependentArtifactObjectId" -> JsString(storeId),
      "settings" -> JsString(settings), "settingsList" -> JsString(settingsList), "usage" -> JsString(usage))

  private class CleanupClient(initial: Vector[JsObject]) extends FabricArtifactCleanup.Client {
    var metadata: Vector[JsObject] = initial
    var deleted: Vector[String] = Vector.empty
    override def inventory(): Vector[FabricArtifactCleanup.Item] = metadata.map(FabricArtifactCleanup.item)
    override def jobs(id: String): Vector[JsValue] = Vector.empty
    override def schedules(id: String): Vector[JsValue] = Vector.empty
    override def delete(id: String): Unit = {
      deleted :+= id
      metadata = metadata.filterNot(_.fields("objectId") == JsString(id))
    }
    def run(): Vector[String] = FabricArtifactCleanup.run(this, now, pause = _ => (), log = _ => ())
  }

  test("Accept typed Fabric artifact relations without losing dependencies during cleanup") {
    Seq(("CascadeDelete", "CascadeDelete", "None"), ("WeakAssociation", "WeakAssociation", "None"),
      ("Datasource", "None", "Datasource"), ("None", "None", "None")).foreach {
      case (settings, settingsList, usage) =>
        val metadata = job(relation(settings, settingsList, usage))
        assert(FabricArtifactCleanup.item(metadata).references == Set(storeId))
        val owned = new CleanupClient(Vector(store, metadata))
        assert(owned.run() == Vector(jobId, storeId))
        assert(owned.deleted == Vector(jobId, storeId))
        assert(owned.metadata.isEmpty)

        val foreign = JsObject(metadata.fields.updated("description", JsString("Another repository")))
        val shared = new CleanupClient(Vector(store, foreign))
        assert(shared.run().isEmpty)
        assert(shared.deleted.isEmpty)
    }
  }

  test("Preserve additional reference edges alongside typed Fabric relation metadata") {
    val metadata = job(JsObject(relation("CascadeDelete", "CascadeDelete", "None").fields +
      ("artifactObjectId" -> JsString(foreignId))))
    assert(FabricArtifactCleanup.item(metadata).references == Set(storeId, foreignId))
    val client = new CleanupClient(Vector(store, metadata))
    assert(client.run().isEmpty)
    assert(client.deleted.isEmpty)
  }

  test("Reject malformed or unknown typed Fabric relation metadata before deleting any item") {
    val valid = relation("CascadeDelete", "CascadeDelete", "None")
    val invalidValues = Seq[JsValue](JsNull, JsNumber(1), JsBoolean(false), JsString("Unknown"),
      JsString(foreignId), JsArray(JsString(foreignId)), JsObject("reference" -> JsString(foreignId)))
    val invalidMetadata = for {
      field <- Seq("settings", "settingsList", "usage")
      value <- invalidValues
    } yield JsObject(valid.fields.updated(field, value))
    val invalidIds = Seq[JsValue](JsNull, JsString("not-an-id"),
      JsObject("reference" -> JsString(storeId))).map(value =>
      JsObject(valid.fields.updated("dependentArtifactObjectId", value)))
    val unknown = JsObject(valid.fields + ("futureMetadata" -> JsString("None")))
    val missingId = JsObject(valid.fields - "dependentArtifactObjectId")
    (invalidMetadata ++ invalidIds ++ Seq(unknown, missingId)).foreach { invalid =>
      val client = new CleanupClient(Vector(store, job(invalid)))
      val error = intercept[IllegalArgumentException](client.run())
      assert(error.getMessage.contains("artifactRelations"))
      assert(client.deleted.isEmpty)
      assert(client.metadata.size == 2)
    }
  }
}
