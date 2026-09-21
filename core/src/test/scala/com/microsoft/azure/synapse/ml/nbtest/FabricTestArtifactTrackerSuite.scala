// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.nbtest

import java.io.File
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, ExecutorService, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import org.scalatest.{Args, Reporter, Suite}
import org.scalatest.events.{Event, TestFailed, TestSucceeded}
import org.scalatest.funsuite.AnyFunSuite
import spray.json._

import java.time.Instant

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration

class FabricTestArtifactTrackerSuite extends AnyFunSuite with FabricTestArtifactTrackerFailureTests {
  private def executeSuite(suite: Suite): Vector[Event] = {
    val events = new ConcurrentLinkedQueue[Event]()
    val reporter = new Reporter {
      override def apply(event: Event): Unit = { events.add(event); () }
    }
    suite.run(None, Args(reporter)).waitUntilCompleted()
    events.iterator().asScala.toVector
  }

  private abstract class NotebookFixture(cleanupFailure: Option[Exception] = None) extends FabricNotebookTests {
    val calls = new ConcurrentLinkedQueue[String]()
    val active = new AtomicInteger()
    val peak = new AtomicInteger()
    private val started = new CountDownLatch(FabricNotebookTests.MaxConcurrency)

    override lazy val fabric: Nothing = throw new IllegalStateException("Unexpected Fabric connection")
    override protected def discoverNotebooks(): Array[File] =
      Array("one.py", "two.py", "three.py", "four.py").map(new File(_))
    override protected def notebookTimeout: Duration = Duration(10, TimeUnit.SECONDS)
    override protected def cleanupStaleArtifacts(): Unit = {
      calls.add("cleanup")
      cleanupFailure.foreach(throw _)
    }
    override protected def createTrackedStore(): String = {
      calls.add("store")
      "test-store"
    }
    override protected def createNotebookExecutor(): ExecutorService = {
      calls.add("executor")
      Executors.newFixedThreadPool(FabricNotebookTests.MaxConcurrency)
    }
    override protected def runNotebook(file: File, storeId: String): String = {
      assert(storeId == "test-store")
      calls.add(file.getName)
      val concurrent = active.incrementAndGet()
      peak.updateAndGet(previous => math.max(previous, concurrent))
      try {
        started.countDown()
        assert(started.await(5, TimeUnit.SECONDS), "Notebook work stopped running in parallel")
        file.getName
      } finally {
        active.decrementAndGet()
      }
    }
  }

  test("Register Fabric cleanup and smoke tests without resolving a live workspace") {
    val cleanup = new FabricTestCleanup {
      override lazy val fabric: Nothing = throw new IllegalStateException("Unexpected Fabric connection")
    }
    val smoke = new FabricSmokeTests {
      override lazy val fabric: Nothing = throw new IllegalStateException("Unexpected Fabric connection")
    }
    assert(cleanup.fabricWorkspaceId.isEmpty)
    assert(smoke.fabricWorkspaceId.isEmpty)
    assert(cleanup.testNames == Set("Clean up owned Fabric test artifacts older than 24 hours"))
    assert(smoke.testNames == Set("OnePlusOne"))
  }

  test("Run smoke preflight before store creation and block all work when it fails") {
    Seq(None, Some(new IllegalStateException("cleanup failed"))).foreach { failure =>
      val calls = ArrayBuffer.empty[String]
      val suite = new FabricSmokeTests {
        override lazy val fabric: Nothing = throw new IllegalStateException("Unexpected Fabric connection")
        override protected def cleanupStaleArtifacts(): Unit = {
          calls += "cleanup"
          failure.foreach(throw _)
        }
        override protected def createTrackedStore(): String = {
          calls += "store"
          "test-store"
        }
        override protected def runSmokeTest(storeId: String): Unit = {
          assert(storeId == "test-store")
          calls += "smoke"
        }
      }
      assert(calls.isEmpty)
      val events = executeSuite(suite)
      val failures = events.collect { case event: TestFailed => event }
      if (failure.isDefined) {
        assert(calls == Seq("cleanup"))
        assert(failures.map(_.throwable) == Vector(failure))
      } else {
        assert(calls == Seq("cleanup", "store", "smoke"))
        assert(failures.isEmpty)
        assert(events.count(_.isInstanceOf[TestSucceeded]) == 1)
      }
    }
  }

  test("Cache workspace resolution failure before any smoke resource allocation") {
    val failure = new IllegalStateException("workspace unavailable")
    var lookups = 0
    val suite = new FabricSmokeTests {
      override lazy val fabric: Nothing = throw new IllegalStateException("Unexpected Fabric connection")
      override protected def integrationWorkspaceId: String = {
        lookups += 1
        throw failure
      }
    }
    (1 to 2).foreach { _ =>
      assert(intercept[IllegalStateException](suite.storeArtifactId) eq failure)
    }
    assert(lookups == 1)
    assert(suite.fabricWorkspaceId.isEmpty)
  }

  test("Defer notebook preflight and preserve bounded parallel execution and executor shutdown") {
    val suite = new NotebookFixture() {}
    assert(suite.calls.isEmpty)
    assert(suite.fabricWorkspaceId.isEmpty)
    assert(suite.testNames == Set("one.py", "two.py", "three.py", "four.py"))
    val events = executeSuite(suite)
    assert(events.collect { case event: TestFailed => event }.isEmpty)
    assert(events.count(_.isInstanceOf[TestSucceeded]) == 4)
    val calls = suite.calls.iterator().asScala.toVector
    assert(calls.take(3) == Vector("cleanup", "store", "executor"))
    assert(calls.drop(3).toSet == suite.testNames)
    assert(calls.size == 7)
    assert(suite.peak.get() == FabricNotebookTests.MaxConcurrency)
    assert(suite.active.get() == 0)
    assert(suite.executorService.isTerminated)
  }

  test("Cache notebook preflight failure and never initialize stores, submissions, or an executor") {
    val failure = new IllegalStateException("cleanup failed")
    val suite = new NotebookFixture(Some(failure)) {}
    val events = executeSuite(suite)
    val failures = events.collect { case event: TestFailed => event }
    assert(failures.size == 4)
    assert(failures.forall(_.throwable.contains(failure)))
    assert(suite.calls.iterator().asScala.toVector == Vector("cleanup"))
    assert(suite.active.get() == 0)
  }

  test("Cache interrupted preflight and restore interrupt status on each access") {
    val failure = new InterruptedException("cleanup interrupted")
    var attempts = 0
    val suite = new FabricSmokeTests {
      override lazy val fabric: Nothing = throw new IllegalStateException("Unexpected Fabric connection")
      override protected def cleanupStaleArtifacts(): Unit = {
        attempts += 1
        throw failure
      }
    }
    try {
      (1 to 2).foreach { _ =>
        Thread.interrupted()
        assert(intercept[InterruptedException](suite.storeArtifactId) eq failure)
        assert(Thread.currentThread().isInterrupted)
      }
      assert(attempts == 1)
    } finally {
      Thread.interrupted()
    }
  }

  test("Cache failed store allocation instead of retrying it for each notebook") {
    val failure = new IllegalStateException("store allocation failed")
    val suite = new NotebookFixture() {
      override protected def createTrackedStore(): String = {
        calls.add("store")
        throw failure
      }
    }
    val failures = executeSuite(suite).collect { case event: TestFailed => event }
    assert(failures.size == 4)
    assert(failures.forall(_.throwable.contains(failure)))
    assert(intercept[IllegalStateException](suite.storeArtifactId) eq failure)
    assert(suite.calls.iterator().asScala.toVector == Vector("cleanup", "store"))
  }

  test("Cache failed executor setup before submitting notebooks") {
    val failure = new IllegalStateException("executor setup failed")
    val suite = new NotebookFixture() {
      override protected def createNotebookExecutor(): ExecutorService = {
        calls.add("executor")
        throw failure
      }
    }
    val failures = executeSuite(suite).collect { case event: TestFailed => event }
    assert(failures.size == 4)
    assert(failures.forall(_.throwable.contains(failure)))
    assert(suite.calls.iterator().asScala.toVector == Vector("cleanup", "store", "executor"))
  }

  private val cleanupNow = Instant.parse("2026-09-18T12:00:00Z")
  private val expiredTime = cleanupNow.minusSeconds(25 * 60 * 60)
  private def cleanupId(n: Int): String = new java.util.UUID(0, n.toLong).toString
  private val staleStore = FabricArtifactCleanup.Item(cleanupId(1),
    "Lakehouse202609160000000123456789abcdef0123456789abcdef", "Lakehouse",
    "SynapseML Test Infra Lakehouse", Some(expiredTime), Some(expiredTime), "Active", Set.empty)
  private val staleJob = FabricArtifactCleanup.Item(cleanupId(2),
    "OnePlusOne-20260916-00-00-00-0123456789abcdef0123456789abcdef", "SparkJobDefinition",
    "Synapse Spark Job Definition SparkJobDefinition", Some(expiredTime), Some(expiredTime), "Active",
    Set(staleStore.id))

  private class CleanupClient(initial: Vector[FabricArtifactCleanup.Item] = Vector(staleStore, staleJob))
    extends FabricArtifactCleanup.Client {
    var items: Vector[FabricArtifactCleanup.Item] = initial
    var deleted: Vector[String] = Vector.empty
    var inventoryReads: Int = 0
    var beforeRead: Int => Unit = (_: Int) => ()
    var removeImmediately: Boolean = true
    var history: Vector[JsValue] = Vector(JsObject(
      "status" -> JsString("Completed"), "endTimeUtc" -> JsString(expiredTime.toString)))
    var jobHistory: Map[String, Vector[JsValue]] = Map.empty
    var schedule: Vector[JsValue] = Vector.empty
    override def inventory(): Vector[FabricArtifactCleanup.Item] = {
      inventoryReads += 1
      beforeRead(inventoryReads)
      items
    }
    override def jobs(id: String): Vector[JsValue] = jobHistory.getOrElse(id, history)
    override def schedules(id: String): Vector[JsValue] = schedule
    def remove(id: String): Unit = {
      items = items.filterNot(_.id == id).map(i => i.copy(references = i.references - id))
    }
    override def delete(id: String): Unit = {
      deleted :+= id
      if (removeImmediately) remove(id)
    }
    def run(dryRun: Boolean = false, pause: () => Unit = () => ()): Vector[String] =
      FabricArtifactCleanup.run(this, cleanupNow, dryRun, pause, _ => ())
  }

  test("Clean expired repository jobs before their lakehouse and confirm each deletion") {
    val client = new CleanupClient()
    assert(client.run() == Vector(staleJob.id, staleStore.id))
    assert(client.items.isEmpty)
  }

  test("Retain items at the exact 24 hour boundary, recently modified items, and unknown timestamps") {
    val boundary = cleanupNow.minusSeconds(24 * 60 * 60)
    Seq(staleJob.copy(created = Some(boundary)), staleJob.copy(updated = Some(boundary)),
      staleJob.copy(created = None), staleJob.copy(updated = None),
      staleJob.copy(state = "Provisioning")).foreach { job =>
      val client = new CleanupClient(Vector(staleStore, job))
      assert(client.run().isEmpty)
    }
    val client = new CleanupClient(Vector(staleStore.copy(created = Some(boundary)), staleJob))
    assert(client.run().isEmpty)
  }

  test("Recognize explicitly owned job definitions, lakehouses, and warehouses") {
    Seq("Lakehouse", "Warehouse").foreach { kind =>
      val store = staleStore.copy(kind = kind, name = staleStore.name.replace("Lakehouse", kind),
        description = FabricArtifactCleanup.Owner)
      val job = staleJob.copy(description = FabricArtifactCleanup.Owner)
      assert(new CleanupClient(Vector(store, job)).run() == Vector(job.id, store.id))
    }
  }

  test("Retain unrelated, ambiguous legacy, and similarly named artifacts") {
    val legacy = staleStore.copy(name = "Lakehouse20260916000000")
    Seq(legacy, staleStore.copy(description = "Customer data"),
      staleStore.copy(name = "LakehouseForManualTesting"),
      staleStore.copy(kind = "Notebook")).foreach { store =>
      val client = new CleanupClient(Vector(store))
      assert(client.run().isEmpty)
    }
    val foreign = staleJob.copy(description = "Another repo", references = Set.empty)
    assert(new CleanupClient(Vector(foreign)).run().isEmpty)
    assert(new CleanupClient(Vector(legacy, staleJob)).run() == Vector(staleJob.id, staleStore.id))
  }

  test("Retain running, recently completed, unknown, and scheduled jobs with their stores") {
    Seq(JsObject("status" -> JsString("InProgress")),
      JsObject("status" -> JsString("Completed"), "endTimeUtc" -> JsString(cleanupNow.toString)),
      JsObject("status" -> JsString("Unknown"), "endTimeUtc" -> JsString(expiredTime.toString)),
      JsObject("status" -> JsString("Completed"))).foreach { job =>
      val client = new CleanupClient()
      client.history = Vector(job)
      assert(client.run().isEmpty)
    }
    Seq(JsObject("enabled" -> JsBoolean(true)), JsObject()).foreach { schedule =>
      val client = new CleanupClient()
      client.schedule = Vector(schedule)
      assert(client.run().isEmpty)
    }
  }

  test("Retain stores and jobs with foreign or unresolved dependents") {
    val foreign = staleJob.copy(id = cleanupId(3), name = "Customer notebook", kind = "Notebook")
    Seq(Vector(staleStore, staleJob, foreign),
      Vector(staleStore.copy(references = Set(cleanupId(4))), staleJob)).foreach { items =>
      assert(new CleanupClient(items).run().isEmpty)
    }
  }

  test("Delete an idle job without deleting its active sibling or their shared store") {
    val active = staleJob.copy(id = cleanupId(3))
    val client = new CleanupClient(Vector(staleStore, staleJob, active))
    client.jobHistory = Map(active.id -> Vector(JsObject("status" -> JsString("InProgress"))))
    assert(client.run() == Vector(staleJob.id))
    assert(client.items.map(_.id).toSet == Set(staleStore.id, active.id))
  }

  test("Poll delayed deletion visibility without resending DELETE") {
    val client = new CleanupClient(Vector(staleJob.copy(references = Set.empty)))
    client.removeImmediately = false
    var pauses = 0
    val deleted = client.run(pause = () => {
      pauses += 1
      if (pauses == 2) client.remove(staleJob.id)
    })
    assert(pauses == 2)
    assert(deleted == Vector(staleJob.id))
    assert(client.deleted == Vector(staleJob.id))
  }

  test("Stop after bounded confirmation retries and never delete the parent after an unconfirmed child") {
    val nextJob = staleJob.copy(id = cleanupId(3))
    val client = new CleanupClient(Vector(staleStore, staleJob, nextJob))
    client.removeImmediately = false
    var pauses = 0
    val error = intercept[IllegalArgumentException](client.run(pause = () => pauses += 1))
    assert(error.getMessage.contains("could not confirm deletion"))
    assert(pauses == 30)
    assert(client.deleted == Vector(staleJob.id))
    assert(client.items.exists(_.id == staleStore.id))
  }

  test("Preserve interrupts, inventory failures, and deletion failures") {
    val interrupted = new CleanupClient()
    interrupted.removeImmediately = false
    intercept[InterruptedException](interrupted.run(pause = () => throw new InterruptedException("stop")))
    assert(interrupted.deleted == Vector(staleJob.id))
    val failed = new CleanupClient() {
      override def delete(id: String): Unit = throw new IllegalStateException("delete denied")
    }
    assert(intercept[IllegalStateException](failed.run()).getMessage == "delete denied")
    val inventoryFailure = new CleanupClient()
    inventoryFailure.beforeRead = n => if (n == 2) throw new IllegalStateException("inventory denied")
    intercept[IllegalStateException](inventoryFailure.run())
    assert(inventoryFailure.deleted.isEmpty)
  }

  test("Preserve deletion errors when later cleanup metadata reads fail") {
    for {
      failedRead <- Seq("inventory", "jobs", "schedules", "confirmation")
      previousFailure <- Seq(false, true)
      reuseFailure <- Seq(false, true)
    } {
      val deletionFailure = new IllegalStateException("delete denied")
      val metadataFailure = if (reuseFailure) deletionFailure else new IllegalStateException("metadata denied")
      val nextJob = staleJob.copy(id = cleanupId(3))
      val lastJob = staleJob.copy(id = cleanupId(4))
      val failedJob = if (previousFailure) nextJob else staleJob
      val attempted = ArrayBuffer.empty[String]
      val client = new CleanupClient(Vector(staleStore, staleJob, nextJob, lastJob)) {
        override def jobs(id: String): Vector[JsValue] = {
          if (id == failedJob.id && failedRead == "jobs") throw metadataFailure
          super.jobs(id)
        }
        override def schedules(id: String): Vector[JsValue] = {
          if (id == failedJob.id && failedRead == "schedules") throw metadataFailure
          super.schedules(id)
        }
        override def delete(id: String): Unit = {
          attempted += id
          if (previousFailure && id == staleJob.id) throw deletionFailure
          super.delete(id)
        }
      }
      val readNumber = (if (previousFailure) 3 else 2) + (if (failedRead == "confirmation") 1 else 0)
      client.beforeRead = n => {
        if (Set("inventory", "confirmation")(failedRead) && n == readNumber) throw metadataFailure
      }
      val thrown = intercept[IllegalStateException](client.run())
      val prior = if (previousFailure) Seq(deletionFailure) else Seq.empty
      val priorAttempts = if (previousFailure) Seq(staleJob.id) else Seq.empty
      assert(thrown eq metadataFailure)
      assert(thrown.getSuppressed.toSeq == prior.filterNot(_ eq metadataFailure))
      assert(attempted == priorAttempts ++ (if (failedRead == "confirmation") Seq(failedJob.id) else Seq.empty))
      assert(client.items.exists(_.id == staleStore.id))
    }
  }

  test("Recheck metadata and consumers immediately before deleting") {
    val changed = new CleanupClient()
    changed.beforeRead = n => if (n == 2) {
      changed.items = Vector(staleStore, staleJob.copy(updated = Some(cleanupNow)))
    }
    assert(changed.run().isEmpty)
    val newConsumer = new CleanupClient()
    newConsumer.beforeRead = n => if (n == 4) {
      newConsumer.items :+= staleJob.copy(id = cleanupId(3), created = Some(cleanupNow))
    }
    assert(newConsumer.run() == Vector(staleJob.id))
  }

  test("Dry runs perform no deletion") {
    val client = new CleanupClient()
    assert(client.run(dryRun = true).isEmpty)
    assert(client.deleted.isEmpty)
    assert(client.items == Vector(staleStore, staleJob))
  }

  test("Retain shared SQL endpoints and delete only a lakehouse with an exclusive managed endpoint") {
    val endpoint = staleJob.copy(id = cleanupId(3), kind = "SQLEndpoint", name = "SQL endpoint")
    val client = new CleanupClient(Vector(staleStore, staleJob, endpoint))
    assert(client.run() == Vector(staleJob.id, staleStore.id))
    assert(!client.deleted.contains(endpoint.id))
    val consumer = staleJob.copy(id = cleanupId(4), kind = "Report", references = Set(endpoint.id))
    val shared = new CleanupClient(Vector(staleStore, staleJob, endpoint, consumer))
    assert(shared.run() == Vector(staleJob.id))
  }

  test("Read every inventory page and reject cross-host, repeated, or malformed pages") {
    val url = "https://example.invalid/items"
    var requested = Vector.empty[String]
    val pages = FabricArtifactCleanup.pages(url, uri => {
      requested :+= uri
      if (uri == url) JsObject("value" -> JsArray(JsNumber(1)), "continuationToken" -> JsString("a+b"))
      else JsArray(JsNumber(2))
    })
    assert(pages == Vector(JsNumber(1), JsNumber(2)))
    assert(requested.last == url + "?continuationToken=a%2Bb")
    Seq("continuationUri", "@odata.nextLink").foreach { field =>
      val result = FabricArtifactCleanup.pages(url, uri =>
        if (uri == url) JsObject("artifacts" -> JsArray(JsNumber(1)), field -> JsString(url + "?page=2"))
        else JsArray(JsNumber(2)))
      assert(result == Vector(JsNumber(1), JsNumber(2)))
    }
    Seq(url, "https://other.invalid/items", "http://example.invalid/items",
      "https://example.invalid/other").foreach { next =>
      intercept[IllegalArgumentException] {
        FabricArtifactCleanup.pages(url, _ =>
          JsObject("value" -> JsArray(), "continuationUri" -> JsString(next)))
      }
    }
    intercept[IllegalArgumentException](FabricArtifactCleanup.pages(url, _ => JsObject()))
    intercept[IllegalArgumentException] {
      FabricArtifactCleanup.pages(url, _ =>
        JsObject("value" -> JsArray(), "continuationToken" -> JsNumber(1)))
    }
    val duplicates = new CleanupClient(Vector(staleStore, staleStore.copy(description = "Conflicting owner")))
    intercept[IllegalArgumentException](duplicates.run())
    assert(duplicates.deleted.isEmpty)
    val identical = new CleanupClient(Vector(staleStore, staleStore))
    assert(identical.run() == Vector(staleStore.id))
  }

  test("Retain stores when active or unknown consumers have no inventory reference edges") {
    val active = new CleanupClient(Vector(staleStore, staleJob.copy(references = Set.empty)))
    active.history = Vector(JsObject("status" -> JsString("InProgress")))
    assert(active.run().isEmpty)
    val foreign = staleJob.copy(kind = "Notebook", name = "Unrelated notebook", references = Set.empty)
    assert(new CleanupClient(Vector(staleStore, foreign)).run().isEmpty)
  }

  test("Confirm concurrent not-found deletions and still clean independent jobs after a deletion fails") {
    val raced = new CleanupClient() {
      override def delete(id: String): Unit = {
        super.delete(id)
        throw new RuntimeException("PowerBIEntityNotFound")
      }
    }
    assert(raced.run() == Vector(staleJob.id, staleStore.id))
    val second = staleJob.copy(id = cleanupId(3))
    val failing = new CleanupClient(Vector(staleStore, staleJob, second)) {
      override def delete(id: String): Unit = {
        if (id == staleJob.id) throw new IllegalStateException("delete denied")
        super.delete(id)
      }
    }
    intercept[IllegalStateException](failing.run())
    assert(failing.deleted == Vector(second.id))
    assert(failing.items.exists(_.id == staleStore.id))
    val failures = new CleanupClient(Vector(staleStore, staleJob, second)) {
      override def delete(id: String): Unit = throw new IllegalStateException(id)
    }
    val error = intercept[IllegalStateException](failures.run())
    assert(error.getMessage == staleJob.id)
    assert(error.getSuppressed.map(_.getMessage).toVector == Vector(second.id))
  }

  test("Retain stores when a not-found deletion remains visible through every confirmation retry") {
    val client = new CleanupClient() {
      override def delete(id: String): Unit = {
        super.delete(id)
        throw new RuntimeException("PowerBIEntityNotFound")
      }
    }
    client.removeImmediately = false
    var pauses = 0
    val error = intercept[IllegalArgumentException](client.run(pause = () => pauses += 1))
    assert(error.getMessage.contains("could not confirm deletion"))
    assert(pauses == 30)
    assert(client.deleted == Vector(staleJob.id))
    assert(client.items == Vector(staleStore, staleJob))
  }

  test("Retain a lakehouse when its managed endpoint is recently updated or has unknown age") {
    val endpoint = staleJob.copy(id = cleanupId(3), kind = "SQLEndpoint", name = "SQL endpoint")
    Seq(endpoint.copy(updated = Some(cleanupNow)),
      endpoint.copy(updated = Some(cleanupNow.minusSeconds(24 * 60 * 60))),
      endpoint.copy(created = None), endpoint.copy(updated = None)).foreach { protectedEndpoint =>
      val client = new CleanupClient(Vector(staleStore, staleJob, protectedEndpoint))
      assert(client.run().isEmpty)
    }
  }

  test("Canonicalize mixed-case artifact IDs and foreign consumer references before checking dependencies") {
    val storeId = "abcdefab-1234-5678-abcd-abcdefabcdef"
    val store = staleStore.copy(id = storeId)
    val foreign = JsObject("objectId" -> JsString("abcdefab-1234-5678-abcd-abcdefabcdee".toUpperCase),
      "displayName" -> JsString("Customer notebook"), "artifactType" -> JsString("Notebook"),
      "artifactRelations" -> JsArray(JsObject("dependentArtifactObjectId" -> JsString(storeId.toUpperCase))),
      "datasetRelations" -> JsNull, "dataflowRelations" -> JsNull, "datamartRelations" -> JsNull)
    val consumer = FabricArtifactCleanup.item(foreign)
    assert(consumer.id == "abcdefab-1234-5678-abcd-abcdefabcdee")
    assert(consumer.references == Set(storeId))
    assert(new CleanupClient(Vector(store, consumer)).run().isEmpty)
    val nested = JsObject(foreign.fields.updated("artifactRelations", JsArray(
      JsObject("artifactObjectId" -> JsString(cleanupId(4)),
        "dependencies" -> JsArray(JsString(storeId.toUpperCase))))))
    assert(FabricArtifactCleanup.item(nested).references == Set(storeId, cleanupId(4)))
  }

  test("Parse only artifact metadata, require known relation shapes, and interpret unzoned timestamps as UTC") {
    val metadata = JsObject("objectId" -> JsString(staleJob.id), "displayName" -> JsString(staleJob.name),
      "artifactType" -> JsString(staleJob.kind), "description" -> JsString(staleJob.description),
      "createdDate" -> JsString("2026-09-17T11:00:00"), "lastUpdatedDate" -> JsString(expiredTime.toString),
      "provisionState" -> JsString("Active"), "artifactRelations" -> JsNull, "datasetRelations" -> JsNull,
      "dataflowRelations" -> JsNull, "datamartRelations" -> JsNull,
      "extendedProperties" -> JsObject("DefaultLakehouseArtifactId" -> JsString(staleStore.id)),
      "workloadPayload" -> JsString("must not inspect execution configuration"))
    assert(FabricArtifactCleanup.item(metadata) == staleJob)
    val offset = JsObject(metadata.fields.updated("createdDate", JsString("2026-09-17T13:00:00+02:00")))
    assert(FabricArtifactCleanup.item(offset) == staleJob)
    intercept[IllegalArgumentException] {
      FabricArtifactCleanup.item(JsObject(metadata.fields - "artifactRelations"))
    }
    intercept[IllegalArgumentException] {
      FabricArtifactCleanup.item(JsObject(metadata.fields.updated("artifactRelations",
        JsArray(JsObject("unknown" -> JsString("not-an-id"))))))
    }
    intercept[IllegalArgumentException] {
      FabricArtifactCleanup.item(JsObject(metadata.fields.updated("parentArtifactObjectId", JsNumber(1))))
    }
  }

  test("Reject mixed valid and malformed relation metadata before any artifact deletion") {
    val relationFields = Seq("artifactRelations", "datasetRelations", "dataflowRelations", "datamartRelations")
    val malformed = Seq[JsValue](JsString(staleStore.id + " "), JsString("not-an-id"), JsNumber(1),
      JsBoolean(false), JsNull, JsObject(), JsArray(),
      JsObject("nestedId" -> JsNumber(1)), JsArray(JsString(cleanupId(4)), JsNull))
    for (field <- relationFields; invalid <- malformed) {
      val relation = JsObject("artifactObjectId" -> JsString(cleanupId(4)),
        "dependentArtifactObjectId" -> invalid)
      val foreign = JsObject(Map[String, JsValue](
        "objectId" -> JsString(cleanupId(3)), "displayName" -> JsString("Customer notebook"),
        "artifactType" -> JsString("Notebook")) ++ relationFields.map(_ -> JsNull) +
        (field -> JsArray(relation)))
      val client = new CleanupClient(Vector(staleStore)) {
        override def inventory(): Vector[FabricArtifactCleanup.Item] =
          super.inventory() :+ FabricArtifactCleanup.item(foreign)
      }
      val error = intercept[IllegalArgumentException](client.run())
      assert(error.getMessage.contains(field))
      assert(client.deleted.isEmpty)
      assert(client.items == Vector(staleStore))
    }
  }

  test("Delete tracked artifacts in reverse creation order") {
    val deleted = ArrayBuffer.empty[String]
    val tracker = new FabricTestArtifactTracker(artifactId => {
      deleted += artifactId
      ()
    })

    tracker.track("store")
    tracker.track("job-1")
    tracker.track("job-2")
    tracker.cleanup()

    assert(deleted == Seq("job-2", "job-1", "store"))
  }

  test("Release each completed job before the next artifact allocation") {
    val live = scala.collection.mutable.Set("store")
    val deleted = ArrayBuffer.empty[String]
    val tracker = new FabricTestArtifactTracker(artifactId => {
      assert(live.remove(artifactId))
      deleted += artifactId
      ()
    })
    tracker.track("store")

    (1 to 6).foreach { index =>
      assert(live.size < 2, "Workspace artifact quota exhausted")
      val artifactId = s"job-$index"
      live += artifactId
      val result = tracker.withArtifact(artifactId) { id =>
        assert(live == Set("store", id))
        s"completed-$id"
      }
      assert(result == s"completed-$artifactId")
      assert(live == Set("store"))
    }

    tracker.cleanup()
    assert(live.isEmpty)
    assert(deleted == (1 to 6).map(index => s"job-$index") :+ "store")
  }

  test("Release failed jobs and preserve the original failure") {
    val deleted = ArrayBuffer.empty[String]
    val tracker = new FabricTestArtifactTracker(id => {
      deleted += id
      ()
    })
    val failure = new IllegalStateException("job failed")
    val thrown = intercept[IllegalStateException] {
      tracker.withArtifact("job") { _ => throw failure }
    }
    assert(thrown eq failure)
    assert(deleted == Seq("job"))
    tracker.cleanup()
    assert(deleted == Seq("job"))
  }

  test("Retain unsuccessful deletions for final cleanup without masking job failure") {
    val jobFailure = new IllegalStateException("job failed")
    val cleanupFailure = new IllegalStateException("delete failed")
    var attempts = 0
    val tracker = new FabricTestArtifactTracker(_ => {
      attempts += 1
      if (attempts == 1) throw cleanupFailure
    })

    val thrown = intercept[IllegalStateException] {
      tracker.withArtifact("job") { _ => throw jobFailure }
    }
    assert(thrown eq jobFailure)
    assert(thrown.getSuppressed.toSeq == Seq(cleanupFailure))
    tracker.cleanup()
    assert(attempts == 2)
  }

  test("Fail successful jobs when artifact cleanup fails") {
    val failure = new IllegalStateException("delete failed")
    val tracker = new FabricTestArtifactTracker(_ => throw failure)
    val thrown = intercept[IllegalStateException] {
      tracker.withArtifact("job") { _ => "completed" }
    }
    assert(thrown eq failure)
  }

  test("Ignore artifacts that were already deleted") {
    val attempted = ArrayBuffer.empty[String]
    val tracker = new FabricTestArtifactTracker(artifactId => {
      attempted += artifactId
      if (artifactId == "missing") {
        throw new RuntimeException("PowerBIEntityNotFound")
      }
    })

    tracker.track("remaining")
    tracker.track("missing")
    tracker.cleanup()

    assert(attempted == Seq("missing", "remaining"))
  }

  test("Recognize only SynapseML Fabric test artifact names") {
    assert(FabricNotebookTests.isTestArtifactName("Lakehouse20260808010917"))
    assert(FabricNotebookTests.isTestArtifactName(
      "Lakehouse202608080109170123456789abcdef0123456789abcdef"))
    assert(FabricNotebookTests.isTestArtifactName(
      "ExploreAlgorithmsRegressionQuickstartTrainRegressor-20260808-01-09-17"))
    assert(FabricNotebookTests.isTestArtifactName(
      "ExploreAlgorithmsRegressionQuickstartTrainRegressor-20260808-01-09-17-" +
        "0123456789abcdef0123456789abcdef"))
    assert(FabricNotebookTests.isTestArtifactName("OnePlusOne-20260808-01-09-17"))
    assert(!FabricNotebookTests.isTestArtifactName("LakehouseForManualTesting"))
    assert(!FabricNotebookTests.isTestArtifactName(
      "Lakehouse20260808010917-not-a-unique-id"))
    assert(!FabricNotebookTests.isTestArtifactName(
      "Lakehouse20260808010917-0123456789abcdef0123456789abcdef"))
    assert(!FabricNotebookTests.isTestArtifactName(
      "ExploreAlgorithmsAdHocNotebook-20260808-01-09-17"))
    assert(!FabricNotebookTests.isTestArtifactName("CustomerNotebook-20260808-01-09-17"))
  }

  test("Wait for notebook tasks before artifact cleanup") {
    val executor = Executors.newSingleThreadExecutor()
    val completed = new CountDownLatch(1)
    try {
      executor.submit(new Runnable {
        override def run(): Unit = completed.countDown()
      })

      FabricNotebookTests.shutdownExecutor(executor)

      assert(completed.await(0, TimeUnit.SECONDS))
      assert(executor.isTerminated)
    } finally {
      executor.shutdownNow()
    }
  }

  test("Interrupt notebook tasks that do not stop gracefully") {
    val executor = Executors.newSingleThreadExecutor()
    val started = new CountDownLatch(1)
    val interrupted = new CountDownLatch(1)
    try {
      executor.submit(new Runnable {
        override def run(): Unit = {
          started.countDown()
          try {
            new CountDownLatch(1).await()
          } catch {
            case _: InterruptedException => interrupted.countDown()
          }
        }
      })

      assert(started.await(5, TimeUnit.SECONDS))
      FabricNotebookTests.shutdownExecutor(executor, 1, TimeUnit.SECONDS)

      assert(interrupted.await(0, TimeUnit.SECONDS))
      assert(executor.isTerminated)
    } finally {
      executor.shutdownNow()
    }
  }

  test("Attempt artifact cleanup after executor shutdown fails") {
    val shutdownFailure = new RuntimeException("shutdown failed")
    val cleanupFailure = new RuntimeException("cleanup failed")
    var cleanupAttempted = false

    val thrown = intercept[RuntimeException] {
      FabricNotebookTests.shutdownAndCleanup(
        throw shutdownFailure,
        {
          cleanupAttempted = true
          throw cleanupFailure
        })
    }

    assert(cleanupAttempted)
    assert(thrown eq shutdownFailure)
    assert(thrown.getSuppressed.toSeq == Seq(cleanupFailure))
  }

  test("Attempt artifact cleanup after executor shutdown is interrupted") {
    var cleanupAttempted = false
    try {
      val thrown = intercept[InterruptedException] {
        FabricNotebookTests.shutdownAndCleanup(
          throw new InterruptedException("shutdown interrupted"),
          {
            cleanupAttempted = true
          })
      }

      assert(cleanupAttempted)
      assert(thrown.getMessage == "shutdown interrupted")
      assert(Thread.currentThread().isInterrupted)
    } finally {
      Thread.interrupted()
    }
  }
}
