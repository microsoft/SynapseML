// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.nio.charset.StandardCharsets
import java.util.{Collections, UUID}
import scala.util.control.NonFatal

private[ai] case class AIPromptManifest(runId: String,
                                      libraryVersion: String,
                                      sparkVersion: String,
                                      fingerprint: String,
                                      inputSchema: String,
                                      outputSchema: String,
                                      bindings: String,
                                      keyColumn: String,
                                      state: String,
                                      inputVersion: Option[Long],
                                      inputCount: Option[Long],
                                      owner: Option[String]) {
  def inputType: StructType = AIPromptStorageCodec.readSchema(inputSchema)
  def outputType: StructType = AIPromptStorageCodec.readSchema(outputSchema)
  def keyed(schema: StructType): StructType =
    StructType(StructField(keyColumn, LongType) +: schema.fields.toSeq)
}

/** SQL transactions arbitrate control transitions; releasing an owner requires stopped prior execution. */
private[ai] object AIPromptDelta {
  val FormatVersion: Int = 2
  private val ByteMask = 0xff
  private val IncompleteInitialization =
    "AIPrompt initialization is incomplete and may still be running in the creating application. " +
      "Status, output reads and resume are unavailable. Wait for initialization to finish, or confirm the " +
      "creating application has stopped before abandoning this path. Only after stopped execution is confirmed, " +
      "correct any input errors and use a new outputPath or clean up this run."
  val ControlSchema: StructType = StructType(Seq(
    StructField("formatVersion", IntegerType), StructField("runId", StringType),
    StructField("libraryVersion", StringType), StructField("sparkVersion", StringType),
    StructField("fingerprint", StringType), StructField("inputSchema", StringType),
    StructField("outputSchema", StringType), StructField("bindings", StringType),
    StructField("keyColumn", StringType), StructField("state", StringType),
    StructField("inputVersion", LongType), StructField("inputCount", LongType),
    StructField("owner", StringType)))

  def validatePath(path: String): Unit = {
    require(Option(path).exists(_.trim.nonEmpty), "AIPrompt outputPath must not be empty.")
    require(!path.contains("${"), "AIPrompt outputPath must not contain SQL variable substitutions (${...}).")
  }

  def qualify(spark: SparkSession, path: String): String = {
    validatePath(path)
    val qualified = org.apache.spark.ml.Serializer.makeQualifiedPath(spark, path).toString
    validatePath(qualified)
    qualified
  }

  def requireRuntime(spark: SparkSession, path: String): Unit = {
    try {
      Class.forName("org.apache.spark.sql.delta.sources.DeltaDataSource", false,
        Thread.currentThread().getContextClassLoader)
    } catch {
      case NonFatal(e) => throw new IllegalStateException(
        "AIPrompt durable output requires a compatible, caller-configured Delta runtime.", e)
    }
    require(spark.conf.getOption("spark.sql.extensions").exists(_.contains("DeltaSparkSessionExtension")) &&
      spark.conf.getOption("spark.sql.catalog.spark_catalog").exists(_.contains("DeltaCatalog")),
      "Configure the Delta Spark extension and catalog before creating the Spark session.")
    val uri = new Path(path).toUri
    val scheme = uri.getScheme
    require(Set("file", "hdfs", "abfs", "abfss").contains(scheme),
      "AIPrompt requires HDFS or ABFS storage, or same-host file: storage in Spark local mode. " +
        "Other filesystem/LogStore configurations are not qualified.")
    require(scheme != "file" || (spark.sparkContext.isLocal && Option(uri.getAuthority).forall(_.isEmpty)),
      "AIPrompt file: durable storage requires Spark local mode and a same-host local directory. " +
        "File URI authorities are not supported; use HDFS or ABFS storage on a non-local cluster.")
  }

  def child(path: String, name: String): String = new Path(new Path(path), name).toString

  def table(path: String, name: String): String = "delta.`" + child(path, name).replace("`", "``") + "`"

  def hex(bytes: Array[Byte]): String = bytes.map(b => f"${b & ByteMask}%02x").mkString

  def literal(value: String): String = s"CAST(unhex('${hex(value.getBytes(StandardCharsets.UTF_8))}') AS STRING)"

  def column(name: String): org.apache.spark.sql.Column = AIColumnPlan.column(name)

  def version(spark: SparkSession, path: String, name: String): Long =
    spark.sql(s"DESCRIBE HISTORY ${table(path, name)} LIMIT 1").select("version").head().getLong(0)

  def read(spark: SparkSession, path: String, name: String, version: Long): DataFrame =
    spark.read.format("delta").option("versionAsOf", version).load(child(path, name))

  def latest(spark: SparkSession, path: String, name: String): (Long, DataFrame) = {
    val pinned = version(spark, path, name)
    pinned -> read(spark, path, name, pinned)
  }

  def create(spark: SparkSession, path: String, manifest: AIPromptManifest,
             beforeCreate: () => Unit): Unit = {
    val root = new Path(path)
    val fs = root.getFileSystem(spark.sessionState.newHadoopConf())
    def uninitialized: Boolean = !fs.exists(root) || (fs.getFileStatus(root).isDirectory &&
      fs.listStatus(root).forall(entry => root.toUri.getScheme == "file" && entry.isFile &&
        entry.getPath.getName == AIPromptLocalTransaction.LockFile))
    if (!uninitialized) {
      requireReady(control(spark, path))
      throw new IllegalArgumentException("AIPrompt transform requires a new or empty run directory; " +
        "use loadOutput to read committed outcomes or resume to continue an existing run.")
    }
    val row = Row(FormatVersion, manifest.runId, manifest.libraryVersion, manifest.sparkVersion,
      manifest.fingerprint, manifest.inputSchema,
      manifest.outputSchema, manifest.bindings, manifest.keyColumn, manifest.state,
      manifest.inputVersion.map(Long.box).orNull, manifest.inputCount.map(Long.box).orNull,
      manifest.owner.orNull)
    beforeCreate()
    AIPromptLocalTransaction(path) {
      if (root.toUri.getScheme == "file") {
        require(uninitialized,
          "AIPrompt initial ownership conflict: another creator has initialized this run.")
      }
      // The Delta create arbitrates remote writers; local rename-based LogStores also need the kernel lock.
      spark.createDataFrame(Collections.singletonList(row), ControlSchema)
        .write.format("delta").mode("errorifexists").save(child(path, "_control"))
      val committed = control(spark, path)
      require(committed.runId == manifest.runId && committed.owner == manifest.owner,
        "AIPrompt initial ownership conflict.")
    }
  }

  def control(spark: SparkSession, path: String): AIPromptManifest = {
    val root = new Path(path)
    val fs = root.getFileSystem(spark.sessionState.newHadoopConf())
    require(fs.exists(root), "No AIPrompt run exists at outputPath. Check the path supplied to this operation.")
    require(fs.isDirectory(root) && fs.isDirectory(new Path(root, "_control")),
      "outputPath is not an AIPrompt run directory. Check the path; do not remove unrelated data. " +
        "For a new run, choose a new or empty outputPath.")
    val rows = try {
      latest(spark, path, "_control")._2.select(ControlSchema.fieldNames.map(column): _*).take(2)
    } catch {
      case error: AnalysisException if error.getErrorClass == "DELTA_TABLE_ONLY_OPERATION" =>
        throw new IllegalArgumentException(IncompleteInitialization, error)
    }
    require(rows.length == 1, "Corrupt AIPrompt control table: expected exactly one row.")
    val row = rows.head
    require(row.getAs[Int]("formatVersion") == FormatVersion, "Unsupported AIPrompt output format version.")
    require(Seq("runId", "libraryVersion", "sparkVersion", "fingerprint", "bindings", "keyColumn",
      "inputSchema", "outputSchema").forall { name =>
      Option(row.getAs[String](name)).exists(_.trim.nonEmpty)
    }, "Corrupt AIPrompt control table: required identity fields must not be null or empty.")
    val manifest = AIPromptManifest(row.getAs[String]("runId"),
      row.getAs[String]("libraryVersion"), row.getAs[String]("sparkVersion"), row.getAs[String]("fingerprint"),
      row.getAs[String]("inputSchema"), row.getAs[String]("outputSchema"), row.getAs[String]("bindings"),
      row.getAs[String]("keyColumn"), row.getAs[String]("state"),
      Option(row.getAs[java.lang.Long]("inputVersion")).map(_.longValue),
      Option(row.getAs[java.lang.Long]("inputCount")).map(_.longValue),
      Option(row.getAs[String]("owner")))
    require(manifest.owner.forall(_.trim.nonEmpty),
      "Corrupt AIPrompt control table: owner must be null or non-blank.")
    require(Set("initializing", "running", "complete").contains(manifest.state),
      "Corrupt AIPrompt run state.")
    manifest.inputType
    manifest.outputType
    manifest
  }

  def requireReady(manifest: AIPromptManifest): Unit =
    require(manifest.state != "initializing" && manifest.inputVersion.isDefined && manifest.inputCount.isDefined,
      IncompleteInitialization)

  def requireUnowned(manifest: AIPromptManifest): Unit =
    require(manifest.owner.isEmpty,
      "AIPrompt run already has an owner. Stopped-job administrative release is required.")

  private def update(spark: SparkSession, path: String, assignments: String, condition: String): Unit = {
    AIPromptLocalTransaction(path) {
      val changed = spark.sql(s"UPDATE ${table(path, "_control")} SET $assignments WHERE $condition").collect()
      require(changed.length == 1 && changed.head.getLong(0) == 1L,
        "AIPrompt ownership conflict: the expected owner no longer owns this run.")
    }
  }

  private def owned(manifest: AIPromptManifest, token: String): String =
    s"runId = ${literal(manifest.runId)} AND owner = ${literal(token)}"

  def assertOwner(spark: SparkSession, path: String, manifest: AIPromptManifest, token: String): Unit = {
    val current = control(spark, path)
    require(current.runId == manifest.runId && current.owner.contains(token),
      "AIPrompt ownership conflict: inference and writes are stopped.")
  }

  def ready(spark: SparkSession, path: String, manifest: AIPromptManifest, token: String,
            inputVersion: Long, count: Long): AIPromptManifest = {
    update(spark, path, s"state = 'running', inputVersion = $inputVersion, inputCount = $count",
      owned(manifest, token) + " AND state = 'initializing'")
    control(spark, path)
  }

  def acquire(spark: SparkSession, path: String, manifest: AIPromptManifest): String = {
    requireReady(manifest)
    require(manifest.state != "complete", "A completed AIPrompt run does not require ownership.")
    requireUnowned(manifest)
    val token = UUID.randomUUID().toString
    update(spark, path, s"owner = ${literal(token)}, state = 'running'",
      s"runId = ${literal(manifest.runId)} AND owner IS NULL AND state = ${literal(manifest.state)}")
    assertOwner(spark, path, manifest, token)
    token
  }

  def complete(spark: SparkSession, path: String, manifest: AIPromptManifest, token: String): Unit =
    update(spark, path, "state = 'complete', owner = NULL", owned(manifest, token))

  def release(spark: SparkSession, path: String, manifest: AIPromptManifest, token: String): Unit =
    update(spark, path, "owner = NULL", owned(manifest, token))

  def validateSchema(frame: DataFrame, expected: StructType): Unit =
    require(AIPromptStorageCodec.nullable(frame.schema) == AIPromptStorageCodec.physical(expected),
      "Corrupt or incompatible AIPrompt Delta schema; schema evolution is not supported.")

  def input(spark: SparkSession, path: String, manifest: AIPromptManifest): DataFrame = {
    requireReady(manifest)
    require(version(spark, path, "_input") == manifest.inputVersion.get,
      "The immutable AIPrompt input snapshot has been modified. After initialization, " +
        "do not OPTIMIZE or VACUUM the private _input table; disable automatic maintenance for it.")
    val frame = read(spark, path, "_input", manifest.inputVersion.get)
    validateSchema(frame, manifest.keyed(manifest.inputType))
    frame
  }

  def keyCount(frame: DataFrame, key: String): Long = {
    val counts = frame.agg(count(lit(1)), count(column(key)), countDistinct(column(key))).head()
    val total = counts.getLong(0)
    require(total == counts.getLong(1) && total == counts.getLong(2),
      "Corrupt AIPrompt row identity: keys must be unique and non-null.")
    total
  }
}
