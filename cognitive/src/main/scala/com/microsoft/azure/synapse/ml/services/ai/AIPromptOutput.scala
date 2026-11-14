// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import com.microsoft.azure.synapse.ml.build.BuildInfo
import com.microsoft.azure.synapse.ml.io.http.ErrorUtils
import com.microsoft.azure.synapse.ml.services.openai.UsageUtils
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConfInjections
import org.apache.spark.sql.types.{DataType, StringType, StructType}
import spray.json._

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.{Collections, Locale, UUID}
import scala.annotation.tailrec
import scala.util.Try
import scala.util.control.NonFatal

/** Counts describe terminal outcomes, not exactly-once execution of external model requests.
  * `isComplete` means every input row has a committed outcome; `allSucceeded` additionally requires no row errors.
  * Both are count-based: the last batch may be committed while `state` is still `running` and an owner remains.
  * Neither flag proves task quiescence or permits owner release. Stop the previous application and confirm its
  * work has quiesced before releasing its exact owner token; resume then finalizes without replaying committed rows.
  */
case class AIPromptOutputStatus(formatVersion: Int,
                                runId: String,
                                state: String,
                                inputCount: Long,
                                committedCount: Long,
                                pendingCount: Long,
                                rowErrorCount: Long,
                                ownerToken: Option[String],
                                resultsVersion: Long) {
  def isComplete: Boolean = pendingCount == 0L
  def allSucceeded: Boolean = isComplete && rowErrorCount == 0L

  def toJson: String = JsObject(
    "formatVersion" -> JsNumber(formatVersion), "runId" -> JsString(runId), "state" -> JsString(state),
    "inputCount" -> JsNumber(inputCount), "committedCount" -> JsNumber(committedCount),
    "pendingCount" -> JsNumber(pendingCount), "rowErrorCount" -> JsNumber(rowErrorCount),
    "ownerToken" -> ownerToken.map(JsString(_)).getOrElse(JsNull), "resultsVersion" -> JsNumber(resultsVersion),
    "isComplete" -> JsBoolean(isComplete), "allSucceeded" -> JsBoolean(allSucceeded)).compactPrint

  override def toString: String = toJson
}

private[ai] object AIPromptOutput {
  private val OutputParams = Set("outputCol", "errorCol", "usageCol", "rawResponseCol", "responseIdCol")
  private val SavedBindings = OutputParams + "messagesCol"
  private val TypedOutputs: Map[String, DataType] = Map(
    "errorCol" -> ErrorUtils.ErrorSchema, "usageCol" -> UsageUtils.UsageStructType,
    "rawResponseCol" -> StringType, "responseIdCol" -> StringType)
  private val AuthParams = Set("subscriptionKey", "AADToken", "CustomAuthHeader")
  private val StorageParams = Set("outputPath", "outputBatchSize")
  private val AutomaticBatchPartitions = 200
  private val SQLSemanticKeys = Set(
    "spark.sql.session.timeZone", "spark.sql.caseSensitive", "spark.sql.timestampType",
    "spark.sql.mapKeyDedupPolicy", "spark.sql.storeAssignmentPolicy",
    "spark.sql.columnNameOfCorruptRecord", "spark.sql.chunkBase64String.enabled",
    "spark.sql.ansi.enabled", "spark.sql.legacy.sizeOfNull")
  private val SQLSemanticPrefixes = Seq(
    "spark.sql.ansi.", "spark.sql.legacy.", "spark.sql.parser.", "spark.sql.function.",
    "spark.sql.json.", "spark.sql.csv.", "spark.sql.jsonGenerator.", "spark.sql.decimalOperations.")
  private val IgnoreEvent: String => Unit = _ => ()

  private def enabledOutput(prompt: AIPrompt, name: String): Boolean = name match {
    case "usageCol" => prompt.isSet(prompt.usageCol)
    case "rawResponseCol" => prompt.isSet(prompt.rawResponseCol)
    case "responseIdCol" => prompt.responseIdEnabled
    case _ => true
  }

  def storageSchema(schema: StructType): StructType = {
    AIPromptStorageCodec.validate(schema)
    AIPromptStorageCodec.validateFieldResolution(schema)
    AIPromptStorageCodec.durable(schema).asInstanceOf[StructType]
  }

  private def bindings(prompt: AIPrompt): Map[String, String] =
    prompt.extractParamMap().toSeq.filter(p => SavedBindings(p.param.name) && enabledOutput(prompt, p.param.name))
      .map(p => p.param.name -> p.value.asInstanceOf[String]).toMap

  private def readBindings(manifest: AIPromptManifest): Map[String, String] = {
    val bindings = AIJson.objectString(manifest.bindings).fields.map { case (name, value) =>
      require(SavedBindings(name), "Unknown AIPrompt stored output binding.")
      val column = value match {
        case JsString(text) if text.nonEmpty => text
        case _ => throw new IllegalArgumentException("Invalid AIPrompt stored output binding.")
      }
      name -> column
    }
    require(Set("outputCol", "errorCol", "messagesCol").subsetOf(bindings.keySet),
      "AIPrompt stored output bindings are incomplete.")
    val outputs = bindings.filter { case (role, _) => OutputParams(role) }
    require(outputs.values.map(_.toLowerCase(Locale.ROOT)).toSeq.distinct.size == outputs.size,
      "AIPrompt stored output bindings must be distinct.")
    val fields = manifest.outputType.fields.map(field => field.name -> field.dataType).toMap
    outputs.foreach { case (role, column) =>
      require(fields.contains(column), "AIPrompt stored output binding is absent from the output schema.")
      require(TypedOutputs.get(role).forall(dataType =>
        AIPromptStorageCodec.nullable(dataType) == AIPromptStorageCodec.nullable(fields(column))),
        "AIPrompt stored output binding has an incompatible type.")
    }
    bindings
  }

  // Escape code units before UTF-8 hashing, which would otherwise replace unpaired surrogates with '?'.
  private def quotedString(value: String): String = JsString(value).compactPrint.flatMap {
    case character if Character.isSurrogate(character) => f"\\u${character.toInt}%04x"
    case character => character.toString
  }

  private def canonical(json: JsValue): String = json match {
    case JsObject(fields) => fields.toSeq.sortBy(_._1)
      .map { case (name, value) => quotedString(name) + ":" + canonical(value) }.mkString("{", ",", "}")
    case JsArray(values) => values.map(canonical).mkString("[", ",", "]")
    case JsString(text) => quotedString(text)
    case other => other.compactPrint
  }

  private def orderedJsonIdentity(json: JsValue): String = json match {
    case JsObject(fields) => fields.iterator
      .map { case (name, value) => JsString(name).compactPrint + ":" + orderedJsonIdentity(value) }
      .mkString("{", ",", "}")
    case JsArray(values) => values.map(orderedJsonIdentity).mkString("[", ",", "]")
    case JsNumber(number) if number.bigDecimal.scale == 0 => number.toString
    case JsNumber(number) if number.signum == 0 => "0e0"
    case JsNumber(number) =>
      val digits = number.bigDecimal.unscaledValue.toString
      val significant = digits.reverse.dropWhile(_ == '0').reverse
      // Avoid stripTrailingZeros scale overflow and expansion of large exponents.
      val exponent = digits.length - significant.length - number.bigDecimal.scale.toLong
      significant + "e" + exponent
    case other => other.compactPrint
  }

  private def credentialValue(value: Any): Option[JsValue] = value match {
    case Right(binding: String) => Some(JsString(binding))
    case _ => None
  }

  private def headerValue(param: Param[Any], value: Any, authHeaders: Set[String]): Option[JsValue] = value match {
    case Left(headers: scala.collection.Map[_, _]) =>
      val filtered = headers.toSeq.collect {
        case (key: String, v: String) if !authHeaders.exists(_.equalsIgnoreCase(key)) =>
          key -> JsString(v)
      }.toMap
      if (filtered.isEmpty) None else Some(JsObject(filtered))
    case _ => Some(param.jsonEncode(value).parseJson)
  }

  private def paramValue(param: Param[Any], value: Any, authHeaders: Set[String]): Option[JsValue] = param.name match {
    case "handler" => Some(JsString(AIPrompt.DefaultHttpPolicy))
    case name if AuthParams(name) => credentialValue(value)
    case "customHeaders" | "telemHeaders" => headerValue(param, value, authHeaders)
    // Preserve provider-owned object order and integer/decimal kind, not equivalent decimal spelling.
    case "responseFormat" | "extraBody" | "questions" =>
      val encoded = value match {
        case Left(json) => orderedJsonIdentity(JsObject("left" -> AIJson.fromAny(json)))
        case _ => param.jsonEncode(value)
      }
      Some(JsString(encoded))
    case _ => Some(param.jsonEncode(value).parseJson)
  }

  private def sqlSemantics(spark: SparkSession): JsObject = {
    val registered = SQLConfInjections.registeredKeys
    val configured = spark.conf.getAll.keys
    val keys = (registered ++ configured ++ SQLSemanticKeys).filter { key =>
      SQLSemanticKeys(key) || SQLSemanticPrefixes.exists(key.startsWith)
    }.toSet
    JsObject(keys.map(key => key -> spark.conf.getOption(key).map(JsString(_)).getOrElse(JsNull)).toMap)
  }

  private[ai] def validateSettings(prompt: AIPrompt): Unit = {
    if (prompt.isDefined(prompt.outputPath)) AIPromptDelta.validatePath(prompt.getOutputPath)
    require(AIPrompt.usesBoundedHandler(prompt),
      "AIPrompt durable output requires the default HTTP handler; custom executable handlers cannot be fingerprinted.")
    batchSize(prompt)
  }

  private[ai] def fingerprint(prompt: AIPrompt, input: StructType, output: StructType,
                              spark: SparkSession): String = {
    validateSettings(prompt)
    // Library-owned defaults participate too; an upgrade can change recovery identity.
    val config = prompt.extractParamMap().toSeq
      .filter(p => !StorageParams(p.param.name) && enabledOutput(prompt, p.param.name)).flatMap { pair =>
      val param = pair.param.asInstanceOf[Param[Any]]
      paramValue(param, pair.value, prompt.authenticationHeaderNames).map { json =>
        val explicit = !OutputParams(param.name) && prompt.isSet(param)
        param.name -> JsObject("value" -> json, "explicit" -> JsBoolean(explicit))
      }
    }.toMap
    val content = canonical(JsObject(
      "format" -> JsNumber(AIPromptDelta.FormatVersion),
      "libraryVersion" -> JsString(BuildInfo.version),
      "sparkVersion" -> JsString(spark.version),
      "stageClass" -> JsString(prompt.getClass.getName),
      "authenticationConfigured" -> JsBoolean(AIPrompt.hasConfiguredAuthentication(prompt)),
      "params" -> JsObject(config),
      "sqlSemantics" -> sqlSemantics(spark),
      "input" -> AIPromptStorageCodec.schemaJson(input).parseJson,
      "output" -> AIPromptStorageCodec.schemaJson(output).parseJson))
    AIPromptDelta.hex(MessageDigest.getInstance("SHA-256").digest(content.getBytes(StandardCharsets.UTF_8)))
  }

  private def batchSize(prompt: AIPrompt): Int = {
    val size = prompt.getOutputBatchSize
    require(prompt.outputBatchSize.isValid(size), "AIPrompt outputBatchSize must be between 1 and 100000.")
    size
  }

  private def requireMatchingConfiguration(prompt: AIPrompt, spark: SparkSession,
                                            manifest: AIPromptManifest): Unit = {
    require(manifest.libraryVersion == BuildInfo.version && manifest.sparkVersion == spark.version,
      "AIPrompt resume build or Spark runtime does not match the saved run. " +
        "Restore the original SynapseML build and Spark runtime, or use loadOutput to read committed results.")
    val output = storageSchema(if (manifest.state == "complete") prompt.inferenceOutputSchema(manifest.inputType)
      else prompt.inferenceSchema(manifest.inputType))
    require(fingerprint(prompt, manifest.inputType, output, spark) == manifest.fingerprint,
      "AIPrompt resume configuration, public output schema or Spark SQL semantics do not match the saved run. " +
        "Restore the original SynapseML build, stage class, settings and Spark runtime, " +
        "including library and SQL defaults and whether authentication is configured, before resuming.")
  }

  private[ai] def transformObserved(prompt: AIPrompt, dataset: DataFrame, event: String => Unit): DataFrame =
    AIPrompt.withSession(dataset.sparkSession) {
      val effective = prompt.effectiveCopy()
      effective.inferenceSchema(dataset.schema)
      transformEffective(effective, dataset, event)
    }

  def transformEffective(effective: AIPrompt, dataset: DataFrame,
                         event: String => Unit = IgnoreEvent): DataFrame =
    AIPrompt.withSession(dataset.sparkSession) {
    require(!dataset.isStreaming, "AIPrompt durable output does not support streaming inputs.")
    val spark = dataset.sparkSession
    val path = AIPromptDelta.qualify(spark, effective.getOutputPath)
    AIPromptDelta.requireRuntime(spark, path)
    batchSize(effective)
    val inputSchema = storageSchema(dataset.schema)
    val outputSchema = storageSchema(effective.inferenceSchema(inputSchema))
    effective.validateFabricModel(inputSchema)
    val token = UUID.randomUUID().toString
    val key = "_aiprompt_row_" + UUID.randomUUID().toString.replace("-", "")
    require(!(inputSchema.fieldNames ++ outputSchema.fieldNames).exists(_.equalsIgnoreCase(key)),
      "AIPrompt internal row key conflicts with an input or output column.")
    val manifest = AIPromptManifest(UUID.randomUUID().toString, BuildInfo.version, spark.version,
      fingerprint(effective, inputSchema, outputSchema, spark), AIPromptStorageCodec.schemaJson(inputSchema),
      AIPromptStorageCodec.schemaJson(outputSchema),
      JsObject(bindings(effective).map { case (name, value) => name -> JsString(value) }).compactPrint,
      key, "initializing", None, None, Some(token))
    effective.validateDurableInput(dataset.schema, spark)
    val keyed = dataset.select(monotonically_increasing_id().as(key) +: dataset.columns
      .map(AIPromptDelta.column): _*)
    val encoded = AIPromptStorageCodec.encode(keyed, manifest.keyed(inputSchema))
    AIPromptDelta.create(spark, path, manifest, () => event("before_control_create"))
    event("control_created")
    encoded.write.format("delta").mode("errorifexists").save(AIPromptDelta.child(path, "_input"))
    event("input_committed")
    val ready = initializeResults(effective, spark, path, manifest, token, event)
    execute(effective, spark, path, ready, token, event)
  }

  private def initializeResults(prompt: AIPrompt, spark: SparkSession, path: String, manifest: AIPromptManifest,
                                token: String, event: String => Unit): AIPromptManifest = {
    val (inputVersion, snapshot) = AIPromptDelta.latest(spark, path, "_input")
    AIPromptDelta.validateSchema(snapshot, manifest.keyed(manifest.inputType))
    if (inputVersion > 0L) {
      // The new table starts at version 0; automatic compaction may commit before this read.
      val changes = spark.sql(s"DESCRIBE HISTORY ${AIPromptDelta.table(path, "_input")}")
        .filter(col("version") > 0L && col("version") <= inputVersion &&
          !col("operation").eqNullSafe(lit("OPTIMIZE")))
      require(changes.take(1).isEmpty, "The immutable AIPrompt input snapshot has been modified.")
    }
    prompt.validateDurableRows(
      AIPromptStorageCodec.decode(snapshot, manifest.keyed(manifest.inputType)).drop(manifest.keyColumn))
    event("input_validated")
    val count = AIPromptDelta.keyCount(snapshot, manifest.keyColumn)
    val physical = AIPromptStorageCodec.physical(manifest.keyed(manifest.outputType)).asInstanceOf[StructType]
    spark.createDataFrame(Collections.emptyList[Row](), physical).write.format("delta").mode("errorifexists")
      .save(AIPromptDelta.child(path, "results"))
    event("results_created")
    require(AIPromptDelta.version(spark, path, "_input") == inputVersion,
      "The immutable AIPrompt input snapshot has been modified.")
    val ready = AIPromptDelta.ready(spark, path, manifest, token, inputVersion, count)
    event("ready")
    ready
  }

  def resume(prompt: AIPrompt, spark: SparkSession): DataFrame = resumeObserved(prompt, spark, IgnoreEvent)

  private[ai] def resumeObserved(prompt: AIPrompt, spark: SparkSession, event: String => Unit): DataFrame =
    AIPrompt.withSession(spark) {
    val effective = prompt.effectiveCopy()
    require(effective.isDefined(effective.outputPath), "resume requires outputPath; set the original run directory")
    val path = AIPromptDelta.qualify(spark, effective.getOutputPath)
    AIPromptDelta.requireRuntime(spark, path)
    batchSize(effective)
    val manifest = AIPromptDelta.control(spark, path)
    AIPromptDelta.requireReady(manifest)
    if (manifest.state != "complete") AIPromptDelta.requireUnowned(manifest)
    effective.restoreDurableOutputDefaults(readBindings(manifest))
    requireMatchingConfiguration(effective, spark, manifest)
    if (manifest.state != "complete") {
      val saved = AIPromptDelta.input(spark, path, manifest)
      val decoded = AIPromptStorageCodec.decode(saved, manifest.keyed(manifest.inputType)).drop(manifest.keyColumn)
      effective.validateDurableInput(decoded.schema, spark)
      effective.validateDurableRows(decoded)
    }
    val committed = ledger(spark, path, manifest)
    if (manifest.state == "complete") {
      AIPromptStorageCodec.decode(committed.results.drop(manifest.keyColumn), manifest.outputType)
    } else {
      if (committed.count < manifest.inputCount.get) effective.validateFabricModel(manifest.inputType)
      event("before_acquire")
      val token = AIPromptDelta.acquire(spark, path, manifest)
      event("acquired")
      execute(effective, spark, path, manifest, token, event)
    }
  }

  private case class Ledger(version: Long, results: DataFrame, count: Long)

  private[ai] def batchPartitions(rowCount: Int, shufflePartitions: String): Int = {
    val normalized = shufflePartitions.trim
    val configured = if (normalized == "auto") AutomaticBatchPartitions else {
      Try(normalized.toInt).toOption.filter(_ > 0).getOrElse {
        throw new IllegalArgumentException("spark.sql.shuffle.partitions must be a positive integer or 'auto'.")
      }
    }
    math.min(rowCount, configured)
  }

  private def ledger(spark: SparkSession, path: String, manifest: AIPromptManifest,
                     validateInputKeys: Boolean = true): Ledger = {
    val input = AIPromptDelta.input(spark, path, manifest)
    if (validateInputKeys) {
      require(AIPromptDelta.keyCount(input, manifest.keyColumn) == manifest.inputCount.get,
        "AIPrompt input count does not match the immutable snapshot.")
    }
    val (version, results) = AIPromptDelta.latest(spark, path, "results")
    AIPromptDelta.validateSchema(results, manifest.keyed(manifest.outputType))
    val count = AIPromptDelta.keyCount(results, manifest.keyColumn)
    val key = manifest.keyColumn
    require(results.select(AIPromptDelta.column(key)).join(input.select(AIPromptDelta.column(key)),
      Seq(key), "left_anti").limit(1).count() == 0L, "AIPrompt results contain unknown row keys.")
    require(count <= manifest.inputCount.get && (manifest.state != "complete" || count == manifest.inputCount.get),
      "AIPrompt completion state contradicts committed outcomes.")
    Ledger(version, results, count)
  }

  private def execute(prompt: AIPrompt, spark: SparkSession, path: String, manifest: AIPromptManifest,
                      token: String, event: String => Unit): DataFrame = {
    // Construct inside guarded batch preparation, only if this run has pending requests.
    lazy val http = AIPrompt.createHttpTransformer(prompt)
    val infer = (batch: DataFrame) => prompt.infer(batch, Seq(manifest.keyColumn), http)
    processBatches(prompt, spark, path, manifest, token, event, ledger(spark, path, manifest), infer)
    // Failed Spark jobs and uncertain commits retain ownership; they do not prove task quiescence.
    AIPromptDelta.complete(spark, path, manifest, token)
    event("completed")
    loadOutput(spark, path)
  }

  @tailrec
  private def processBatches(prompt: AIPrompt, spark: SparkSession, path: String, manifest: AIPromptManifest,
                             token: String, event: String => Unit, committed: Ledger,
                             infer: DataFrame => DataFrame): Unit = {
    AIPromptDelta.assertOwner(spark, path, manifest, token)
    val input = AIPromptDelta.input(spark, path, manifest)
    event("before_ledger_version_check")
    require(AIPromptDelta.version(spark, path, "results") == committed.version,
      "AIPrompt committed outcomes changed outside the current owner.")
    val key = manifest.keyColumn
    val keys = input.select(AIPromptDelta.column(key))
      .join(committed.results.select(AIPromptDelta.column(key)), Seq(key), "left_anti")
      .take(batchSize(prompt)).map(_.getLong(0))
    if (keys.nonEmpty) {
      val selectedKeys = keys.map(Long.box)
      val prepared = try {
        requireMatchingConfiguration(prompt, spark, manifest)
        val savedBatch = input.filter(AIPromptDelta.column(key).isin(selectedKeys: _*))
        val partitions = batchPartitions(keys.length, spark.conf.get("spark.sql.shuffle.partitions"))
        val batch = AIPromptStorageCodec.decode(savedBatch, manifest.keyed(manifest.inputType))
          .repartition(partitions)
        val inferred = infer(batch)
        val selected = inferred.select(manifest.keyed(manifest.outputType).fieldNames.map(AIPromptDelta.column): _*)
        AIPromptStorageCodec.encode(selected, manifest.keyed(manifest.outputType))
      } catch {
        case NonFatal(error) if !Thread.currentThread().isInterrupted =>
          // Previous actions finished successfully; this block only builds the next lazy plan.
          try AIPromptDelta.release(spark, path, manifest, token) catch {
            case releaseError: InterruptedException =>
              Thread.currentThread().interrupt()
              error.addSuppressed(releaseError)
            case NonFatal(releaseError) => error.addSuppressed(releaseError)
          }
          throw error
      }
      AIPromptDelta.assertOwner(spark, path, manifest, token)
      event("before_batch_commit")
      prepared.write.format("delta").mode("append").save(AIPromptDelta.child(path, "results"))
      event("batch_committed")
      val after = ledger(spark, path, manifest, validateInputKeys = false)
      require(after.count == committed.count + keys.length &&
        after.results.filter(AIPromptDelta.column(key).isin(selectedKeys: _*)).count() == keys.length,
        "AIPrompt committed batch cardinality does not match the selected input keys.")
      processBatches(prompt, spark, path, manifest, token, event, after, infer)
    }
  }

  private def open(spark: SparkSession, outputPath: String): (String, AIPromptManifest) = {
    val path = AIPromptDelta.qualify(spark, outputPath)
    AIPromptDelta.requireRuntime(spark, path)
    val manifest = AIPromptDelta.control(spark, path)
    AIPromptDelta.requireReady(manifest)
    readBindings(manifest)
    path -> manifest
  }

  def loadOutput(spark: SparkSession, outputPath: String): DataFrame = {
    val (path, manifest) = open(spark, outputPath)
    val committed = ledger(spark, path, manifest)
    AIPromptStorageCodec.decode(committed.results.drop(manifest.keyColumn), manifest.outputType)
  }

  def getOutputStatus(spark: SparkSession, outputPath: String): AIPromptOutputStatus = {
    val (path, manifest) = open(spark, outputPath)
    val committed = ledger(spark, path, manifest)
    val errors = committed.results.filter(AIPromptDelta.column(readBindings(manifest)("errorCol")).isNotNull).count()
    AIPromptOutputStatus(AIPromptDelta.FormatVersion, manifest.runId, manifest.state,
      manifest.inputCount.get, committed.count, manifest.inputCount.get - committed.count,
      errors, manifest.owner, committed.version)
  }

  def releaseOutputOwner(spark: SparkSession, outputPath: String,
                         expectedOwnerToken: String, confirmStopped: Boolean): Unit = {
    require(confirmStopped, "Confirm that the previous Spark application has stopped before releasing its owner.")
    require(Option(expectedOwnerToken).exists(_.nonEmpty), "An exact nonempty expected owner token is required.")
    val path = AIPromptDelta.qualify(spark, outputPath)
    AIPromptDelta.requireRuntime(spark, path)
    val manifest = AIPromptDelta.control(spark, path)
    AIPromptDelta.release(spark, path, manifest, expectedOwnerToken)
  }
}
