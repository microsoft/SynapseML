// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.stages

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import com.microsoft.azure.synapse.ml.param.StringIntMapParam
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.SQLDataTypes._
import org.apache.spark.ml.param._
import org.apache.spark.ml.stat.Summarizer
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, ExprId, RowOrdering}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}

import scala.collection.JavaConverters._
import scala.util.Try

object EnsembleByKey extends DefaultParamsReadable[EnsembleByKey] {

  // Spark's union analysis re-aliases duplicated child outputs and tags them with this key so that
  // AttributeSeq.resolve can prune them before reporting an ambiguous reference.
  private val DuplicateMetadataKey = "__is_duplicate"

  private case class PathStep(name: String, mapKeyType: Option[DataType])

  private case class ResolvedField(
      reference: String,
      qualifier: Array[String],
      path: Array[PathStep],
      ordinals: Array[Int],
      field: StructField)

  private case class ResolvedColumns(
      inputFields: Array[ResolvedField],
      outputNames: Array[String],
      keyFields: Array[ResolvedField],
      aggregateFields: Array[StructField],
      caseSensitive: Boolean)

  private case class ResolvedStep(
      fieldName: String,
      dataType: DataType,
      nullable: Boolean,
      metadata: Metadata,
      ordinal: Int,
      mapKeyType: Option[DataType])

  private case class FieldRole(
      consumesInputColumn: Boolean,
      declaredOutput: StructField => Option[StructField])

  private case class QualifiedMatch(
      qualifier: Array[String],
      requestedPath: Array[String],
      ordinal: Int,
      exprId: ExprId)

  private def columnNamesMatch(left: String, right: String, caseSensitive: Boolean): Boolean =
    if (caseSensitive) left == right else left.equalsIgnoreCase(right)

  private def resolveFieldAtLevel(
      schema: StructType,
      fieldName: String,
      reference: String,
      caseSensitive: Boolean
  ): (StructField, Int) = {
    schema.fields.zipWithIndex.filter { case (field, _) =>
      columnNamesMatch(field.name, fieldName, caseSensitive)
    } match {
      case Array(result) => result
      case Array() => throw new IllegalArgumentException(
        s"$reference does not exist. Available: ${schema.fieldNames.mkString(", ")}")
      case matches => throw new IllegalArgumentException(
        s"$reference is ambiguous. Matches: ${matches.map(_._1.name).mkString(", ")}")
    }
  }
}

class EnsembleByKey(val uid: String) extends Transformer
  with Wrappable with DefaultParamsWritable with SynapseMLLogging {

  import EnsembleByKey._

  logClass(FeatureNames.Core)
  override protected lazy val pyInternalWrapper = true

  def this() = this(Identifiable.randomUID("EnsembleByKey"))

  val keys = new StringArrayParam(this, "keys", "Keys to group by")

  def getKeys: Array[String] = $(keys)

  def setKeys(arr: Array[String]): this.type = set(keys, arr)

  def setKeys(arr: String*): this.type = set(keys, arr.toArray)

  def setKey(value: String): this.type = set(keys, Array(value))

  val cols = new StringArrayParam(this, "cols", "Cols to ensemble")

  def getCols: Array[String] = $(cols)

  def setCols(arr: Array[String]): this.type = set(cols, arr)

  def setCols(arr: String*): this.type = set(cols, arr.toArray)

  def setCol(value: String): this.type = set(cols, Array(value))

  val colNames = new StringArrayParam(this, "colNames", "Names of the result of each col")

  def getColNames: Array[String] = get(colNames).getOrElse(getCols.map(name => s"$getStrategy($name)"))

  def setColNames(arr: Array[String]): this.type = set(colNames, arr)

  def setColNames(arr: String*): this.type = set(colNames, arr.toArray)

  def setColName(value: String): this.type = set(colNames, Array(value))

  val allowedStrategies: Set[String] = Set("mean")
  val strategy = new Param[String](this, "strategy", "How to ensemble the scores, ex: mean",
    { x: String => allowedStrategies(x) })

  def getStrategy: String = $(strategy)

  def setStrategy(value: String): this.type = set(strategy, value)

  setDefault(strategy -> "mean")

  val collapseGroup = new BooleanParam(
    this, "collapseGroup", "Whether to collapse all items in group to one entry")

  def getCollapseGroup: Boolean = $(collapseGroup)

  def setCollapseGroup(value: Boolean): this.type = set(collapseGroup, value)

  val vectorDims = new StringIntMapParam(this, "vectorDims",
    "the dimensions of any vector columns, used to avoid materialization")

  def getVectorDims: Map[String, Int] = get(vectorDims).getOrElse(Map())

  def setVectorDims(value: Map[String, Int]): this.type = set(vectorDims, value)

  def setVectorDims(value: java.util.HashMap[String, Int]): this.type = set(vectorDims, value.asScala.toMap)

  setDefault(collapseGroup -> true)

  private val aggregateType: DataType => Option[DataType] = {
    case _: DoubleType => Some(DoubleType)
    case _: FloatType => Some(DoubleType)
    case fdt if fdt == VectorType => Some(VectorType)
    case _ => None
  }

  private val aggregateField = (outputName: String, dataType: DataType) =>
    StructField(outputName, dataType, nullable = dataType != VectorType)

  private val keyRole = FieldRole(
    consumesInputColumn = true,
    field => Some(field.copy(name = "")))

  private val aggregateRole = FieldRole(
    consumesInputColumn = false,
    field => aggregateType(field.dataType).map(aggregateField("", _)))

  private val topLevelMatches = (schema: StructType, fieldName: String, caseSensitive: Boolean) =>
    schema.fields.zipWithIndex.collect {
      case (field, ordinal) if columnNamesMatch(field.name, fieldName, caseSensitive) => ordinal
    }

  private val analyzedAttributes = (dataset: Option[Dataset[_]]) =>
    dataset.toSeq.flatMap(_.queryExecution.analyzed.output)

  private def pruneDuplicates[A](candidates: Seq[A])(metadataOf: A => Metadata): Seq[A] = {
    if (candidates.length <= 1) {
      candidates
    } else {
      val pruned = candidates.filterNot(metadataOf(_).contains(DuplicateMetadataKey))
      if (pruned.isEmpty) candidates else pruned
    }
  }

  private val withoutDuplicateMarker = (metadata: Metadata) =>
    if (metadata.contains(DuplicateMetadataKey)) {
      new MetadataBuilder().withMetadata(metadata).remove(DuplicateMetadataKey).build()
    } else {
      metadata
    }

  private val declaredField = (field: StructField, name: String) =>
    field.copy(name = name, metadata = withoutDuplicateMarker(field.metadata))

  private val shareOneExpression = (attributes: Seq[Attribute], ordinals: Array[Int]) =>
    ordinals.length > 1 && ordinals.forall(_ < attributes.length) &&
      ordinals.map(attributes(_).exprId).distinct.length == 1

  private def qualifiedPathMatches(
      attributes: Seq[Attribute],
      parsedPath: Array[String],
      caseSensitive: Boolean
  ): Seq[QualifiedMatch] = {
    attributes.zipWithIndex
      .flatMap { case (attribute, ordinal) =>
        (1 until parsedPath.length).collect {
          case index
              if columnNamesMatch(attribute.name, parsedPath(index), caseSensitive) &&
                qualifiersMatch(attribute.qualifier, parsedPath.take(index), caseSensitive) =>
            QualifiedMatch(parsedPath.take(index), parsedPath.drop(index), ordinal, attribute.exprId)
        }
      }
  }

  private def qualifiedMatch(
      parsedPath: Array[String],
      reference: String,
      caseSensitive: Boolean,
      dataset: Option[Dataset[_]]
  ): Option[QualifiedMatch] = {
    val attributes = analyzedAttributes(dataset)
    val allMatches = qualifiedPathMatches(attributes, parsedPath, caseSensitive)
    if (allMatches.isEmpty) {
      None
    } else {
      // Spark selects the qualifier/name candidate set first and only then prunes duplicate-marked
      // candidates, so pruning must never change which qualifier length wins.
      val longestQualifier = allMatches.map(_.qualifier.length).max
      val selected = allMatches.filter(_.qualifier.length == longestQualifier)
      val matches = pruneDuplicates(selected)(candidate => attributes(candidate.ordinal).metadata)
      require(
        matches.map(_.exprId).distinct.length == 1,
        s"$reference is ambiguous because it matches multiple dataset attributes")
      Some(matches.head)
    }
  }

  private val schemaSplit = (schema: StructType, parsedPath: Array[String], caseSensitive: Boolean) =>
    parsedPath.indices.filter(index =>
      schema.fields.exists(field =>
        columnNamesMatch(field.name, parsedPath(index), caseSensitive)))

  private val qualifiersMatch = (actual: Seq[String], configured: Array[String], caseSensitive: Boolean) =>
    actual.length >= configured.length &&
      actual.takeRight(configured.length).zip(configured)
        .forall { case (left, right) => columnNamesMatch(left, right, caseSensitive) }

  private def bindQualifier(
      dataset: Dataset[_],
      resolved: ResolvedField,
      caseSensitive: Boolean
  ): ResolvedField = {
    if (resolved.qualifier.isEmpty) {
      resolved
    } else {
      val candidates = dataset.queryExecution.analyzed.output.zipWithIndex.filter { case (attribute, _) =>
        columnNamesMatch(attribute.name, resolved.path.head.name, caseSensitive) &&
          qualifiersMatch(attribute.qualifier, resolved.qualifier, caseSensitive)
      }
      val matches = pruneDuplicates(candidates)(_._1.metadata)
      matches match {
        case Seq() =>
          throw new IllegalArgumentException(s"${resolved.reference} does not match a dataset qualifier")
        case _ if matches.map(_._1.exprId).distinct.length == 1 =>
          resolved.copy(ordinals = resolved.ordinals.updated(0, matches.head._2))
        case _ =>
          throw new IllegalArgumentException(s"${resolved.reference} is ambiguous")
      }
    }
  }

  // Spark's GetMapValue casts the requested literal to the map key type and additionally requires
  // that key type to be orderable (TypeUtils.checkForOrderingExpr -> RowOrdering.isOrderable).
  // RowOrdering.isOrderable(DataType) is identical in Spark 3.5 and Spark 4.1, so it is safe here.
  private val mapKeyIsExtractable = (keyType: DataType) =>
    Cast.canCast(StringType, keyType) && RowOrdering.isOrderable(keyType)

  private val unsupportedMapKeyMessage = (reference: String, keyType: DataType) =>
    s"$reference cannot be extracted because map key type $keyType " + (
      if (Cast.canCast(StringType, keyType)) {
        "is not orderable, so Spark cannot look up a map value by key. " +
          "Use a map column whose key type is orderable, such as string."
      } else "does not accept string keys")

  private def resolveStep(
      currentType: DataType,
      fieldName: String,
      currentNullable: Boolean,
      reference: String,
      caseSensitive: Boolean
  ): ResolvedStep = {
    currentType match {
      case currentSchema: StructType =>
        val (field, fieldOrdinal) = resolveFieldAtLevel(
          currentSchema,
          fieldName,
          reference,
          caseSensitive)
        ResolvedStep(
          field.name,
          field.dataType,
          currentNullable || field.nullable,
          field.metadata,
          fieldOrdinal,
          None)
      case ArrayType(elementSchema: StructType, containsNull) =>
        val (field, fieldOrdinal) =
          resolveFieldAtLevel(elementSchema, fieldName, reference, caseSensitive)
        ResolvedStep(
          field.name,
          ArrayType(field.dataType, containsNull || field.nullable),
          currentNullable,
          Metadata.empty,
          fieldOrdinal,
          None)
      case MapType(keyType, valueType, _) if mapKeyIsExtractable(keyType) =>
        ResolvedStep(fieldName, valueType, nullable = true, Metadata.empty, -1, Some(keyType))
      case MapType(keyType, _, _) =>
        throw new IllegalArgumentException(unsupportedMapKeyMessage(reference, keyType))
      case _ =>
        throw new IllegalArgumentException(
          s"$reference is not supported by Spark nested field extraction")
    }
  }

  private def resolvePath(
      currentType: DataType,
      remainingPath: List[String],
      currentNullable: Boolean,
      ordinals: List[Int],
      reference: String,
      caseSensitive: Boolean
  ): (StructField, List[Int], List[PathStep]) = {
    val step = resolveStep(
      currentType,
      remainingPath.head,
      currentNullable,
      reference,
      caseSensitive)
    val pathStep = PathStep(step.fieldName, step.mapKeyType)

    remainingPath.tail match {
      case Nil =>
        (StructField(step.fieldName, step.dataType, step.nullable, step.metadata),
          ordinals :+ step.ordinal,
          List(pathStep))
      case nestedPath =>
        val (field, fieldOrdinals, fieldSteps) = resolvePath(
          step.dataType,
          nestedPath,
          step.nullable,
          ordinals :+ step.ordinal,
          reference,
          caseSensitive)
        (field, fieldOrdinals, pathStep +: fieldSteps)
    }
  }

  private def candidateOutput(
      schema: StructType,
      requestedPath: Array[String],
      ordinal: Int,
      reference: String,
      caseSensitive: Boolean,
      role: FieldRole
  ): Option[Option[StructField]] = {
    Try(resolveAtOrdinal(schema, Array.empty[String], requestedPath, ordinal, reference, caseSensitive))
      .toOption
      .map(resolved => role.declaredOutput(resolved.field))
  }

  private val candidateOutputsAgree = (
      schema: StructType,
      matches: Array[Int],
      requestedPath: Array[String],
      reference: String,
      caseSensitive: Boolean,
      role: FieldRole) =>
    matches
      .map(ordinal => candidateOutput(schema, requestedPath, ordinal, reference, caseSensitive, role))
      .distinct
      .length <= 1

  private def requireStableQualifiedField(
      schema: StructType,
      matches: Array[Int],
      requestedPath: Array[String],
      reference: String,
      caseSensitive: Boolean,
      role: FieldRole
  ): Unit = {
    require(
      candidateOutputsAgree(schema, matches, requestedPath, reference, caseSensitive, role),
      s"$reference matches columns with incompatible declared outputs")
    require(
      matches.length <= 1 || requestedPath.length > 1 ||
        getCollapseGroup || !role.consumesInputColumn,
      s"$reference cannot be resolved from schema because multiple columns are named " +
        s"${requestedPath.head} when collapseGroup is false")
  }

  private def resolveFromSchema(
      schema: StructType,
      qualifier: Array[String],
      requestedPath: Array[String],
      reference: String,
      caseSensitive: Boolean,
      role: FieldRole,
      dataset: Option[Dataset[_]]
  ): ResolvedField = {
    val candidates = topLevelMatches(schema, requestedPath.head, caseSensitive)
    if (qualifier.isEmpty) {
      resolveUnqualifiedFromSchema(schema, candidates, requestedPath, reference, caseSensitive, role, dataset)
    } else if (candidates.isEmpty) {
      resolveNestedPath(schema, qualifier, requestedPath, reference, caseSensitive)
    } else {
      // A schema carries no qualifier metadata, so every ordinal the dataset-aware path could select
      // must derive the same output instead of pruning duplicate-marked fields out of the candidates.
      requireStableQualifiedField(schema, candidates, requestedPath, reference, caseSensitive, role)
      resolveAtOrdinal(schema, qualifier, requestedPath, candidates.head, reference, caseSensitive)
    }
  }

  private def resolveUnqualifiedFromSchema(
      schema: StructType,
      candidates: Array[Int],
      requestedPath: Array[String],
      reference: String,
      caseSensitive: Boolean,
      role: FieldRole,
      dataset: Option[Dataset[_]]
  ): ResolvedField = {
    val matches = pruneDuplicates(candidates.toSeq)(schema(_).metadata).toArray
    val resolvableDuplicates = candidates.length > 1 &&
      (matches.length == 1 ||
        ((dataset.isEmpty || shareOneExpression(analyzedAttributes(dataset), matches)) &&
          Try(requireStableQualifiedField(
            schema, matches, requestedPath, reference, caseSensitive, role)).isSuccess))
    if (!resolvableDuplicates) {
      resolveNestedPath(schema, Array.empty[String], requestedPath, reference, caseSensitive)
    } else {
      requireStableQualifiedField(schema, matches, requestedPath, reference, caseSensitive, role)
      resolveAtOrdinal(schema, Array.empty[String], requestedPath, matches.head, reference, caseSensitive)
    }
  }

  private def resolveNestedPath(
      schema: StructType,
      qualifier: Array[String],
      requestedPath: Array[String],
      reference: String,
      caseSensitive: Boolean
  ): ResolvedField = {
    val (field, ordinals, steps) =
      resolvePath(schema, requestedPath.toList, false, Nil, reference, caseSensitive)
    ResolvedField(reference, qualifier, steps.toArray, ordinals.toArray, declaredField(field, requestedPath.last))
  }

  private def resolveFromOrdinal(
      schema: StructType,
      qualifier: Array[String],
      requestedPath: Array[String],
      ordinal: Int,
      reference: String,
      caseSensitive: Boolean,
      role: FieldRole
  ): ResolvedField = {
    val matches = topLevelMatches(schema, requestedPath.head, caseSensitive)
    requireStableQualifiedField(schema, matches, requestedPath, reference, caseSensitive, role)
    resolveAtOrdinal(schema, qualifier, requestedPath, ordinal, reference, caseSensitive)
  }

  private def resolveAtOrdinal(
      schema: StructType,
      qualifier: Array[String],
      requestedPath: Array[String],
      ordinal: Int,
      reference: String,
      caseSensitive: Boolean
  ): ResolvedField = {
    val topField = schema(ordinal)
    val topStep = PathStep(topField.name, None)
    val (field, ordinals, steps) = requestedPath.tail.toList match {
      case Nil => (topField, List(ordinal), List(topStep))
      case nestedPath =>
        val (nestedField, nestedOrdinals, nestedSteps) = resolvePath(
          topField.dataType,
          nestedPath,
          topField.nullable,
          List(ordinal),
          reference,
          caseSensitive)
        (nestedField, nestedOrdinals, topStep +: nestedSteps)
    }
    ResolvedField(reference, qualifier, steps.toArray, ordinals.toArray, declaredField(field, requestedPath.last))
  }

  private def outputContribution(
      role: FieldRole,
      resolved: ResolvedField
  ): (Option[StructField], Option[Int]) = {
    val consumesOrdinal =
      role.consumesInputColumn && !getCollapseGroup && resolved.path.length == 1
    val consumedOrdinal = if (consumesOrdinal) Some(resolved.ordinals.head) else None
    (role.declaredOutput(resolved.field), consumedOrdinal)
  }

  private def schemaInterpretations(
      schema: StructType,
      parsedPath: Array[String],
      reference: String,
      caseSensitive: Boolean,
      role: FieldRole,
      dataset: Option[Dataset[_]]
  ): Seq[ResolvedField] = {
    schemaSplit(schema, parsedPath, caseSensitive).flatMap(index =>
      Try(resolveFromSchema(
        schema,
        parsedPath.take(index),
        parsedPath.drop(index),
        reference,
        caseSensitive,
        role,
        dataset)).toOption)
  }

  private def resolveField(
      schema: StructType,
      reference: String,
      caseSensitive: Boolean,
      dataset: Option[Dataset[_]],
      role: FieldRole
  ): ResolvedField = {
    val parsedPath = UnresolvedAttribute.parseAttributeName(reference).toArray
    val interpretations = schemaInterpretations(schema, parsedPath, reference, caseSensitive, role, dataset)
    require(
      interpretations.map(outputContribution(role, _)).distinct.length <= 1,
      s"$reference is ambiguous between a nested field and a dataset qualifier")

    qualifiedMatch(parsedPath, reference, caseSensitive, dataset) match {
      case Some(matched) =>
        resolveFromOrdinal(
          schema,
          matched.qualifier,
          matched.requestedPath,
          matched.ordinal,
          reference,
          caseSensitive,
          role)
      case None =>
        interpretations.headOption.getOrElse {
          val pathStart = schemaSplit(schema, parsedPath, caseSensitive).headOption.getOrElse(0)
          resolveFromSchema(
            schema,
            parsedPath.take(pathStart),
            parsedPath.drop(pathStart),
            reference,
            caseSensitive,
            role,
            dataset)
        }
    }
  }

  private def validateNonCollapsedKeys(
      schema: StructType,
      keyFields: Array[ResolvedField],
      outputNames: Array[String],
      caseSensitive: Boolean
  ): Unit = {
    val keyOutputCollisions = outputNames.filter(outputName =>
      keyFields.exists(resolved =>
        columnNamesMatch(resolved.field.name, outputName, caseSensitive))).distinct
    require(
      keyOutputCollisions.isEmpty,
      s"Output columns ${keyOutputCollisions.mkString(", ")} cannot overwrite grouping keys " +
        s"${keyFields.map(_.field.name).mkString(", ")} when collapseGroup is false")

    val nestedKeyCollisions = keyFields.filter(_.path.length > 1).filter(resolved =>
      schema.fields.exists(field =>
        columnNamesMatch(field.name, resolved.field.name, caseSensitive)))
    require(
      nestedKeyCollisions.isEmpty,
      s"Nested grouping keys ${nestedKeyCollisions.map(_.reference).mkString(", ")} " +
        "cannot overwrite top-level columns when collapseGroup is false")

    val duplicateNestedKeyNames = keyFields.indices.flatMap { leftIndex =>
      ((leftIndex + 1) until keyFields.length).collect {
        case rightIndex
            if columnNamesMatch(
                keyFields(leftIndex).field.name,
                keyFields(rightIndex).field.name,
                caseSensitive) =>
          keyFields(leftIndex).field.name
      }
    }.distinct
    require(
      duplicateNestedKeyNames.isEmpty,
      s"Grouping keys must resolve to distinct output columns when collapseGroup is false: " +
        duplicateNestedKeyNames.mkString(", "))
  }

  private def getSchemaFields(
      schema: StructType,
      dataset: Option[Dataset[_]] = None
  ): ResolvedColumns = {
    val inputNames = get(cols).getOrElse(
      throw new IllegalArgumentException("cols must be set and non-empty"))
    val keyNames = get(keys).getOrElse(
      throw new IllegalArgumentException("keys must be set and non-empty"))
    require(inputNames.nonEmpty, "cols must be set and non-empty")
    require(keyNames.nonEmpty, "keys must be set and non-empty")
    val outputNames = get(colNames).getOrElse(
      inputNames.map(name => s"$getStrategy($name)"))
    require(
      inputNames.length == outputNames.length,
      s"cols (${inputNames.length}) and colNames (${outputNames.length}) must have the same length")

    val caseSensitive = dataset.map(_.sparkSession).orElse(SparkSession.getActiveSession)
      .exists(_.conf.get("spark.sql.caseSensitive", "false").trim.toBoolean)
    val inputFields = inputNames.map(resolveField(schema, _, caseSensitive, dataset, aggregateRole))
    val keyFields = keyNames.map(resolveField(schema, _, caseSensitive, dataset, keyRole))
    keyFields.foreach { key =>
      require(RowOrdering.isOrderable(key.field.dataType),
        s"${key.reference} resolves to ${key.field.dataType}, which Spark cannot use as a grouping key")
    }
    if (!getCollapseGroup) {
      validateNonCollapsedKeys(schema, keyFields, outputNames, caseSensitive)
    }

    val aggregateFields = inputFields.zip(outputNames).map { case (resolvedInput, outputName) =>
      aggregateType(resolvedInput.field.dataType)
        .map(aggregateField(outputName, _))
        .getOrElse(throw new IllegalArgumentException(
          s"Cannot operate on type ${resolvedInput.field.dataType} with strategy $getStrategy"))
    }

    ResolvedColumns(inputFields, outputNames, keyFields, aggregateFields, caseSensitive)
  }

  private def bindQualifiers(
      dataset: Dataset[_],
      resolvedColumns: ResolvedColumns
  ): ResolvedColumns = {
    resolvedColumns.copy(
      inputFields = resolvedColumns.inputFields.map(bindQualifier(
        dataset,
        _,
        resolvedColumns.caseSensitive)),
      keyFields = resolvedColumns.keyFields.map(bindQualifier(
        dataset,
        _,
        resolvedColumns.caseSensitive)))
  }

  private val quoteIdentifier = (name: String) => s"`${name.replace("`", "``")}`"

  private val inputName = (index: Int) => s"__ensemble_by_key_input_$index"

  private val keyName = (index: Int) => s"__ensemble_by_key_key_$index"

  private val aggregateName = (index: Int) => s"__ensemble_by_key_aggregate_$index"

  private val normalize = (dataset: Dataset[_]) =>
    dataset.toDF(dataset.schema.indices.map(inputName): _*)

  private def resolvedColumn(resolved: ResolvedField): Column = {
    val root = col(quoteIdentifier(inputName(resolved.ordinals.head)))
    resolved.path.tail.foldLeft(root) { (column, step) =>
      step.mapKeyType match {
        case Some(keyType) => column(lit(step.name).cast(keyType))
        case None => column.getField(step.name)
      }
    }
  }

  // The identity cast prevents grouping analysis from propagating source metadata to the key.
  private val keyColumn = (resolved: ResolvedField, index: Int) =>
    resolvedColumn(resolved).cast(resolved.field.dataType).as(keyName(index), resolved.field.metadata)

  private def aggregateColumn(
      resolvedInput: ResolvedField,
      outputName: String
  ): Column = {
    val inputColumn = resolvedColumn(resolvedInput)
    aggregateType(resolvedInput.field.dataType) match {
      case Some(fdt) if fdt == VectorType => Summarizer.mean(inputColumn).alias(outputName)
      case Some(_) => mean(inputColumn).alias(outputName)
      case None => throw new IllegalArgumentException(
        s"Cannot operate on type ${resolvedInput.field.dataType} with strategy $getStrategy")
    }
  }

  private def aggregate(
      dataset: Dataset[_],
      normalized: DataFrame,
      resolvedColumns: ResolvedColumns
  ): DataFrame = {
    val keyColumns = resolvedColumns.keyFields.zipWithIndex.map { case (r, i) => keyColumn(r, i) }
    val newColumns = resolvedColumns.inputFields.zipWithIndex.map { case (resolvedInput, index) =>
        aggregateColumn(resolvedInput, aggregateName(index))
    }
    val retainGroupColumns = dataset.sparkSession.conf
      .get("spark.sql.retainGroupColumns", "true").trim.toBoolean
    val aggregateColumns = if (retainGroupColumns) newColumns else keyColumns ++ newColumns

    normalized
      .groupBy(keyColumns: _*)
      .agg(aggregateColumns.head, aggregateColumns.tail: _*)
  }

  private def outputKeyColumns(resolvedColumns: ResolvedColumns): Array[Column] = {
    resolvedColumns.keyFields.zipWithIndex.map { case (resolved, index) =>
      col(quoteIdentifier(keyName(index))).as(resolved.field.name, resolved.field.metadata)
    }
  }

  private def outputAggregateColumns(resolvedColumns: ResolvedColumns): Array[Column] = {
    resolvedColumns.outputNames.indices.map(index =>
      col(quoteIdentifier(aggregateName(index))).as(resolvedColumns.outputNames(index))).toArray
  }

  private def passthroughColumns(
      schema: StructType,
      resolvedColumns: ResolvedColumns
  ): Array[Column] = {
    val topLevelKeyOrdinals = resolvedColumns.keyFields.filter(_.path.length == 1)
      .map(_.ordinals.head).toSet
    schema.fields.zipWithIndex.collect {
      case (field, index)
          if !topLevelKeyOrdinals(index) &&
            !resolvedColumns.outputNames.exists(outputName =>
              columnNamesMatch(field.name, outputName, resolvedColumns.caseSensitive)) =>
        col(quoteIdentifier(inputName(index))).as(field.name, field.metadata)
    }
  }

  private def mergeWithGroups(
      normalized: DataFrame,
      aggregated: DataFrame,
      resolvedColumns: ResolvedColumns,
      inputSchema: StructType
  ): DataFrame = {
    val leftKeys = resolvedColumns.keyFields.zipWithIndex.map { case (r, i) => keyColumn(r, i) }
    val left = normalized.select((col("*") +: leftKeys.toSeq): _*)
    val conditions = resolvedColumns.keyFields.indices.map(i => left(keyName(i)) <=> aggregated(keyName(i)))
    val joined = left.join(aggregated, conditions.reduce(_ && _)).select(
      (left.columns.map(left(_)) ++ resolvedColumns.outputNames.indices.map(i =>
        aggregated(aggregateName(i)))): _*)
    val outputColumns =
      outputKeyColumns(resolvedColumns) ++
        passthroughColumns(inputSchema, resolvedColumns) ++
        outputAggregateColumns(resolvedColumns)
    joined.select(outputColumns: _*)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame]({
      val resolvedColumns = bindQualifiers(dataset, getSchemaFields(dataset.schema, Some(dataset)))
      val normalized = normalize(dataset)
      val aggregated = aggregate(dataset, normalized, resolvedColumns)

      if (getCollapseGroup) {
        aggregated.select((outputKeyColumns(resolvedColumns) ++
          outputAggregateColumns(resolvedColumns)): _*)
      } else {
        mergeWithGroups(normalized, aggregated, resolvedColumns, dataset.schema)
      }
    }, dataset.columns.length)
  }

  def transformSchema(schema: StructType): StructType = {
    val resolvedColumns = getSchemaFields(schema)
    val fields = if (getCollapseGroup) {
      resolvedColumns.keyFields.map(_.field) ++ resolvedColumns.aggregateFields
    } else {
      val topLevelKeyOrdinals = resolvedColumns.keyFields.filter(_.path.length == 1)
        .map(_.ordinals.head).toSet
      val inputFields = schema.fields.zipWithIndex.collect {
        case (field, index)
            if !topLevelKeyOrdinals(index) &&
              !resolvedColumns.outputNames.exists(outputName =>
                columnNamesMatch(field.name, outputName, resolvedColumns.caseSensitive)) =>
          field
      }
      resolvedColumns.keyFields.map(_.field) ++ inputFields ++ resolvedColumns.aggregateFields
    }

    new StructType(fields)
  }

  def copy(extra: ParamMap): this.type = defaultCopy(extra)
}
