// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.stages

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import com.microsoft.azure.synapse.ml.param.{StringIntMapParam, StringStringMapParam}
import org.apache.spark.ml.param.{Param, ParamMap, ParamValidators}
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.functions.{asc, coalesce, col, count, desc, lit, when}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/** Parameters shared by [[LumpFeatures]] and [[LumpFeaturesModel]]. */
trait LumpFeaturesParams extends Wrappable with DefaultParamsWritable {

  val lumpRules: StringIntMapParam = new StringIntMapParam(
    this, "lumpRules",
    "Map from column name to the positive number of most-frequent values (top-K) to retain per column. " +
      "Every other non-null value in each column is replaced with otherValue.")

  def getLumpRules: Map[String, Int] = get(lumpRules).getOrElse(Map.empty)

  def setLumpRules(value: Map[String, Int]): this.type = set(lumpRules, value)

  def setLumpRules(value: java.util.HashMap[String, Int]): this.type = set(lumpRules, value.asScala.toMap)

  def setLumpRules(value: String): this.type = set(lumpRules, lumpRules.jsonDecode(value))

  val otherValue: Param[String] = new Param[String](
    this, "otherValue",
    "The single explicit replacement value used for every non-retained value (and, when handleNull is " +
      "'other', for null) across all rule columns. Must be non-null and must not occur in any rule column.")

  def getOtherValue: String = $(otherValue)

  def setOtherValue(value: String): this.type = set(otherValue, value)

  val handleNull: Param[String] = new Param[String](
    this, "handleNull",
    "How to treat null inputs in rule columns: 'keep' preserves null, 'other' maps null to otherValue. " +
      "Unseen non-null values always map to otherValue.",
    ParamValidators.inArray(Array("keep", "other")))

  def getHandleNull: String = $(handleNull)

  def setHandleNull(value: String): this.type = set(handleNull, value)

  setDefault(otherValue -> "__other__", handleNull -> "keep")

  protected def validateRules(): Unit = {
    val rules = getLumpRules
    require(rules.nonEmpty,
      "lumpRules must be a non-empty map from column name to a positive top-K.")
    rules.foreach { case (name, k) =>
      require(Option(name).exists(_.nonEmpty),
        "lumpRules contains an empty or null column name; every rule column name must be non-empty.")
      require(k > 0,
        s"lumpRules top-K for column '$name' must be a positive integer but was $k.")
    }
    require(Option(getOtherValue).isDefined,
      "otherValue must be non-null.")
  }

  protected def validateAndTransformSchema(schema: StructType): StructType = {
    validateRules()
    val ruleCols = getLumpRules.keySet
    ruleCols.foreach { name =>
      require(schema.fieldNames.contains(name),
        s"lumpRules references column '$name' which is not present in the input schema " +
          s"[${schema.fieldNames.mkString(", ")}].")
      val dt = schema(name).dataType
      require(dt == StringType,
        s"lumpRules column '$name' must be StringType for LumpFeatures v1 but was ${dt.simpleString}.")
    }
    val keepNulls = getHandleNull == "keep"
    val fields = schema.fields.map { f =>
      if (ruleCols.contains(f.name)) {
        StructField(f.name, StringType, nullable = f.nullable && keepNulls, Metadata.empty)
      } else {
        f
      }
    }
    StructType(fields)
  }

  /** Reference a top-level column by its literal name, backtick-quoting it and doubling any embedded
    * backticks so names containing dots, backticks, or reserved words like `count` are never parsed
    * as nested paths or interpreted as aggregate/reserved identifiers.
    */
  protected def litCol(name: String): Column = col("`" + name.replace("`", "``") + "`")
}

object LumpFeatures extends DefaultParamsReadable[LumpFeatures]

/** Learns, per configured column, the top-K most frequent string values and produces a
  * [[LumpFeaturesModel]] that replaces every other value with a single global otherValue.
  */
class LumpFeatures(override val uid: String)
  extends Estimator[LumpFeaturesModel] with LumpFeaturesParams with SynapseMLLogging {
  logClass(FeatureNames.Core)

  def this() = this(Identifiable.randomUID("LumpFeatures"))

  override def fit(dataset: Dataset[_]): LumpFeaturesModel = {
    logFit({
      transformSchema(dataset.schema)
      val rules = getLumpRules
      val other = getOtherValue
      val df = dataset.toDF()

      val collided = findOtherValueCollisions(df, rules.keys.toSeq, other)
      require(collided.isEmpty,
        s"otherValue '$other' collides with an existing value in rule column(s) " +
          s"[${collided.sorted.mkString(", ")}]. Choose an otherValue that does not occur in the data.")

      val valueAlias = "__lump_value__"
      val countAlias = "__lump_count__"
      val keptJson = rules.map { case (name, k) =>
        val top = df.select(litCol(name).as(valueAlias))
          .where(col(valueAlias).isNotNull)
          .groupBy(col(valueAlias))
          .agg(count(lit(1)).as(countAlias))
          .orderBy(desc(countAlias), asc(valueAlias))
          .limit(k)
          .collect()
          .map(_.getString(0))
          .toList
        (name, LumpFeaturesModel.encodeKept(k, top))
      }

      new LumpFeaturesModel(uid)
        .setLumpRules(rules)
        .setOtherValue(other)
        .setHandleNull(getHandleNull)
        .setKeptValuesJson(keptJson)
        .setParent(this)
    }, dataset.columns.length)
  }

  private def findOtherValueCollisions(df: DataFrame, cols: Seq[String], other: String): Seq[String] = {
    val aliases = cols.indices.map(i => s"__lump_collision_$i")
    val exprs = cols.zip(aliases).map { case (c, a) =>
      count(when(litCol(c) === lit(other), lit(1))).as(a)
    }
    val row = df.agg(exprs.head, exprs.tail: _*).collect().head
    cols.zip(aliases).collect { case (c, a) if row.getAs[Long](a) > 0L => c }
  }

  override def copy(extra: ParamMap): LumpFeatures = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)
}

object LumpFeaturesModel extends DefaultParamsReadable[LumpFeaturesModel] {

  /** Encode a column fitted state as a JSON object recording both the fitted top-K and the retained
    * values so a fitted model can later reject any incompatible lumpRules change. */
  private[stages] def encodeKept(topK: Int, values: Seq[String]): String =
    JsObject("topK" -> JsNumber(topK), "values" -> JsArray(values.map(JsString(_)).toVector)).compactPrint

  /** Decode a column fitted state into its fitted top-K and retained values, failing clearly when the
    * stored JSON is malformed or not the expected object shape. */
  private[stages] def decodeKept(name: String, json: String): (Int, Seq[String]) = {
    parseKeptJson(name, json) match {
      case JsObject(fields) => (decodeTopK(name, json, fields), decodeValueList(name, json, fields))
      case _ =>
        throw new IllegalArgumentException(
          s"keptValuesJson for column '$name' must be a JSON object with topK and values fields: '$json'.")
    }
  }

  private def parseKeptJson(name: String, json: String): JsValue =
    try json.parseJson
    catch {
      case NonFatal(e) =>
        throw new IllegalArgumentException(s"keptValuesJson for column '$name' is not valid JSON: '$json'.", e)
    }

  private def decodeTopK(name: String, json: String, fields: Map[String, JsValue]): Int =
    fields.get("topK") match {
      case Some(JsNumber(n)) if n.isValidInt => n.toInt
      case _ =>
        throw new IllegalArgumentException(
          s"keptValuesJson for column '$name' must contain an integer topK field: '$json'.")
    }

  private def decodeValueList(name: String, json: String, fields: Map[String, JsValue]): Seq[String] =
    fields.get("values") match {
      case Some(JsArray(elems)) => elems.map {
        case JsString(v) => v
        case other =>
          throw new IllegalArgumentException(
            s"keptValuesJson values for column '$name' must be strings but found: ${other.compactPrint}.")
      }.toList
      case _ =>
        throw new IllegalArgumentException(
          s"keptValuesJson for column '$name' must contain a string-array values field: '$json'.")
    }
}

/** Model produced by [[LumpFeatures]]. Replaces, in place, every non-retained value in each rule
  * column with otherValue while retaining the learned top-K values unchanged.
  */
class LumpFeaturesModel(override val uid: String)
  extends Model[LumpFeaturesModel] with LumpFeaturesParams with SynapseMLLogging {
  logClass(FeatureNames.Core)

  def this() = this(Identifiable.randomUID("LumpFeaturesModel"))

  val keptValuesJson: StringStringMapParam = new StringStringMapParam(
    this, "keptValuesJson",
    "Learned model-only state per column, encoded as a map from column name to a JSON object holding the " +
      "fitted top-K (field topK) and the retained values array (field values) ordered by descending " +
      "frequency then ascending value. Populated by LumpFeatures during fit and immutable thereafter.")

  def getKeptValuesJson: Map[String, String] = get(keptValuesJson).getOrElse(Map.empty)

  def setKeptValuesJson(value: Map[String, String]): this.type = set(keptValuesJson, value)

  def setKeptValuesJson(value: java.util.HashMap[String, String]): this.type =
    set(keptValuesJson, value.asScala.toMap)

  def getKeptValues: Map[String, Seq[String]] =
    decodedKept.map { case (name, (_, values)) => (name, values) }

  private def decodedKept: Map[String, (Int, Seq[String])] =
    getKeptValuesJson.map { case (name, json) => (name, LumpFeaturesModel.decodeKept(name, json)) }

  /** The fitted top-K per column recorded at fit time; a fitted model keeps lumpRules equal to this. */
  private def getFittedTopK: Map[String, Int] =
    decodedKept.map { case (name, (topK, _)) => (name, topK) }

  private def guardLumpRulesChange(value: Map[String, Int]): Unit = {
    if (isDefined(keptValuesJson)) {
      val fitted = getFittedTopK
      require(value == fitted,
        s"Cannot change lumpRules on a fitted LumpFeaturesModel. The model was fitted with top-K $fitted " +
          s"but $value was requested; re-fit LumpFeatures to change the rules.")
    }
  }

  override def setLumpRules(value: Map[String, Int]): this.type = {
    guardLumpRulesChange(value)
    set(lumpRules, value)
  }

  override def setLumpRules(value: java.util.HashMap[String, Int]): this.type =
    setLumpRules(value.asScala.toMap)

  override def setLumpRules(value: String): this.type =
    setLumpRules(lumpRules.jsonDecode(value))

  protected def validateModelState(): Unit = {
    validateRules()
    val rules = getLumpRules
    val kept = decodedKept
    require(rules.keySet == kept.keySet,
      s"Model state is inconsistent with lumpRules: rule columns " +
        s"[${rules.keySet.toSeq.sorted.mkString(", ")}] do not match learned columns " +
        s"[${kept.keySet.toSeq.sorted.mkString(", ")}].")
    val other = getOtherValue
    kept.foreach { case (name, (topK, values)) =>
      require(rules(name) == topK,
        s"lumpRules top-K for column '$name' is ${rules(name)} but the model was fitted with top-K $topK. " +
          "A fitted LumpFeaturesModel does not allow lumpRules to change after fit; re-fit to change rules.")
      require(values.size <= topK,
        s"Model retains ${values.size} values for column '$name' which exceeds its fitted top-K of $topK.")
      require(!values.contains(other),
        s"otherValue '$other' collides with a retained value in column '$name'. " +
          "Choose an otherValue that is not among the learned values.")
    }
  }

  override def transformSchema(schema: StructType): StructType = {
    validateModelState()
    validateAndTransformSchema(schema)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame]({
      transformSchema(dataset.schema)
      val other = getOtherValue
      val keepNulls = getHandleNull == "keep"
      getKeptValues.foldLeft(dataset.toDF()) { case (acc, (name, values)) =>
        acc.withColumn(name, lumpExpr(name, values, other, keepNulls, acc.schema(name).nullable))
      }
    }, dataset.columns.length)
  }

  private def lumpExpr(name: String, values: Seq[String], other: String,
                       keepNulls: Boolean, inputNullable: Boolean): Column = {
    val retain =
      if (values.isEmpty) lit(other)
      else when(litCol(name).isin(values: _*), litCol(name)).otherwise(lit(other))
    // For handleNull=keep, referencing the column in the null branch yields null without a null literal.
    val handled = if (keepNulls) when(litCol(name).isNull, litCol(name)).otherwise(retain) else retain
    val declaredNullable = inputNullable && keepNulls
    if (declaredNullable) handled else coalesce(handled, lit(other))
  }

  override def copy(extra: ParamMap): LumpFeaturesModel = {
    val copied = copyValues(new LumpFeaturesModel(uid), extra).setParent(parent)
    if (copied.isDefined(copied.keptValuesJson)) copied.validateModelState()
    copied
  }
}
