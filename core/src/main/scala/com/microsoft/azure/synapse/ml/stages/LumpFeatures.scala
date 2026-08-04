// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.stages

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import com.microsoft.azure.synapse.ml.param.{StringIntMapParam, StringStringMapParam}
import org.apache.spark.ml.param.{DoubleParam, IntParam, Param, ParamMap, ParamValidators}
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{array, asc, coalesce, col, count, desc}
import org.apache.spark.sql.functions.{explode, lit, row_number, struct, sum, typedLit, when}
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
    "Map from column name to the maximum number of most-frequent values (top-K) to retain per column. " +
      "This is a cap, not the only criterion: a value is retained only if it also clears minCount and " +
      "minFreq. Every value that is not retained is replaced with otherValue.")

  def getLumpRules: Map[String, Int] = get(lumpRules).getOrElse(Map.empty)

  def setLumpRules(value: Map[String, Int]): this.type = set(lumpRules, value)

  def setLumpRules(value: java.util.HashMap[String, Int]): this.type = set(lumpRules, value.asScala.toMap)

  def setLumpRules(value: String): this.type = set(lumpRules, lumpRules.jsonDecode(value))

  val outputCols: StringStringMapParam = new StringStringMapParam(
    this, "outputCols",
    "Optional map from a lumpRules column name to the column the lumped values are written to. " +
      "Rule columns without an entry are replaced in place, which is the default and discards the " +
      "original values. Naming a new destination keeps the raw column intact alongside the lumped one.")

  def getOutputCols: Map[String, String] = get(outputCols).getOrElse(Map.empty)

  def setOutputCols(value: Map[String, String]): this.type = set(outputCols, value)

  def setOutputCols(value: java.util.HashMap[String, String]): this.type = set(outputCols, value.asScala.toMap)

  def setOutputCols(value: String): this.type = set(outputCols, outputCols.jsonDecode(value))

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
    getOutputCols.foreach { case (name, out) =>
      require(rules.contains(name),
        s"outputCols references column '$name' which has no lumpRules entry " +
          s"[${rules.keySet.toSeq.sorted.mkString(", ")}].")
      require(Option(out).exists(_.nonEmpty),
        s"outputCols destination for column '$name' must be a non-empty column name.")
    }
    val duplicated = orderedOutputs.map(_._2).groupBy(identity).collect { case (d, ds) if ds.size > 1 => d }
    require(duplicated.isEmpty,
      s"outputCols maps more than one rule column to destination(s) " +
        s"[${duplicated.toSeq.sorted.mkString(", ")}]; every destination must be distinct.")
    require(Option(getOtherValue).isDefined,
      "otherValue must be non-null.")
  }

  /** Every rule column paired with the column its lumped values are written to, in a deterministic
    * order so transformSchema and transform always agree on where appended columns land.
    */
  protected def orderedOutputs: Seq[(String, String)] = {
    val explicit = getOutputCols
    getLumpRules.keys.toSeq.sorted.map(name => (name, explicit.getOrElse(name, name)))
  }

  /** A lumped column is always a plain string, and its nullability follows handleNull alone so the
    * declared contract never depends on how the input schema happened to declare nullability. Input
    * metadata is dropped because it describes the pre-lumping value set.
    */
  private def lumpedField(name: String, keepNulls: Boolean): StructField =
    StructField(name, StringType, nullable = keepNulls, Metadata.empty)

  protected def validateAndTransformSchema(schema: StructType): StructType = {
    validateRules()
    val outputs = orderedOutputs
    outputs.foreach { case (name, out) =>
      require(schema.fieldNames.contains(name),
        s"lumpRules references column '$name' which is not present in the input schema " +
          s"[${schema.fieldNames.mkString(", ")}].")
      val dt = schema(name).dataType
      require(dt == StringType,
        s"lumpRules column '$name' must be StringType for LumpFeatures v1 but was ${dt.simpleString}.")
      require(out == name || !schema.fieldNames.contains(out),
        s"outputCols destination '$out' for rule column '$name' already exists in the input schema; " +
          "choose a new name or drop the entry to replace the rule column in place.")
    }
    val keepNulls = getHandleNull == "keep"
    val replacedInPlace = outputs.collect { case (name, out) if name == out => name }.toSet
    val retained = schema.fields.map { f =>
      if (replacedInPlace.contains(f.name)) lumpedField(f.name, keepNulls) else f
    }
    val appended = outputs.collect { case (name, out) if name != out => lumpedField(out, keepNulls) }
    StructType(retained ++ appended)
  }

  /** Reference a top-level column by its literal name, backtick-quoting it and doubling any embedded
    * backticks so names containing dots, backticks, or reserved words like `count` are never parsed
    * as nested paths or interpreted as aggregate/reserved identifiers.
    */
  protected def litCol(name: String): Column = col("`" + name.replace("`", "``") + "`")
}

object LumpFeatures extends DefaultParamsReadable[LumpFeatures] {

  /** One distinct value of one rule column, with its rank inside that column and whether it survived
    * the eligibility filters. Collected once per fit.
    */
  private[stages] case class RankedValue(column: String, value: String, rank: Int, retained: Boolean)
}

/** Learns, per configured column, which string values to retain and produces a [[LumpFeaturesModel]]
  * that replaces every other value with a single global otherValue.
  *
  * A value is retained when it clears both eligibility filters (minCount and minFreq) and then falls
  * inside its column's top-K from lumpRules. Filtering before capping is the order scikit-learn's
  * OneHotEncoder uses for min_frequency and max_categories: the frequency thresholds decide which
  * values are worth keeping at all, and top-K only bounds how many survive. Relying on top-K alone
  * is unreliable for training, because it is blind to the shape of the distribution - it will lump
  * healthy levels in a low-cardinality column, and in a very long-tailed column it can retain values
  * that together cover almost none of the rows.
  */
class LumpFeatures(override val uid: String)
  extends Estimator[LumpFeaturesModel] with LumpFeaturesParams with SynapseMLLogging {
  logClass(FeatureNames.Core)

  def this() = this(Identifiable.randomUID("LumpFeatures"))

  val minCount: IntParam = new IntParam(
    this, "minCount",
    "Minimum number of times a value must occur in the fitting data to be eligible for retention. " +
      "Rarer values are lumped into otherValue. Applied before the lumpRules top-K cap. " +
      "The default of 1 disables count-based lumping.",
    ParamValidators.gtEq(1))

  def getMinCount: Int = $(minCount)

  def setMinCount(value: Int): this.type = set(minCount, value)

  val minFreq: DoubleParam = new DoubleParam(
    this, "minFreq",
    "Minimum share of a column's non-null fitting rows a value must account for to be eligible for " +
      "retention, in [0, 1]. Rarer values are lumped into otherValue. Applied before the lumpRules " +
      "top-K cap. The default of 0.0 disables frequency-based lumping.",
    ParamValidators.inRange(0.0, 1.0))

  def getMinFreq: Double = $(minFreq)

  def setMinFreq(value: Double): this.type = set(minFreq, value)

  setDefault(minCount -> 1, minFreq -> 0.0)

  override def fit(dataset: Dataset[_]): LumpFeaturesModel = {
    logFit({
      transformSchema(dataset.schema)
      val rules = getLumpRules
      val other = getOtherValue
      val ranked = rankValues(dataset.toDF(), rules, other)

      val collided = ranked.filter(_.value == other).map(_.column).distinct.sorted
      require(collided.isEmpty,
        s"otherValue '$other' collides with an existing value in rule column(s) " +
          s"[${collided.mkString(", ")}]. Choose an otherValue that does not occur in the data.")

      val byColumn = ranked.filter(_.retained).groupBy(_.column)
      val keptJson = rules.map { case (name, k) =>
        val top = byColumn.getOrElse(name, Seq.empty).sortBy(_.rank).map(_.value).toList
        (name, LumpFeaturesModel.encodeKept(k, top))
      }

      val model = new LumpFeaturesModel(uid)
        .setLumpRules(rules)
        .setOtherValue(other)
        .setHandleNull(getHandleNull)
      get(outputCols).foreach(model.setOutputCols)
      model.setKeptValuesJson(keptJson).setParent(this)
    }, dataset.columns.length)
  }

  /** Rank every distinct non-null value of every rule column in a single pass.
    *
    * The rule columns are melted into (column, value) pairs so one aggregation serves all of them.
    * That keeps the cost at one shuffle instead of a separate full scan per column and, more
    * importantly, guarantees every column is learned from the same materialization of the input.
    * Per-column jobs cannot promise that: against a non-deterministic upstream plan (a sample, rand,
    * or an unordered limit) each column would be learned from different rows. Values equal to
    * otherValue are always returned so the collision check reuses this same aggregation.
    */
  private def rankValues(df: DataFrame, rules: Map[String, Int], other: String): Seq[LumpFeatures.RankedValue] = {
    val columnAlias = "__lump_column__"
    val valueAlias = "__lump_value__"
    val countAlias = "__lump_count__"
    val totalAlias = "__lump_total__"
    val rankAlias = "__lump_rank__"
    val retainedAlias = "__lump_retained__"
    val pairAlias = "__lump_pair__"
    val cols = rules.keys.toSeq.sorted

    val melted = df
      .select(explode(array(cols.map(c => struct(lit(c).as(columnAlias), litCol(c).as(valueAlias))): _*))
        .as(pairAlias))
      .select(col(s"$pairAlias.$columnAlias").as(columnAlias), col(s"$pairAlias.$valueAlias").as(valueAlias))
      .where(col(valueAlias).isNotNull)

    val perColumn = Window.partitionBy(col(columnAlias))
    val counted = melted
      .groupBy(col(columnAlias), col(valueAlias))
      .agg(count(lit(1)).as(countAlias))
      .withColumn(totalAlias, sum(col(countAlias)).over(perColumn))
      .withColumn(rankAlias, row_number().over(perColumn.orderBy(desc(countAlias), asc(valueAlias))))

    // Eligibility can only reject a suffix of the count-descending order, so intersecting it with the
    // rank cap is exactly "filter by frequency first, then keep at most K".
    val topK = cols.foldLeft(lit(0)) { (fallback, c) =>
      when(col(columnAlias) === lit(c), lit(rules(c))).otherwise(fallback)
    }
    val eligible = col(countAlias) >= lit(getMinCount) && col(countAlias) >= col(totalAlias) * lit(getMinFreq)

    counted
      .withColumn(retainedAlias, col(rankAlias) <= topK && eligible)
      .where(col(retainedAlias) || col(valueAlias) === lit(other))
      .select(col(columnAlias), col(valueAlias), col(rankAlias), col(retainedAlias))
      .collect()
      .map(r => LumpFeatures.RankedValue(r.getString(0), r.getString(1), r.getInt(2), r.getBoolean(3)))
      .toSeq
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

/** Model produced by [[LumpFeatures]]. Replaces every non-retained value in each rule column with
  * otherValue, leaving the learned values unchanged. By default each rule column is rewritten in
  * place; set outputCols to write the lumped values to new columns and keep the raw values.
  */
class LumpFeaturesModel(override val uid: String)
  extends Model[LumpFeaturesModel] with LumpFeaturesParams with SynapseMLLogging {
  logClass(FeatureNames.Core)

  override protected lazy val pyInternalWrapper: Boolean = true

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

  /** The learned values per column as one JSON object mapping column name to its retained values.
    * Language wrappers cannot reliably read the keptValuesJson param map off the JVM object, so this
    * gives Python and R callers a single well-defined way to inspect and audit what the model learned.
    */
  def getKeptValuesAsJson: String =
    JsObject(getKeptValues.map { case (name, values) =>
      (name, JsArray(values.map(JsString(_)).toVector): JsValue)
    }).compactPrint

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
      val kept = getKeptValues
      orderedOutputs.foldLeft(dataset.toDF()) { case (acc, (name, out)) =>
        acc.withColumn(out, lumpExpr(name, kept.getOrElse(name, Seq.empty), other, keepNulls))
      }
    }, dataset.columns.length)
  }

  private def lumpExpr(name: String, values: Seq[String], other: String, keepNulls: Boolean): Column = {
    val retain =
      if (values.isEmpty) lit(other)
      else when(litCol(name).isin(values: _*), litCol(name)).otherwise(lit(other))
    // The declared schema takes nullability from handleNull alone, so force the expression to agree
    // regardless of how the input column declared itself: a typed null literal stays nullable even
    // over a non-nullable input, and coalesce makes the non-null contract hold over a nullable one.
    if (keepNulls) when(litCol(name).isNull, typedLit(Option.empty[String])).otherwise(retain)
    else coalesce(retain, lit(other))
  }

  override def copy(extra: ParamMap): LumpFeaturesModel = {
    val copied = copyValues(new LumpFeaturesModel(uid), extra).setParent(parent)
    if (copied.isDefined(copied.keptValuesJson)) copied.validateModelState()
    copied
  }
}
