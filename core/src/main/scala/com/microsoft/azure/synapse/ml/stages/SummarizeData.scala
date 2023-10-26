// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.stages

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{BooleanParam, DoubleParam, ParamMap}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

trait SummarizeDataParams extends Wrappable with DefaultParamsWritable {

  /** Compute count statistics. Default is true.
    * @group param
    */
  final val counts: BooleanParam = new BooleanParam(this, "counts", "Compute count statistics")
  setDefault(counts -> true)

  /** @group getParam */
  final def getCounts: Boolean = $(counts)

  /** @group setParam */
  def setCounts(value: Boolean): this.type = set(counts, value)

  /** Compute basic statistics. Default is true.
    * @group param
    */
  final val basic: BooleanParam = new BooleanParam(this, "basic", "Compute basic statistics")
  setDefault(basic, true)

  /** @group getParam */
  final def getBasic: Boolean = $(basic)

  /** @group setParam */
  def setBasic(value: Boolean): this.type = set(basic, value)

  /** Compute sample statistics. Default is true.
    * @group param
    */
  final val sample: BooleanParam = new BooleanParam(this, "sample", "Compute sample statistics")
  setDefault(sample, true)

  /** @group getParam */
  final def getSample: Boolean = $(sample)

  /** @group setParam */
  def setSample(value: Boolean): this.type = set(sample, value)

  /** Compute percentiles. Default is true
    * @group param
    */
  final val percentiles: BooleanParam = new BooleanParam(this, "percentiles", "Compute percentiles")
  setDefault(percentiles, true)

  /** @group getParam */
  final def getPercentiles: Boolean = $(percentiles)

  /** @group setParam */
  def setPercentiles(value: Boolean): this.type = set(percentiles, value)

  /** Threshold for quantiles - 0 is exact
    * @group param
    */
  final val errorThreshold: DoubleParam =
    new DoubleParam(this, "errorThreshold", "Threshold for quantiles - 0 is exact")
  setDefault(errorThreshold, 0.0)

  /** @group getParam */
  final def getErrorThreshold: Double = $(errorThreshold)

  /** @group setParam */
  def setErrorThreshold(value: Double): this.type = set(errorThreshold, value)

  protected def validateAndTransformSchema(schema: StructType): StructType = {
    val columns = ListBuffer(SummarizeData.FeatureColumn)
    if ($(counts)) columns ++= SummarizeData.CountFields
    if ($(basic)) columns ++= SummarizeData.BasicFields
    if ($(sample)) columns ++= SummarizeData.SampleFields
    if ($(percentiles)) columns ++= SummarizeData.PercentilesFields
    StructType(columns)
  }
}

// UID should be overridden by driver for controlled identification at the DAG level
/** Compute summary statistics for the dataset. The following statistics are computed:
  * - counts
  * - basic
  * - sample
  * - percentiles
  * - errorThreshold - error threshold for quantiles
  * @param uid The id of the module
  */
class SummarizeData(override val uid: String)
  extends Transformer
    with SummarizeDataParams with SynapseMLLogging {
  logClass(FeatureNames.Core)

  def this() = this(Identifiable.randomUID("SummarizeData"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame]({

      val df = dataset.toDF()
      // Some of these statistics are bad to compute
      df.persist(StorageLevel.MEMORY_ONLY)

      val subFrames = ListBuffer[DataFrame]()
      if ($(counts)) subFrames += computeCounts(df)
      if ($(basic)) subFrames += curriedBasic(df)
      if ($(sample)) subFrames += sampleStats(df)
      if ($(percentiles)) subFrames += curriedPerc(df)

      df.unpersist(false)

      val base = createJoinBase(df)
      subFrames.foldLeft(base) { (z, dfi) => z.join(dfi, SummarizeData.FeatureColumnName) }
    }, dataset.columns.length)
  }

  def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  def copy(extra: ParamMap): SummarizeData = defaultCopy(extra)

  private def computeCounts = computeOnAll(computeCountsImpl, SummarizeData.CountFields)

  private def computeCountsImpl(col: String, df: DataFrame): Array[Double] = {
    val column = df.col(col)
    val dataType = df.schema(col).dataType
    val mExpr = isnull(column) || (if (dataType.equals(BooleanType)) isnan(column.cast(DoubleType)) else isnan(column))

    val countMissings = df.where(mExpr).count().toDouble
    // approxCount returns Long which > Double!
    val dExpr = approx_count_distinct(column)
    val distinctCount = df.select(dExpr).first.getLong(0).toDouble
    Array(df.count() - countMissings, distinctCount, countMissings)
  }

  private def sampleStats = computeOnNumeric(sampleStatsImpl, SummarizeData.SampleFields)

  private def sampleStatsImpl(col: String, df: DataFrame): Array[Double] = {
    val column = df.col(col)
    val k = kurtosis(column)
    val sk = skewness(column)
    val v = variance(column)
    val sd = stddev(column)
    df.select(v, sd, sk, k).first.toSeq.map(_.asInstanceOf[Double]).toArray
  }

  private def curriedBasic = {
    val quants = SummarizeData.BasicQuantiles
    computeOnNumeric(quantStub(quants, $(errorThreshold)), SummarizeData.BasicFields)
  }

  private def curriedPerc = {
    val quants = SummarizeData.PercentilesQuantiles
    computeOnNumeric(quantStub(quants, $(errorThreshold)), SummarizeData.PercentilesFields)
  }

  private def quantStub(vals: Array[Double], err: Double) =
    (cn: String, df: DataFrame) => df.stat.approxQuantile(cn, vals, err)

  private def computeOnNumeric = computeColumnStats(sf => sf.dataType.isInstanceOf[NumericType]) _

  private def computeOnAll = computeColumnStats(sf => true) _

  private def allNaNs(l: Int): Array[Double] = Array.fill(l)(Double.NaN)

  private def createJoinBase(df: DataFrame) = computeColumnStats(sf => false)((cn, df) => Array(), List())(df)

  private def computeColumnStats
  (p: StructField => Boolean)
  (statFunc: (String, DataFrame) => Array[Double], newColumns: Seq[StructField])
  (df: DataFrame): DataFrame = {
    val emptyRow = allNaNs(newColumns.length)
    val outList = df.schema.map(col => (col.name, if (p(col)) statFunc(col.name, df) else emptyRow))
    val rows = outList.map { case (n, r) => Row.fromSeq(n +: r) }
    val schema = SummarizeData.FeatureColumn +: newColumns
    df.sparkSession.createDataFrame(rows.asJava, StructType(schema))
  }

}

object SummarizeData extends DefaultParamsReadable[SummarizeData] {

  object Statistic extends Enumeration {
    type Statistic = Value
    val Counts, Basic, Sample, Percentiles = Value
  }

  final val FeatureColumnName = "Feature"
  final val FeatureColumn = StructField(FeatureColumnName, StringType, false)

  final val PercentilesQuantiles = Array(0.005, 0.01, 0.05, 0.95, 0.99, 0.995)
  final val PercentilesFields = List(
    StructField("P0_5", DoubleType, true),
    StructField("P1", DoubleType, true),
    StructField("P5", DoubleType, true),
    StructField("P95", DoubleType, true),
    StructField("P99", DoubleType, true),
    StructField("P99_5", DoubleType, true))

  final val SampleFields = List(
    StructField("Sample_Variance", DoubleType, true),
    StructField("Sample_Standard_Deviation", DoubleType, true),
    StructField("Sample_Skewness", DoubleType, true),
    StructField("Sample_Kurtosis", DoubleType, true))

  final val BasicQuantiles = Array(0, 0.25, 0.5, 0.75, 1)
  final val BasicFields = List(
    StructField("Min", DoubleType, true),
    StructField("1st_Quartile", DoubleType, true),
    StructField("Median", DoubleType, true),
    StructField("3rd_Quartile", DoubleType, true),
    StructField("Max", DoubleType, true)
    //TODO: StructField("Range", DoubleType, true),
    //TODO: StructField("Mean", DoubleType, true),
    //TODO: StructField("Mean Deviation", DoubleType, true),
    // Mode is JSON Array of modes - needs a little special treatment
    //TODO: StructField("Mode", StringType, true))
  )

  final val CountFields = List(
    StructField("Count", DoubleType, false),
    StructField("Unique_Value_Count", DoubleType, false),
    StructField("Missing_Value_Count", DoubleType, false))
}
