// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.stages

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.contracts.HasLabelCol
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.HasSeed
import org.apache.spark.ml.util._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, max => sqlMax, rand, row_number, spark_partition_id}

/** Constants for <code>StratifiedRepartition</code>. */
object SPConstants {
  val Count = "count"
  val Equal = "equal"
  val Original = "original"
  val Mixed = "mixed"
}

object StratifiedRepartition extends DefaultParamsReadable[DropColumns]

/** <code>StratifiedRepartition</code> repartitions the DataFrame such that each label is selected in each partition.
  * This may be necessary in some cases such as in LightGBM multiclass classification, where it is necessary for
  * at least one instance of each label to be present on each partition.
  */
class StratifiedRepartition(val uid: String) extends Transformer with Wrappable
  with DefaultParamsWritable with HasLabelCol with HasSeed with SynapseMLLogging {
  logClass(FeatureNames.Core)

  def this() = this(Identifiable.randomUID("StratifiedRepartition"))

  def setSeed(value: Long): this.type = set(seed, value)

  val mode = new Param[String](this, "mode",
    "Specify equal to repartition with replacement across all labels, specify " +
      "original to keep the ratios in the original dataset, or specify mixed to use a heuristic")
  setDefault(mode -> SPConstants.Mixed)

  def getMode: String = $(mode)
  def setMode(value: String): this.type = set(mode, value)

  /** @param dataset - The input dataset, to be transformed
    * @return The DataFrame that results from stratified repartitioning
    */
  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame]({
      val df = dataset.toDF()
      val labelToFraction = computeLabelFractions(df)
      val sampled = stratifiedSample(df, labelToFraction)
      val numPartitions = getNumPartitions(df)
      roundRobinRepartition(sampled, numPartitions)
    }, dataset.columns.length)
  }

  private def computeLabelFractions(df: DataFrame): Map[Int, Double] = {
    val distinctLabelCounts = df.select(getLabelCol).groupBy(getLabelCol).count().collect()
    val labelToCount = distinctLabelCounts.map(row => (row.getInt(0), row.getLong(1)))
    getMode match {
      case SPConstants.Equal => getEqualLabelCount(labelToCount, df)
      case SPConstants.Mixed =>
        val equalLabelToCount = getEqualLabelCount(labelToCount, df)
        val normalizedRatio = equalLabelToCount.map { case (_, count) => count }.sum / labelToCount.length
        labelToCount.map { case (label, count) => (label, count / normalizedRatio) }.toMap
      case SPConstants.Original => labelToCount.map { case (label, _) => (label, 1.0) }.toMap
      case _ => throw new Exception(s"Unknown mode specified to StratifiedRepartition: $getMode")
    }
  }

  private def stratifiedSample(df: DataFrame, labelToFraction: Map[Int, Double]): DataFrame = {
    val spark = df.sparkSession
    val emptyDF = spark.createDataFrame(java.util.Collections.emptyList[Row](), df.schema)
    val labelDFs = labelToFraction.map { case (label, fraction) =>
      val labelData = df.filter(col(getLabelCol) === lit(label))
      val wholeReplicates = math.floor(fraction).toInt
      val fractionalPart = fraction - wholeReplicates
      val wholePart = if (wholeReplicates > 0) {
        (1 to wholeReplicates).map(_ => labelData).reduce(_ union _)
      } else emptyDF
      val fracPart = if (fractionalPart > 0) {
        labelData.sample(withReplacement = false, fractionalPart, getSeed)
      } else emptyDF
      wholePart.union(fracPart)
    }
    labelDFs.reduce(_ union _)
  }

  private def getNumPartitions(df: DataFrame): Int =
    df.select(spark_partition_id().as("_pid")).agg(sqlMax("_pid")).head().getInt(0) + 1

  private def roundRobinRepartition(df: DataFrame, numPartitions: Int): DataFrame = {
    val windowSpec = Window.partitionBy(col(getLabelCol)).orderBy(rand(getSeed))
    val withPartition = df.withColumn("_rr_idx", row_number().over(windowSpec) % lit(numPartitions))
    withPartition.repartitionByRange(numPartitions, col("_rr_idx")).drop("_rr_idx")
  }

  private def getEqualLabelCount(labelToCount: Array[(Int, Long)], df: DataFrame): Map[Int, Double] = {
    val numPartitions = getNumPartitions(df)
    val maxLabelCount = Math.max(labelToCount.map { case (_, count) => count }.max, numPartitions)
    labelToCount.map { case (label, count) => (label, maxLabelCount.toDouble / count) }.toMap
  }

  def transformSchema(schema: StructType): StructType = schema

  def copy(extra: ParamMap): StratifiedRepartition = defaultCopy(extra)
}
