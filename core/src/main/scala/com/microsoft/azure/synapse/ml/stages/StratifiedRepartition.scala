// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.stages

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.contracts.HasLabelCol
import com.microsoft.azure.synapse.ml.core.schema.DatasetExtensions
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.HasSeed
import org.apache.spark.ml.util._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, rand, row_number}

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
      val numPartitions = getNumPartitions(df)
      val labelToFraction = computeLabelFractions(df, numPartitions)
      val sampled = stratifiedSample(df, labelToFraction)
      roundRobinRepartition(sampled, numPartitions)
    }, dataset.columns.length)
  }

  private def computeLabelFractions(df: DataFrame, numPartitions: Int): Map[Int, Double] = {
    val distinctLabelCounts = df.select(getLabelCol).groupBy(getLabelCol).count().collect()
    val labelToCount = distinctLabelCounts.map(row => (row.getInt(0), row.getLong(1)))
    getMode match {
      case SPConstants.Equal => getEqualLabelCount(labelToCount, numPartitions)
      case SPConstants.Mixed =>
        val equalLabelToCount = getEqualLabelCount(labelToCount, numPartitions)
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
    // An input with no rows yields no per-label frames, and reduce would throw on the empty
    // collection. The RDD implementation returned an empty result here, so preserve that.
    labelDFs.reduceOption(_ union _).getOrElse(emptyDF)
  }

  // Spark exposes no DataFrame API for a plan's partition count. Reading it off the RDD does not
  // launch a job, and counting distinct spark_partition_id values would both cost a full scan and
  // silently drop empty partitions from the target count. A plan can report zero partitions (an
  // empty relation), which repartitionByRange rejects, so keep a floor of one as the
  // RangePartitioner in the previous RDD implementation effectively did.
  private def getNumPartitions(df: DataFrame): Int = math.max(df.rdd.getNumPartitions, 1)

  private def roundRobinRepartition(df: DataFrame, numPartitions: Int): DataFrame = {
    val rrCol = DatasetExtensions.findUnusedColumnName("roundRobinIndex", df)
    // Zero based, so every label starts filling at bucket 0 and each bucket receives the same
    // number of rows of that label to within one.
    val windowSpec = Window.partitionBy(col(getLabelCol)).orderBy(rand(getSeed))
    val withPartition = df.withColumn(rrCol, (row_number().over(windowSpec) - lit(1)) % lit(numPartitions))
    // Range partitioning on a dense key covering exactly [0, numPartitions) sends each bucket to its
    // own partition. Hash partitioning would collide buckets and leave partitions empty, breaking
    // the guarantee that every label appears in every partition.
    withPartition.repartitionByRange(numPartitions, col(rrCol)).drop(rrCol).persist()
  }

  private def getEqualLabelCount(labelToCount: Array[(Int, Long)], numPartitions: Int): Map[Int, Double] = {
    val maxLabelCount = Math.max(labelToCount.map { case (_, count) => count }.max, numPartitions)
    labelToCount.map { case (label, count) => (label, maxLabelCount.toDouble / count) }.toMap
  }

  def transformSchema(schema: StructType): StructType = schema

  def copy(extra: ParamMap): StratifiedRepartition = defaultCopy(extra)
}
