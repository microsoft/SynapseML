// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.stages

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.contracts.HasLabelCol
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import org.apache.spark.RangePartitioner
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.HasSeed
import org.apache.spark.ml.util._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset}

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
      // Count unique values in label column
      val distinctLabelCounts = dataset.select(getLabelCol).groupBy(getLabelCol).count().collect()
      val labelToCount = distinctLabelCounts.map(row => (row.getInt(0), row.getLong(1)))
      val labelToFraction =
        getMode match {
          case SPConstants.Equal => getEqualLabelCount(labelToCount, dataset)
          case SPConstants.Mixed =>
            val equalLabelToCount = getEqualLabelCount(labelToCount, dataset)
            val normalizedRatio = equalLabelToCount.map { case (label, count) => count }.sum / labelToCount.length
            labelToCount.map { case (label, count) => (label, count / normalizedRatio) }.toMap
          case SPConstants.Original => labelToCount.map { case (label, count) => (label, 1.0) }.toMap
          case _ => throw new Exception(s"Unknown mode specified to StratifiedRepartition: $getMode")
        }
      val labelColIndex = dataset.schema.fieldIndex(getLabelCol)
      val spdata = dataset.toDF().rdd.keyBy(row => row.getInt(labelColIndex))
        .sampleByKeyExact(true, labelToFraction, getSeed)
        .mapPartitions(keyToRow => keyToRow.zipWithIndex.map { case ((key, row), index) => (index, row) })
      val rangePartitioner = new RangePartitioner(dataset.rdd.getNumPartitions, spdata)
      val rspdata = spdata.partitionBy(rangePartitioner).mapPartitions(keyToRow =>
        keyToRow.map { case (key, row) => row }).persist()
      dataset.sqlContext.createDataFrame(rspdata, dataset.schema)
    }, dataset.columns.length)
  }

  private def getEqualLabelCount(labelToCount: Array[(Int, Long)], dataset: Dataset[_]): Map[Int, Double] = {
    val maxLabelCount = Math.max(labelToCount.map { case (label, count) => count }.max, dataset.rdd.getNumPartitions)
    labelToCount.map { case (label, count) => (label, maxLabelCount.toDouble / count) }.toMap
  }

  def transformSchema(schema: StructType): StructType = schema

  def copy(extra: ParamMap): StratifiedRepartition = defaultCopy(extra)
}
