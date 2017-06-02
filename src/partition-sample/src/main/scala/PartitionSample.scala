// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.param._
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types._

object PSConstants {
    final val ModeRS = "RandomSample"
    final val ModeHead = "Head"
    final val ModeATP = "AssignToPartition"

    final val rsAbsolute = "Absolute"
    final val rsPercent = "Percentage"

    final val newColDefault = "Partition"
}

trait PartitionSampleParams extends MMLParams {

  /* Mode: {RandomSample|AssignToPartition|Head}
        - RS:   {Absolute|Percentage, Seed}
            - Absolute: {Count}
            - Percentage: {Percent}
        - ATP:  {Seed, numParts, newColName}
        - Head: {Count}
  */
  // TODO: Convert to Enum
  final val mode = StringParam(this, "mode", "AssignToPartition, RandomSample, or Head")
  setDefault(mode, PSConstants.ModeRS)
  final def getMode: String = $(mode)
  def setMode(value: String): this.type = set(mode, value)

  // TODO: Convert to Enum
  // Relevant on Mode = RS
  final val rsMode = StringParam(this, "rsMode", "Absolute or Percentage", PSConstants.rsPercent)
  final def getRandomSampleMode: String = $(rsMode)
  def setRandomSampleMode(value: String): this.type = set(rsMode, value)

  // Relevant on Mode = RS|ATP
  // TODO: We need to create Option[Int] idiom for params
  final val seed = LongParam(this, "seed", "seed for random ops", -1L)
  final def getSeed: Long = $(seed)
  def setSeed(value: Long): this.type = set(seed, value)

  // Relevant on RSMode = Percentage
  final val percent = DoubleParam(this, "percent", "percent of rows to return", 0.01)
  final def getPercent: Double = $(percent)
  def setPercent(value: Double): this.type = set(percent, value)

  // Relevant on Mode = Head | RSMode = Absolute
  final val count = LongParam(this, "count", "number of rows to return", 1000L)
  final def getCount: Long = $(count)
  def setCount(value: Long): this.type = set(count, value)

  // Relevant on Mode = ATP
  final val newColName = StringParam(this, "newColName", "name of the partition column", PSConstants.newColDefault)
  final def getNewColName: String = $(newColName)
  def setNewColName(value: String): this.type = set(newColName, value)

  // Relevant on Mode = ATP
  final val numParts = IntParam(this, "numParts", "number of partitions", 10)
  final def getNumParts: Int = $(numParts)
  def setNumParts(value: Int): this.type = set(numParts, value)

  protected def validateAndTransformSchema(schema: StructType): StructType = {
    if (Seq(PSConstants.ModeHead, PSConstants.ModeRS).contains($(mode)))
      schema
    else
      ??? // schema + newCol
  }
}

object PartitionSample extends DefaultParamsReadable[PartitionSample]

// UID should be overridden by driver for controlled identification at the DAG level
sealed class PartitionSample(override val uid: String)
  extends Transformer
  with PartitionSampleParams {

  def this() = this(Identifiable.randomUID("PartitionSample"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    $(mode) match {
      case PSConstants.ModeHead => dataset.limit(
        if ($(count) <= 2000000000) $(count).toInt else throw new Exception("Head limit 2b rows")).toDF
      case PSConstants.ModeRS => randomSample(dataset, $(rsMode), $(seed)).toDF
      case PSConstants.ModeATP => dataset.withColumn($(newColName), /* broken */ dataset.col("input"))
      case _ => ???
    }
  }

  def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  def copy(extra: ParamMap): PartitionSample = defaultCopy(extra)

  private def randomSample(
    ds: Dataset[_],
    rsMode: String,
    seed: Long,
    replace: Boolean = false): Dataset[_] = {
        val frac = rsMode match {
            case PSConstants.rsPercent => $(percent)
            case PSConstants.rsAbsolute => $(count).toDouble / ds.count
            case _ => ???
        }
        println(s"Sampling ${ds.count} rows by ${frac * 100}% to get ~${ds.count * frac} rows")
        return ds.sample(replace, frac, seed)
  }

}
