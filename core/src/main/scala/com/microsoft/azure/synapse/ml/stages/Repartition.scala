// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.stages

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

object Repartition extends DefaultParamsReadable[Repartition]

/** Partitions the dataset into n partitions
  * @param uid The id of the module
  */
class Repartition(val uid: String) extends Transformer with Wrappable with DefaultParamsWritable with SynapseMLLogging {
  logClass(FeatureNames.Core)

  def this() = this(Identifiable.randomUID("Repartition"))

  val disable = new BooleanParam(this, "disable",
    "Whether to disable repartitioning (so that one can turn it off for evaluation)")

  def getDisable: Boolean = $(disable)

  def setDisable(value: Boolean): this.type = set(disable, value)

  setDefault(disable->false)

  /** Number of partitions. Default is 10
    * @group param
    */
  val n: IntParam = new IntParam(this, "n", "Number of partitions", ParamValidators.gt[Int](0))

  /** @group getParam */
  final def getN: Int = $(n)

  /** @group setParam */
  def setN(value: Int): this.type = set(n,value)

  /** Partition the dataset
    * @param dataset The data to be partitioned
    * @return partitoned DataFrame
    */
  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame]({
      if (getDisable)
        dataset.toDF
      else if (getN < dataset.rdd.getNumPartitions)
        dataset.coalesce(getN).toDF()
      else
        dataset.sqlContext.createDataFrame(
          dataset.rdd.repartition(getN).asInstanceOf[RDD[Row]],
          dataset.schema)
    }, dataset.columns.length)
  }

  def transformSchema(schema: StructType): StructType = {
    schema
  }

  def copy(extra: ParamMap): this.type = defaultCopy(extra)

}
