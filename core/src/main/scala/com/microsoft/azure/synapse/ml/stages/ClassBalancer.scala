// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.stages

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.contracts.{HasInputCol, HasOutputCol}
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import com.microsoft.azure.synapse.ml.param.DataFrameParam
import org.apache.spark.ml.param.{BooleanParam, ParamMap}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, Estimator, Model}
import org.apache.spark.sql.functions.{broadcast, col, lit, max}
import org.apache.spark.sql.types.{DoubleType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

/** An estimator that calculates the weights for balancing a dataset.
  * For example, if the negative class is half the size of the positive class, the weights will be
  * 2 for rows with negative classes and 1 for rows with positive classes.
  * these weights can be used in weighted classifiers and regressors to correct for heavily
  * skewed datasets. The inputCol should be the labels of the classes, and the output col will
  * be the requisite weights.
  *
  * @param uid
  */
class ClassBalancer(override val uid: String) extends Estimator[ClassBalancerModel]
  with DefaultParamsWritable with HasInputCol with HasOutputCol with Wrappable with SynapseMLLogging {
  logClass(FeatureNames.Core)

  def this() = this(Identifiable.randomUID("ClassBalancer"))

  setDefault(outputCol -> "weight")

  val broadcastJoin = new BooleanParam(this, "broadcastJoin",
    "Whether to broadcast the class to weight mapping to the worker")

  def getBroadcastJoin: Boolean = $(broadcastJoin)

  def setBroadcastJoin(value: Boolean): this.type = set(broadcastJoin, value)

  setDefault(broadcastJoin -> true)

  def fit(dataset: Dataset[_]): ClassBalancerModel = {
    logFit({
      val counts = dataset.toDF().select(getInputCol).groupBy(getInputCol).count()
      val maxVal = counts.agg(max("count")).collect().head.getLong(0)
      val weights = counts
        .withColumn(getOutputCol, lit(maxVal) / col("count"))
        .select(getInputCol, getOutputCol)
      new ClassBalancerModel()
        .setInputCol(getInputCol)
        .setOutputCol(getOutputCol)
        .setWeights(weights)
        .setBroadcastJoin(getBroadcastJoin)
        .setParent(this)
    }, dataset.columns.length)
  }

  override def copy(extra: ParamMap): Estimator[ClassBalancerModel] = defaultCopy(extra)

  def transformSchema(schema: StructType): StructType = schema.add(getOutputCol, DoubleType)

}

object ClassBalancer extends DefaultParamsReadable[ClassBalancer]

class ClassBalancerModel(val uid: String) extends Model[ClassBalancerModel]
  with ComplexParamsWritable with Wrappable with HasInputCol with HasOutputCol with SynapseMLLogging {
  logClass(FeatureNames.Core)

  def this() = this(Identifiable.randomUID("ClassBalancerModel"))

  val weights = new DataFrameParam(this, "weights", "the dataframe of weights")

  def getWeights: DataFrame = $(weights)

  def setWeights(v: DataFrame): this.type = set(weights, v)

  val broadcastJoin = new BooleanParam(this, "broadcastJoin", "whether to broadcast join")

  def getBroadcastJoin: Boolean = $(broadcastJoin)

  def setBroadcastJoin(v: Boolean): this.type = set(broadcastJoin, v)

  override def copy(extra: ParamMap): ClassBalancerModel = defaultCopy(extra)

  def transformSchema(schema: StructType): StructType = schema.add(getOutputCol, DoubleType)

  def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame]({
      val w = if (getBroadcastJoin) {
        broadcast(getWeights)
      } else {
        getWeights
      }
      dataset.toDF().join(w, getInputCol)
    }, dataset.columns.length)
  }

}

object ClassBalancerModel extends ComplexParamsReadable[ClassBalancerModel]
