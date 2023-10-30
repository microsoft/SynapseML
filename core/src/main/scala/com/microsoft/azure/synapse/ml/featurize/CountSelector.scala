// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.featurize

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.contracts.{HasInputCol, HasOutputCol}
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import scala.collection.immutable.BitSet

object CountSelector extends DefaultParamsReadable[CountSelector]

/** Drops vector indicies with no nonzero data. */
class CountSelector(override val uid: String) extends Estimator[CountSelectorModel]
  with Wrappable with DefaultParamsWritable with HasInputCol with HasOutputCol with SynapseMLLogging {
  logClass(FeatureNames.Featurize)

  def this() = this(Identifiable.randomUID("CountBasedFeatureSelector"))

  private def toBitSet(indices: Array[Int]): BitSet = {
    indices.foldLeft(BitSet())((bitset, index) => bitset + index)
  }

  override def fit(dataset: Dataset[_]): CountSelectorModel = {
    logFit({
      val encoder = Encoders.kryo[BitSet]
      val slotsToKeep = dataset.select(getInputCol)
        .map(row => toBitSet(row.getAs[Vector](0).toSparse.indices))(encoder)
        .reduce(_ | _)
        .toArray
      new CountSelectorModel()
        .setIndices(slotsToKeep)
        .setInputCol(getInputCol)
        .setOutputCol(getOutputCol)
    }, dataset.columns.length)
  }

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType =
    schema.add(getOutputCol, VectorType)

}

object CountSelectorModel extends DefaultParamsReadable[CountSelectorModel]

class CountSelectorModel(val uid: String) extends Model[CountSelectorModel]
  with HasInputCol with HasOutputCol with DefaultParamsWritable with Wrappable with SynapseMLLogging {
  logClass(FeatureNames.Featurize)

  def this() = this(Identifiable.randomUID("CountBasedFeatureSelectorModel"))

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  val indices = new IntArrayParam(this, "indices",
    "An array of indices to select features from a vector column." +
      " There can be no overlap with names.")

  /** @group getParam */
  def getIndices: Array[Int] = $(indices)

  /** @group setParam */
  def setIndices(value: Array[Int]): this.type = set(indices, value)

  private def getModel: VectorSlicer = {
    new VectorSlicer().setInputCol(getInputCol).setOutputCol(getOutputCol).setIndices(getIndices)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame](
      getModel.transform(dataset),
      dataset.columns.length
    )
  }

  override def transformSchema(schema: StructType): StructType = {
    getModel.transformSchema(schema)
  }

}
