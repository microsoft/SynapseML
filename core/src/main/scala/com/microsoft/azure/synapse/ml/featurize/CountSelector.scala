// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.featurize

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.contracts.{HasInputCol, HasOutputCol}
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._

import scala.collection.immutable.BitSet

object CountSelector extends DefaultParamsReadable[CountSelector]

/** Drops vector indices with no nonzero data.
  *
  * If no indices remain, the fitted model emits a zero-width sparse vector with size-0 metadata
  * for non-null inputs and preserves nulls.
  */
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

  private def hasEmptySelection: Boolean = getIndices.isEmpty

  private def emptyOutputMetadata: Metadata = new AttributeGroup(getOutputCol, 0).toMetadata()

  private def emptyOutputField: StructField =
    StructField(getOutputCol, VectorType, nullable = true, emptyOutputMetadata)

  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame](
      if (hasEmptySelection) {
        transformSchema(dataset.schema)
        val emptySelectionVector = Vectors.sparse(0, Array.empty[Int], Array.empty[Double])
        val emptyVector = udf { vector: Vector =>
          if (vector eq null) null else emptySelectionVector  // scalastyle:ignore null
        }
        dataset.withColumn(getOutputCol, emptyVector(dataset(getInputCol)).as(getOutputCol, emptyOutputMetadata))
      } else {
        getModel.transform(dataset)
      },
      dataset.columns.length
    )
  }

  override def transformSchema(schema: StructType): StructType = {
    if (hasEmptySelection) {
      require(schema(getInputCol).dataType == VectorType,
        s"Input column ${getInputCol} must be of type vector.")
      require(!schema.fieldNames.contains(getOutputCol),
        s"Output column ${getOutputCol} already exists.")
      schema.add(emptyOutputField)
    } else {
      getModel.transformSchema(schema)
    }
  }

}
