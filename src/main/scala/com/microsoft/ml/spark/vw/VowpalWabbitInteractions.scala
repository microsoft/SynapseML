// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.vw

import com.microsoft.ml.spark.core.contracts.{HasInputCols, HasOutputCol, Wrappable}
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, Transformer}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.{col, struct, udf}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType

object VowpalWabbitInteractions extends ComplexParamsReadable[VowpalWabbitInteractions]

/**
  * This transformer is not intended to be used with VW classifier or regressor, but rather to bring
  * sparse interaction concept to other SparkML learners (e.g. LR).
  */
class VowpalWabbitInteractions(override val uid: String) extends Transformer
  with HasInputCols with HasOutputCol with HasNumBits with Wrappable with ComplexParamsWritable
{
  def this() = this(Identifiable.randomUID("VowpalWabbitInteractions"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    val fieldSubset = dataset.schema.fields
      .filter(f => getInputCols.contains(f.name))

    val mask = getMask

    val mode = udf((r: Row) => {

      // compute the final number of features
      val numElems = (0 until r.length)
        .map(r.getAs[Vector](_).numNonzeros).product

      val newIndices = new Array[Int](numElems)
      val newValues = new Array[Double](numElems)

      // build interaction features using FNV-1
      val fnvPrime = 16777619
      var i = 0

      def interact(idx: Int, value: Double, ns: Int): Unit = {
        if (ns == r.size) {
          newIndices(i) += mask & idx
          newValues(i) += value

          i += 1
        }
        else {
          val idx1 = idx * fnvPrime

          r.getAs[Vector](ns).foreachActive { case (idx2, value2) =>
            interact(idx1 ^ idx2, value * value2, ns + 1)
          }
        }
      }

      // start the recursion
      interact(0, 1, 0)

      val (indicesSorted, valuesSorted) = VectorUtils.sortAndDistinct(newIndices, newValues, getSumCollisions)

      Vectors.sparse(1 << getNumBits, indicesSorted, valuesSorted)
    })

    dataset.toDF.withColumn(getOutputCol, mode.apply(struct(fieldSubset.map(f => col(f.name)): _*)))
  }

  override def transformSchema(schema: StructType): StructType = {
    val fieldNames = schema.fields.map(_.name)
    for (f <- getInputCols)
      if (!fieldNames.contains(f))
        throw new IllegalArgumentException("missing input column " + f)
      else {
        val fieldType = schema.fields(schema.fieldIndex(f)).dataType

        if (fieldType != VectorType)
          throw new IllegalArgumentException("column " + f + " must be of type Vector but is " + fieldType.typeName)
      }

    schema.add(StructField(getOutputCol, VectorType, true))
  }

  override def copy(extra: ParamMap): VowpalWabbitFeaturizer = defaultCopy(extra)
}
