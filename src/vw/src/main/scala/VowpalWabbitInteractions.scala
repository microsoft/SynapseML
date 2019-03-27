// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import com.microsoft.ml.spark.featurizer._
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{BooleanParam, IntParam, ParamMap, StringArrayParam}
import org.apache.spark.sql.types.{ByteType, DoubleType, FloatType, IntegerType, LongType, ShortType, _}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.{col, struct, udf}
import org.vowpalwabbit.bare.VowpalWabbitMurmur
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.util.Identifiable

import scala.collection.mutable.ArrayBuilder

class VowpalWabbitInteractions(override val uid: String) extends Transformer with HasInputCols with HasOutputCol with HasNumBits
{
  def this() = this(Identifiable.randomUID("VowpalWabbitInteractions"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    val fieldSubset = dataset.schema.fields
      .filter(f => getInputCols.contains(f.name))

    val mask = getMask

    val mode = udf((r: Row) => {

      val numElems = (0 until r.length)
        .map(r.getAs[Vector](_).numNonzeros)
        .fold(1)(_ * _)

      val newIndices = new Array[Int](numElems)
      val newValues = new Array[Double](numElems)

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

  override def copy(extra: ParamMap): VowpalWabbitFeaturizer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    val fieldNames = schema.fields.map(_.name)
    for (f <- getInputCols)
      if (!fieldNames.contains(f))
        throw new IllegalArgumentException("missing input column " + f)

    schema
  }
}
