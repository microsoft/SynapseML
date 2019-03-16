// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import com.microsoft.ml.spark.featurizer._
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{IntParam, ParamMap}
import org.apache.spark.sql.types.{ByteType, DoubleType, FloatType, IntegerType, LongType, ShortType, _}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.{col, struct, udf}
import org.vowpalwabbit.bare.VowpalWabbitMurmur
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.util.Identifiable

import scala.collection.mutable.ArrayBuffer

class VowpalWabbitFeaturizer(override val uid:String) extends Transformer with HasInputCols with HasOutputCol
{
  def this() = this(Identifiable.randomUID("VowpalWabbitFeaturizer"))

  val seed = new IntParam(this, "seed", "Hash seed")
  setDefault(seed -> 0)

  def getSeed: Int = $(seed)
  def setSeed(value: Int): this.type = set(seed, value)

  private def sortAndDistinct(indices:ArrayBuffer[Int], values:ArrayBuffer[Double]): (Array[Int], Array[Double]) = {
    if (indices.length == 0)
      return (indices.toArray, values.toArray)

    // get a sorted list of indices
    val argsort = (0 until indices.length)
      .sortWith(indices(_) < indices(_))
      .toArray

    val indicesSorted = new Array[Int](indices.length)
    val valuesSorted = new Array[Double](indices.length)

    var previousIndex = indicesSorted(0) = indices(argsort(0))
    valuesSorted(0) = values(argsort(0))

    // in-place de-duplicate
    var j = 1
    for (i <- 1 until indices.length) {
      val index = indices(argsort(i))

      if (index != previousIndex) {
        previousIndex = indicesSorted(j) = index
        valuesSorted(j) = values(argsort(i))

        j += 1
      }
    }

    if (j == indices.length)
      (indicesSorted, valuesSorted)
    else {
      // just in case we found duplicates, let compact the array
      val indicesCompacted = new Array[Int](j)
      val valuesCompacted = new Array[Double](j)

      Array.copy(indicesSorted, 0, indicesCompacted, 0, j)
      Array.copy(valuesSorted, 0, valuesCompacted, 0, j)

      (indicesCompacted, valuesCompacted)
    }
  }

  private def getFeaturizer(name:String, dataType:DataType, idx:Int, namespaceHash:Int): Featurizer =
    dataType match {
      case DoubleType => new NumericFeaturizer(idx, name, namespaceHash, r => r.getDouble(idx))
      case FloatType => new NumericFeaturizer(idx, name, namespaceHash, r => r.getFloat(idx).toDouble)
      case IntegerType => new NumericFeaturizer(idx, name, namespaceHash, r => r.getInt(idx).toDouble)
      case LongType => new NumericFeaturizer(idx, name, namespaceHash, r => r.getLong(idx).toDouble)
      case ShortType => new NumericFeaturizer(idx, name, namespaceHash, r => r.getShort(idx).toDouble)
      case ByteType => new NumericFeaturizer(idx, name, namespaceHash, r => r.getByte(idx).toDouble)
      case BooleanType => new BooleanFeaturizer(idx, name, namespaceHash)
      case StringType => new StringFeaturizer(idx, name, namespaceHash)
      case arr: ArrayType => {
        if (arr.elementType != DataTypes.StringType)
          throw new RuntimeException(s"Unsupported array element type: ${dataType}")

        new StringArrayFeaturizer(idx, name, namespaceHash)
      }
      case m: MapType => {
        if (m.keyType != DataTypes.StringType)
          throw new RuntimeException(s"Unsupported map key type: ${dataType}")

        m.valueType match {
          case StringType => new MapStringFeaturizer(idx, name, namespaceHash)
          case DoubleType => new MapFeaturizer[Double](idx, name, namespaceHash, v => v)
          case FloatType => new MapFeaturizer[Float](idx, name, namespaceHash, v => v.toDouble)
          case IntegerType => new MapFeaturizer[Int](idx, name, namespaceHash, v => v.toDouble)
          case LongType => new MapFeaturizer[Long](idx, name, namespaceHash, v => v.toDouble)
          case ShortType => new MapFeaturizer[Short](idx, name, namespaceHash, v => v.toDouble)
          case ByteType => new MapFeaturizer[Byte](idx, name, namespaceHash, v => v.toDouble)
          case _ => throw new RuntimeException(s"Unsupported map value type: ${dataType}")
        }
      }
      case _ => throw new RuntimeException(s"Unsupported data type: ${dataType}")
    }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val inputColsList= getInputCols
    val namespaceHash: Int = VowpalWabbitMurmur.hash(this.getOutputCol, this.getSeed)

    val featurizers:Array[Featurizer] = dataset.schema.fields
      .filter(f => inputColsList.contains(f.name))
      .zipWithIndex
      .map { case (field, idx) => getFeaturizer(field.name, field.dataType, idx, namespaceHash) }

        // TODO: list types
        // BinaryType
        // CalendarIntervalType
        // DateType
        // NullType
        // TimestampType
        // getStruct

    val mode = udf((r: Row) => {
      // educated guess on size
      val indices = new ArrayBuffer[Int](featurizers.length)
      val values = new ArrayBuffer[Double](featurizers.length)

      // apply all featurizers
      for (f <- featurizers)
        if (!r.isNullAt(f.fieldIdx))
          f.featurize(r, indices, values)

      // sort by indices and remove duplicate values
      // Warning:
      //   - due to SparseVector limitations (which doesn't allow duplicates) we need filter
      //   - VW command line allows for duplicate features with different values (just updates twice)
      val (indicesSorted, valuesSorted) = sortAndDistinct(indices, values)

      Vectors.sparse(Featurizer.maxIndexMask, indicesSorted, valuesSorted)
    })

    dataset.toDF.withColumn(getOutputCol, mode.apply(struct(getInputCols.map(col) :_ *)))
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