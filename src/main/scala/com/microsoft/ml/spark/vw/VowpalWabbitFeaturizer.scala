// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.vw

import com.microsoft.ml.spark.core.contracts.{HasInputCols, HasOutputCol, Wrappable}
import com.microsoft.ml.spark.vw.featurizer._
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, Transformer}
import org.apache.spark.ml.param.{IntParam, ParamMap, StringArrayParam}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.{col, struct, udf}
import org.vowpalwabbit.spark.VowpalWabbitMurmur
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType

import scala.collection.mutable

object VowpalWabbitFeaturizer extends ComplexParamsReadable[VowpalWabbitFeaturizer]

class VowpalWabbitFeaturizer(override val uid: String) extends Transformer
  with HasInputCols with HasOutputCol with HasNumBits with Wrappable with ComplexParamsWritable
{
  def this() = this(Identifiable.randomUID("VowpalWabbitFeaturizer"))

  setDefault(inputCols -> Array())

  val seed = new IntParam(this, "seed", "Hash seed")
  setDefault(seed -> 0)

  def getSeed: Int = $(seed)
  def setSeed(value: Int): this.type = set(seed, value)

  val stringSplitInputCols = new StringArrayParam(this, "stringSplitInputCols",
    "Input cols that should be split at word boundaries")
  setDefault(stringSplitInputCols -> Array())

  def getStringSplitInputCols: Array[String] = $(stringSplitInputCols)
  def setStringSplitInputCols(value: Array[String]): this.type = set(stringSplitInputCols, value)

  private def getAllInputCols = getInputCols ++ getStringSplitInputCols

  private def getFeaturizer(name: String, dataType: DataType, idx: Int, namespaceHash: Int): Featurizer = {
    val stringSplitInputCols = getStringSplitInputCols

    dataType match {
      case DoubleType => new NumericFeaturizer(idx, name, namespaceHash, getMask, r => r.getDouble(idx))
      case FloatType => new NumericFeaturizer(idx, name, namespaceHash, getMask, r => r.getFloat(idx).toDouble)
      case IntegerType => new NumericFeaturizer(idx, name, namespaceHash, getMask, r => r.getInt(idx).toDouble)
      case LongType => new NumericFeaturizer(idx, name, namespaceHash, getMask, r => r.getLong(idx).toDouble)
      case ShortType => new NumericFeaturizer(idx, name, namespaceHash, getMask, r => r.getShort(idx).toDouble)
      case ByteType => new NumericFeaturizer(idx, name, namespaceHash, getMask, r => r.getByte(idx).toDouble)
      case BooleanType => new BooleanFeaturizer(idx, name, namespaceHash, getMask)
      case StringType =>
        if (stringSplitInputCols.contains(name))
          new StringSplitFeaturizer(idx, name, namespaceHash, getMask)
      else new StringFeaturizer(idx, name, namespaceHash, getMask)
      case arr: ArrayType =>
        if (arr.elementType != DataTypes.StringType)
          throw new RuntimeException(s"Unsupported array element type: $dataType")
        new StringArrayFeaturizer(idx, name, namespaceHash, getMask)

      case m: MapType =>
        if (m.keyType != DataTypes.StringType)
          throw new RuntimeException(s"Unsupported map key type: $dataType")

        m.valueType match {
          case StringType => new MapStringFeaturizer(idx, name, namespaceHash, getMask)
          case DoubleType => new MapFeaturizer[Double](idx, name, namespaceHash, getMask, v => v)
          case FloatType => new MapFeaturizer[Float](idx, name, namespaceHash, getMask, v => v.toDouble)
          case IntegerType => new MapFeaturizer[Int](idx, name, namespaceHash, getMask, v => v.toDouble)
          case LongType => new MapFeaturizer[Long](idx, name, namespaceHash, getMask, v => v.toDouble)
          case ShortType => new MapFeaturizer[Short](idx, name, namespaceHash, getMask, v => v.toDouble)
          case ByteType => new MapFeaturizer[Byte](idx, name, namespaceHash, getMask, v => v.toDouble)
          case _ => throw new RuntimeException(s"Unsupported map value type: $dataType")
        }
      case m: Any =>
        if (m == VectorType) // unfortunately the type is private
          new VectorFeaturizer(idx, name, getMask)
        else
          throw new RuntimeException(s"Unsupported data type: $dataType")
    }
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val inputColsList = getAllInputCols
    val namespaceHash: Int = VowpalWabbitMurmur.hash(this.getOutputCol, this.getSeed)

    val fieldSubset = dataset.schema.fields
      .filter(f => inputColsList.contains(f.name))

    val featurizers: Array[Featurizer] = fieldSubset.zipWithIndex
      .map { case (field, idx) => getFeaturizer(field.name, field.dataType, idx, namespaceHash) }

        // TODO: list types
        // BinaryType
        // CalendarIntervalType
        // DateType
        // NullType
        // TimestampType
        // getStruct

    val mode = udf((r: Row) => {
      val indices = mutable.ArrayBuilder.make[Int]
      val values = mutable.ArrayBuilder.make[Double]

      // educated guess on size
      indices.sizeHint(featurizers.length)
      values.sizeHint(featurizers.length)

      // apply all featurizers
      for (f <- featurizers)
        if (!r.isNullAt(f.fieldIdx))
          f.featurize(r, indices, values)

      // sort by indices and remove duplicate values
      // Warning:
      //   - due to SparseVector limitations (which doesn't allow duplicates) we need filter
      //   - VW command line allows for duplicate features with different values (just updates twice)
      val (indicesSorted, valuesSorted) = VectorUtils.sortAndDistinct(indices.result, values.result, getSumCollisions)

      Vectors.sparse(1 << getNumBits, indicesSorted, valuesSorted)
    })

    dataset.toDF.withColumn(getOutputCol, mode.apply(struct(fieldSubset.map(f => col(f.name)): _*)))
  }

  override def copy(extra: ParamMap): VowpalWabbitFeaturizer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    val fieldNames = schema.fields.map(_.name)
    for (f <- getAllInputCols)
      if (!fieldNames.contains(f))
        throw new IllegalArgumentException(s"missing input column $f")

    schema.add(StructField(getOutputCol, VectorType, true))
  }
}
