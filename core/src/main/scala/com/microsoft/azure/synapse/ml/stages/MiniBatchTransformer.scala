// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.stages

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import com.microsoft.azure.synapse.ml.param.TransformerParam
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.collection.mutable

trait MiniBatchBase extends Transformer with DefaultParamsWritable with Wrappable with SynapseMLLogging {
  def transpose(nestedSeq: Seq[Seq[Any]]): Seq[Seq[Any]] = {

    val innerLength = nestedSeq.head.length
    assert(nestedSeq.forall(_.lengthCompare(innerLength) == 0))
    (0 until innerLength).map(i => nestedSeq.map(innerSeq => innerSeq(i)))
  }

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    StructType(schema.map(f => StructField(f.name, ArrayType(f.dataType))))
  }

  def getBatcher(it: Iterator[Row]): Iterator[List[Row]]

  def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame]({
      val outputSchema = transformSchema(dataset.schema)
      implicit val outputEncoder: ExpressionEncoder[Row] = RowEncoder(outputSchema)
      dataset.toDF().mapPartitions { it =>
        if (it.isEmpty) {
          it
        } else {
          getBatcher(it).map {
            listOfRows =>
              new GenericRowWithSchema(transpose(listOfRows.map(r => r.toSeq)).toArray, outputSchema)
          }
        }
      }
    }, dataset.columns.length)
  }
}

object DynamicMiniBatchTransformer extends DefaultParamsReadable[DynamicMiniBatchTransformer]

class DynamicMiniBatchTransformer(val uid: String)
    extends MiniBatchBase with SynapseMLLogging {
  logClass(FeatureNames.Core)

  val maxBatchSize: Param[Int] = new IntParam(
    this, "maxBatchSize", "The max size of the buffer")

  /** @group getParam */
  def getMaxBatchSize: Int = $(maxBatchSize)

  /** @group setParam */
  def setMaxBatchSize(value: Int): this.type = set(maxBatchSize, value)

  def this() = this(Identifiable.randomUID("DynamicMiniBatchTransformer"))

  setDefault(maxBatchSize -> Integer.MAX_VALUE)

  override def getBatcher(it: Iterator[Row]): DynamicBufferedBatcher[Row] =
    new DynamicBufferedBatcher(it, getMaxBatchSize)

}

object TimeIntervalMiniBatchTransformer extends DefaultParamsReadable[TimeIntervalMiniBatchTransformer]

class TimeIntervalMiniBatchTransformer(val uid: String)
  extends MiniBatchBase with SynapseMLLogging {
  logClass(FeatureNames.Core)

  val maxBatchSize: Param[Int] = new IntParam(
    this, "maxBatchSize", "The max size of the buffer")

  /** @group getParam */
  def getMaxBatchSize: Int = $(maxBatchSize)

  /** @group setParam */
  def setMaxBatchSize(value: Int): this.type = set(maxBatchSize, value)

  val millisToWait: Param[Int] = new IntParam(
    this, "millisToWait", "The time to wait before constructing a batch")

  /** @group getParam */
  def getMillisToWait: Int = $(millisToWait)

  /** @group setParam */
  def setMillisToWait(value: Int): this.type = set(millisToWait, value)

  def this() = this(Identifiable.randomUID("DynamicMiniBatchTransformer"))

  setDefault(maxBatchSize -> Integer.MAX_VALUE)

  override def getBatcher(it: Iterator[Row]): TimeIntervalBatcher[Row] =
    new TimeIntervalBatcher(it, getMillisToWait, getMaxBatchSize)

}

trait HasMiniBatcher extends Params {
  /** Size of minibatches. Must be greater than 0; default is 10
    * @group param
    */
  val miniBatcher: TransformerParam = new TransformerParam(this, "miniBatcher", "Minibatcher to use", {
    case _: MiniBatchBase => true
    case _ => false
  })

  /** @group setParam */
  def setMiniBatcher(value: MiniBatchBase): this.type = set(miniBatcher, value)

  /** @group getParam */
  def getMiniBatcher: MiniBatchBase = $(miniBatcher).asInstanceOf[MiniBatchBase]

  def setMiniBatchSize(n: Int): this.type = setMiniBatcher(getMiniBatcher match {
    case d: DynamicMiniBatchTransformer => d.setMaxBatchSize(n)
    case f: FixedMiniBatchTransformer => f.setBatchSize(n)
    case t: TimeIntervalMiniBatchTransformer => t.setMaxBatchSize(n)
  })

  def getMiniBatchSize: Int = getMiniBatcher match {
    case d: DynamicMiniBatchTransformer => d.getMaxBatchSize
    case f: FixedMiniBatchTransformer => f.getBatchSize
    case t: TimeIntervalMiniBatchTransformer => t.getMaxBatchSize
  }
}

object FixedMiniBatchTransformer extends DefaultParamsReadable[FixedMiniBatchTransformer]

trait HasBatchSize extends Params {

  val batchSize: Param[Int] = new IntParam(
    this, "batchSize", "The max size of the buffer")

  /** @group getParam */
  def getBatchSize: Int = $(batchSize)

  /** @group setParam */
  def setBatchSize(value: Int): this.type = set(batchSize, value)

}

class FixedMiniBatchTransformer(val uid: String)
  extends MiniBatchBase with HasBatchSize with SynapseMLLogging {
  logClass(FeatureNames.Core)

  val maxBufferSize: Param[Int] = new IntParam(
    this, "maxBufferSize", "The max size of the buffer")

  /** @group getParam */
  def getMaxBufferSize: Int = $(maxBufferSize)

  /** @group setParam */
  def setMaxBufferSize(value: Int): this.type = set(maxBufferSize, value)

  val buffered: Param[Boolean] = new BooleanParam(
    this, "buffered", "Whether or not to buffer batches in memory")

  /** @group getParam */
  def getBuffered: Boolean = $(buffered)

  /** @group setParam */
  def setBuffered(value: Boolean): this.type = set(buffered, value)

  setDefault(buffered->false, maxBufferSize->Integer.MAX_VALUE)

  def this() = this(Identifiable.randomUID("FixedMiniBatchTransformer"))

  override def getBatcher(it: Iterator[Row]): Iterator[List[Row]] = if (getBuffered){
    new FixedBufferedBatcher(it, getBatchSize, getMaxBufferSize)
  }else{
    new FixedBatcher(it, getBatchSize)
  }

}

object FlattenBatch extends DefaultParamsReadable[FlattenBatch]

class FlattenBatch(val uid: String)
    extends Transformer with Wrappable with DefaultParamsWritable with SynapseMLLogging {
  logClass(FeatureNames.Core)

  def this() = this(Identifiable.randomUID("FlattenBatch"))

  def transpose(nestedSeq: Seq[Any]): Seq[Seq[Any]] = {

    val innerLength = nestedSeq.filter {
      case null => false  //scalastyle:ignore null
      case _: Seq[Any] => true
      case _ => false
    }.head.asInstanceOf[Seq[Any]].length

    assert(nestedSeq.forall{
      case null => true  //scalastyle:ignore null
      case innerSeq: Seq[Any] => innerSeq.lengthCompare(innerLength) == 0
      case _ => true
    })
    (0 until innerLength).map(i => nestedSeq.map{
      case null => null  //scalastyle:ignore null
      case innerSeq: Seq[Any] => innerSeq(i)
      case any => any
    })
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame]({
      val outputSchema = transformSchema(dataset.schema)
      implicit val outputEncoder: ExpressionEncoder[Row] = RowEncoder(outputSchema)

      dataset.toDF().mapPartitions(it =>
        it.flatMap { rowOfLists =>
          val transposed: Seq[Seq[Any]] = transpose(
            (0 until rowOfLists.length)
              .map(i => {
                if (rowOfLists.isNullAt(i)) {
                  null  //scalastyle:ignore null
                } else {
                  val value = rowOfLists.get(i)
                  value match {
                    case _: mutable.WrappedArray[_] => rowOfLists.getSeq(i)
                    case _ => value
                  }
                }
              }))
          transposed.map {
            values => new GenericRowWithSchema(values.toArray, outputSchema)
          }
        }
      )
    }, dataset.columns.length)
  }

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    StructType(schema.map(f => {
      f.dataType match {
        case _: ArrayType => StructField(f.name, f.dataType.asInstanceOf[ArrayType].elementType)
        case _ => StructField(f.name, f.dataType)
      }
    }))
  }
}
