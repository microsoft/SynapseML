// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.lightgbm.dataset

import com.microsoft.ml.lightgbm.{SWIGTYPE_p_int, lightgbmlib, lightgbmlibConstants}

import java.util.concurrent.atomic.AtomicLong
import com.microsoft.ml.spark.lightgbm.{ColumnParams, LightGBMUtils}
import com.microsoft.ml.spark.lightgbm.dataset.DatasetUtils.getRowAsDoubleArray
import com.microsoft.ml.spark.lightgbm.swig._
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.{DenseVector, SparseVector}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ListBuffer

private[lightgbm] object ChunkedArrayUtils {
  def copyChunkedArray[T: Numeric](chunkedArray: ChunkedArray[T],
                                   mainArray: BaseSwigArray[T],
                                   threadRowStartIndex: Long,
                                   chunkSize: Long): Unit = {
    val num = implicitly[Numeric[T]]
    val defaultVal = num.fromInt(-1)
    // Copy in parallel on each thread
    // First copy full chunks
    val chunkCount = chunkedArray.getChunksCount - 1
    for (chunk <- 0L until chunkCount) {
      for (inChunkIdx <- 0L until chunkSize) {
        mainArray.setItem(threadRowStartIndex + chunk * chunkSize + inChunkIdx,
          chunkedArray.getItem(chunk, inChunkIdx, defaultVal))
      }
    }
    // Next copy filled values from last chunk only
    val lastChunkCount = chunkedArray.getLastChunkAddCount
    for (lastChunkIdx <- 0L until lastChunkCount) {
      mainArray.setItem(threadRowStartIndex + chunkCount * chunkSize + lastChunkIdx,
        chunkedArray.getItem(chunkCount, lastChunkIdx, defaultVal))
    }
  }
}

class PeekingIterator[T](it: Iterator[T]) extends Iterator[T] {
  var nextHolder: Option[T] = None

  override def hasNext: Boolean = {
    nextHolder.isDefined || it.hasNext
  }

  override def next(): T = {
    if (nextHolder.isDefined) {
      val n = nextHolder.get
      nextHolder = None
      n
    } else {
      it.next()
    }
  }

  def peek: T = {
    if (nextHolder.isEmpty && it.hasNext) {
      nextHolder = Some(it.next())
    }
    nextHolder.get
  }
}

private[lightgbm] abstract class BaseChunkedColumns(rowsIter: PeekingIterator[Row],
                                                    columnParams: ColumnParams,
                                                    schema: StructType,
                                                    chunkSize: Int) {
  val labels: FloatChunkedArray = new FloatChunkedArray(chunkSize)
  val weights: Option[FloatChunkedArray] = columnParams.weightColumn.map {
    _ => new FloatChunkedArray(chunkSize)
  }
  val initScores: Option[DoubleChunkedArray] = columnParams.initScoreColumn.map {
    _ => new DoubleChunkedArray(chunkSize)
  }
  val groups: ListBuffer[Any] = new ListBuffer[Any]()

  val numCols: Int = rowsIter.peek.getAs[Any](columnParams.featuresColumn) match {
    case dense: DenseVector => dense.toSparse.size
    case sparse: SparseVector => sparse.size
  }

  lazy val rowCount: Int = rowsIter.map { row =>
    addFeatures(row)
    labels.add(row.getDouble(schema.fieldIndex(columnParams.labelColumn)).toFloat)
    columnParams.weightColumn.foreach { col =>
      weights.get.add(row.getDouble(schema.fieldIndex(col)).toFloat)
    }
    addInitScoreColumnRow(row)
    addGroupColumnRow(row)
  }.length

  protected def addInitScoreColumnRow(row: Row): Unit = {
    columnParams.initScoreColumn.foreach { col =>
      if (schema(col).dataType == VectorType) {
        row.getAs[DenseVector](col).values.foreach(initScores.get.add)
        // Note: rows * # classes in multiclass case
      } else {
        initScores.get.add(row.getAs[Double](col))
      }
    }
  }

  protected def addGroupColumnRow(row: Row): Unit = {
    columnParams.groupColumn.foreach { col =>
      groups.append(row.getAs[Any](col))
    }
  }

  protected def addFeatures(row: Row): Unit

  def release(): Unit = {
    // Clear memory
    labels.delete()
    weights.foreach(_.delete())
    initScores.foreach(_.delete())
  }

  def numInitScores: Long = initScores.map(_.getAddCount).getOrElse(0L)

}

private[lightgbm] final class SparseChunkedColumns(rowsIter: PeekingIterator[Row],
                                                   columnParams: ColumnParams,
                                                   schema: StructType,
                                                   chunkSize: Int,
                                                   useSingleDataset: Boolean)
  extends BaseChunkedColumns(rowsIter, columnParams, schema, chunkSize) {

  val indexes = new IntChunkedArray(chunkSize)
  val values = new DoubleChunkedArray(chunkSize)
  val indexPointers = new IntChunkedArray(chunkSize)

  if (!useSingleDataset) {
    indexPointers.add(0)
  }

  override protected def addFeatures(row: Row): Unit = {
    val sparseVector = row.getAs[Any](columnParams.featuresColumn) match {
      case dense: DenseVector => dense.toSparse
      case sparse: SparseVector => sparse
    }
    sparseVector.values.foreach(values.add)
    sparseVector.indices.foreach(indexes.add)
    indexPointers.add(sparseVector.numNonzeros)
  }

  def getNumIndexes: Long = indexes.getAddCount

  def getNumIndexPointers: Long = indexPointers.getAddCount

  override def release(): Unit = {
    // Clear memory
    super.release()
    indexes.delete()
    values.delete()
    indexPointers.delete()
  }
}

private[lightgbm] final class DenseChunkedColumns(rowsIter: PeekingIterator[Row],
                                                  columnParams: ColumnParams,
                                                  schema: StructType,
                                                  chunkSize: Int)
  extends BaseChunkedColumns(rowsIter, columnParams, schema, chunkSize) {

  val features = new DoubleChunkedArray(numCols * chunkSize)

  override protected def addFeatures(row: Row): Unit = {
    getRowAsDoubleArray(row, columnParams).foreach(features.add)
  }

  override def release(): Unit = {
    // Clear memory
    super.release()
    features.delete()
  }

}

private[lightgbm] abstract class BaseAggregatedColumns(val chunkSize: Int) {
  protected var labels: FloatSwigArray = _
  protected var weights: Option[FloatSwigArray] = None
  protected var initScores: Option[DoubleSwigArray] = None
  protected var groups: Array[Any] = _

  /**
    * Variables for knowing how large full array should be allocated to
    */
  protected val rowCount = new AtomicLong(0L)
  protected val initScoreCount = new AtomicLong(0L)

  protected var numCols = 0

  def getRowCount: Int = rowCount.get().toInt

  def getNumCols: Int = numCols

  def getNumColsFromChunkedArray(chunkedCols: BaseChunkedColumns): Int

  protected def initializeFeatures(chunkedCols: BaseChunkedColumns, rowCount: Long): Unit

  def getGroups: Array[Any] = groups

  def cleanup(): Unit = {
    labels.delete()
    weights.foreach(_.delete())
    initScores.foreach(_.delete())
  }

  def generateDataset(referenceDataset: Option[LightGBMDataset], datasetParams: String): LightGBMDataset

  def incrementCount(chunkedCols: BaseChunkedColumns): Unit = {
    rowCount.addAndGet(chunkedCols.rowCount)
    initScoreCount.addAndGet(chunkedCols.numInitScores)
  }

  def addRows(chunkedCols: BaseChunkedColumns): Unit = {
    numCols = getNumColsFromChunkedArray(chunkedCols)
  }

  protected def initializeRows(chunkedCols: BaseChunkedColumns): Unit = {
    // this.numCols = numCols
    val rc = rowCount.get()
    val isc = initScoreCount.get()
    labels = new FloatSwigArray(rc)
    weights = chunkedCols.weights.map(_ => new FloatSwigArray(rc))
    initScores = chunkedCols.initScores.map(_ => new DoubleSwigArray(isc))
    initializeFeatures(chunkedCols, rc)
    groups = new Array[Any](rc.toInt)
  }

}

private[lightgbm] trait DisjointAggregatedColumns extends BaseAggregatedColumns {
  def addFeatures(chunkedCols: BaseChunkedColumns): Unit

  /** Adds the rows to the internal data structure.
    */
  override def addRows(chunkedCols: BaseChunkedColumns): Unit = {
    super.addRows(chunkedCols)
    initializeRows(chunkedCols)
    // Coalesce to main arrays passed to dataset create
    chunkedCols.labels.coalesceTo(labels)
    chunkedCols.weights.foreach(_.coalesceTo(weights.get))
    chunkedCols.initScores.foreach(_.coalesceTo(initScores.get))
    this.addFeatures(chunkedCols)
    chunkedCols.groups.copyToArray(groups)
  }
}

private[lightgbm] trait SyncAggregatedColumns extends BaseAggregatedColumns {
  /**
    * Variables for current thread to use in order to update common arrays in parallel
    */
  protected val threadRowStartIndex = new AtomicLong(0L)
  protected val threadInitScoreStartIndex = new AtomicLong(0L)

  /** Adds the rows to the internal data structure.
    */
  override def addRows(chunkedCols: BaseChunkedColumns): Unit = {
    super.addRows(chunkedCols)
    parallelInitializeRows(chunkedCols)
    parallelizedCopy(chunkedCols)
  }

  private def parallelInitializeRows(chunkedCols: BaseChunkedColumns): Unit = {
    // Initialize arrays if they are not defined - first thread to get here does the initialization for all of them
    if (labels == null) {
      this.synchronized {
        if (labels == null) {
          initializeRows(chunkedCols)
        }
      }
    }
  }

  protected def updateThreadLocalIndices(chunkedCols: BaseChunkedColumns, threadRowStartIndex: Long): List[Long]

  protected def parallelizeFeaturesCopy(chunkedCols: BaseChunkedColumns, featureIndexes: List[Long]): Unit

  private def parallelizedCopy(chunkedCols: BaseChunkedColumns): Unit = {
    // Parallelized copy to common arrays
    var threadRowStartIndex = 0L
    var threadInitScoreStartIndex = 0L
    val featureIndexes =
      this.synchronized {
        val labelsSize = chunkedCols.labels.getAddCount
        threadRowStartIndex = this.threadRowStartIndex.getAndAdd(labelsSize.toInt)
        val initScoreSize = chunkedCols.initScores.map(_.getAddCount)
        initScoreSize.foreach(size => threadInitScoreStartIndex = this.threadInitScoreStartIndex.getAndAdd(size))
        updateThreadLocalIndices(chunkedCols, threadRowStartIndex)
      }
    ChunkedArrayUtils.copyChunkedArray(chunkedCols.labels, labels, threadRowStartIndex, chunkSize)
    chunkedCols.weights.foreach {
      weightChunkedArray =>
        ChunkedArrayUtils.copyChunkedArray(weightChunkedArray, weights.get, threadRowStartIndex,
          chunkSize)
    }
    chunkedCols.initScores.foreach {
      initScoreChunkedArray =>
        ChunkedArrayUtils.copyChunkedArray(initScoreChunkedArray, initScores.get,
          threadInitScoreStartIndex, chunkSize)
    }
    parallelizeFeaturesCopy(chunkedCols, featureIndexes)
    chunkedCols.groups.copyToArray(groups, threadRowStartIndex.toInt)
    // rewrite array reference for volatile arrays, see: https://www.javamex.com/tutorials/volatile_arrays.shtml
    this.synchronized {
      groups = groups
    }
  }
}

private[lightgbm] abstract class BaseDenseAggregatedColumns(chunkSize: Int) extends BaseAggregatedColumns(chunkSize) {
  protected var features: DoubleSwigArray = _

  def getNumColsFromChunkedArray(chunkedCols: BaseChunkedColumns): Int = {
    chunkedCols.asInstanceOf[DenseChunkedColumns].numCols
  }

  protected def initializeFeatures(chunkedCols: BaseChunkedColumns, rowCount: Long): Unit = {
    features = new DoubleSwigArray(numCols * rowCount)
  }

  def getFeatures: DoubleSwigArray = features

  def generateDataset(referenceDataset: Option[LightGBMDataset], datasetParams: String): LightGBMDataset = {
    val pointer = lightgbmlib.voidpp_handle()
    try {
      // Generate the dataset for features
      LightGBMUtils.validate(lightgbmlib.LGBM_DatasetCreateFromMat(
        lightgbmlib.double_to_voidp_ptr(features.array),
        lightgbmlibConstants.C_API_DTYPE_FLOAT64,
        rowCount.get().toInt,
        numCols,
        1,
        datasetParams,
        referenceDataset.map(_.datasetPtr).orNull,
        pointer), "Dataset create")
    } finally {
      lightgbmlib.delete_doubleArray(features.array)
    }
    val dataset = new LightGBMDataset(lightgbmlib.voidpp_value(pointer))
    dataset.addFloatField(labels.array, "label", getRowCount)
    weights.map(_.array).foreach(dataset.addFloatField(_, "weight", getRowCount))
    initScores.map(_.array).foreach(dataset.addDoubleField(_, "init_score", getRowCount))
    dataset
  }

}

private[lightgbm] final class DenseAggregatedColumns(chunkSize: Int)
  extends BaseDenseAggregatedColumns(chunkSize) with DisjointAggregatedColumns {

  def addFeatures(chunkedCols: BaseChunkedColumns): Unit = {
    chunkedCols.asInstanceOf[DenseChunkedColumns].features.coalesceTo(features)
  }

}

/** Defines class for aggregating rows to a single structure before creating the native LightGBMDataset.
  *
  * @param chunkSize The chunk size for the chunked arrays.
  */
private[lightgbm] final class DenseSyncAggregatedColumns(chunkSize: Int)
  extends BaseDenseAggregatedColumns(chunkSize) with SyncAggregatedColumns {
  protected def updateThreadLocalIndices(chunkedCols: BaseChunkedColumns, threadRowStartIndex: Long): List[Long] = {
    List(threadRowStartIndex)
  }

  protected def parallelizeFeaturesCopy(chunkedCols: BaseChunkedColumns, featureIndexes: List[Long]): Unit = {
    ChunkedArrayUtils.copyChunkedArray(chunkedCols.asInstanceOf[DenseChunkedColumns].features,
      features, featureIndexes.head * numCols, chunkSize)
  }

}

private[lightgbm] abstract class BaseSparseAggregatedColumns(chunkSize: Int)
  extends BaseAggregatedColumns(chunkSize) {
  protected var indexes: IntSwigArray = _
  protected var values: DoubleSwigArray = _
  protected var indexPointers: IntSwigArray = _

  /**
    * Aggregated variables for knowing how large full array should be allocated to
    */
  protected var indexesCount = new AtomicLong(0L)
  protected var indptrCount = new AtomicLong(0L)

  def getNumColsFromChunkedArray(chunkedCols: BaseChunkedColumns): Int = {
    chunkedCols.asInstanceOf[SparseChunkedColumns].numCols
  }

  override def incrementCount(chunkedCols: BaseChunkedColumns): Unit = {
    super.incrementCount(chunkedCols)
    val sparseChunkedCols = chunkedCols.asInstanceOf[SparseChunkedColumns]
    indexesCount.addAndGet(sparseChunkedCols.getNumIndexes)
    indptrCount.addAndGet(sparseChunkedCols.getNumIndexPointers)
  }

  protected def initializeFeatures(chunkedCols: BaseChunkedColumns, rowCount: Long): Unit = {
    val indexesCount = this.indexesCount.get()
    val indptrCount = this.indptrCount.get()
    indexes = new IntSwigArray(indexesCount)
    values = new DoubleSwigArray(indexesCount)
    indexPointers = new IntSwigArray(indptrCount)
    indexPointers.setItem(0, 0)
  }

  def getIndexes: IntSwigArray = indexes

  def getValues: DoubleSwigArray = values

  def getIndexPointers: IntSwigArray = indexPointers

  override def cleanup(): Unit = {
    labels.delete()
    weights.foreach(_.delete())
    initScores.foreach(_.delete())
    values.delete()
    indexes.delete()
    indexPointers.delete()
  }

  private def indexPointerArrayIncrement(indptrArray: SWIGTYPE_p_int): Unit = {
    // Update indptr array indexes in sparse matrix
    (1L until indptrCount.get()).foreach { index =>
      val indptrPrevValue = lightgbmlib.intArray_getitem(indptrArray, index - 1)
      val indptrCurrValue = lightgbmlib.intArray_getitem(indptrArray, index)
      lightgbmlib.intArray_setitem(indptrArray, index, indptrPrevValue + indptrCurrValue)
    }
  }

  def generateDataset(referenceDataset: Option[LightGBMDataset], datasetParams: String): LightGBMDataset = {
    indexPointerArrayIncrement(getIndexPointers.array)
    val pointer = lightgbmlib.voidpp_handle()
    // Generate the dataset for features
    LightGBMUtils.validate(lightgbmlib.LGBM_DatasetCreateFromCSR(
      lightgbmlib.int_to_voidp_ptr(indexPointers.array),
      lightgbmlibConstants.C_API_DTYPE_INT32,
      indexes.array,
      lightgbmlib.double_to_voidp_ptr(values.array),
      lightgbmlibConstants.C_API_DTYPE_FLOAT64,
      indptrCount.get(),
      indexesCount.get(),
      numCols,
      datasetParams,
      referenceDataset.map(_.datasetPtr).orNull,
      pointer), "Dataset create")
    val dataset = new LightGBMDataset(lightgbmlib.voidpp_value(pointer))
    dataset.addFloatField(labels.array, "label", getRowCount)
    weights.map(_.array).foreach(dataset.addFloatField(_, "weight", getRowCount))
    initScores.map(_.array).foreach(dataset.addDoubleField(_, "init_score", getRowCount))
    dataset
  }

}

/** Defines class for aggregating rows to a single structure before creating the native LightGBMDataset.
  *
  * @param chunkSize The chunk size for the chunked arrays.
  */
private[lightgbm] final class SparseAggregatedColumns(chunkSize: Int)
  extends BaseSparseAggregatedColumns(chunkSize) with DisjointAggregatedColumns {

  /** Adds the indexes, values and indptr to the internal data structure.
    */
  def addFeatures(chunkedCols: BaseChunkedColumns): Unit = {
    val sparseChunkedColumns = chunkedCols.asInstanceOf[SparseChunkedColumns]
    sparseChunkedColumns.indexes.coalesceTo(indexes)
    sparseChunkedColumns.values.coalesceTo(values)
    sparseChunkedColumns.indexPointers.coalesceTo(indexPointers)
  }
}

/** Defines class for aggregating rows to a single structure before creating the native LightGBMDataset.
  *
  * @param chunkSize The chunk size for the chunked arrays.
  */
private[lightgbm] final class SparseSyncAggregatedColumns(chunkSize: Int)
  extends BaseSparseAggregatedColumns(chunkSize) with SyncAggregatedColumns {
  /**
    * Variables for current thread to use in order to update common arrays in parallel
    */
  protected val threadIndexesStartIndex = new AtomicLong(0L)
  protected val threadIndptrStartIndex = new AtomicLong(1L)

  override protected def initializeRows(chunkedCols: BaseChunkedColumns): Unit = {
    // Add extra 0 for start of indptr in parallel case
    this.indptrCount.addAndGet(1L)
    super.initializeRows(chunkedCols)
  }

  protected def updateThreadLocalIndices(chunkedCols: BaseChunkedColumns, threadRowStartIndex: Long): List[Long] = {
    val sparseChunkedCols = chunkedCols.asInstanceOf[SparseChunkedColumns]
    val indexesSize = sparseChunkedCols.indexes.getAddCount
    val threadIndexesStartIndex = this.threadIndexesStartIndex.getAndAdd(indexesSize)

    val indPtrSize = sparseChunkedCols.indexPointers.getAddCount
    val threadIndPtrStartIndex = this.threadIndptrStartIndex.getAndAdd(indPtrSize)
    List(threadIndexesStartIndex, threadIndPtrStartIndex)
  }

  protected def parallelizeFeaturesCopy(chunkedCols: BaseChunkedColumns, featureIndexes: List[Long]): Unit = {
    val sparseChunkedCols = chunkedCols.asInstanceOf[SparseChunkedColumns]
    ChunkedArrayUtils.copyChunkedArray(sparseChunkedCols.indexes, indexes, featureIndexes(0), chunkSize)
    ChunkedArrayUtils.copyChunkedArray(sparseChunkedCols.values, values, featureIndexes(0), chunkSize)
    ChunkedArrayUtils.copyChunkedArray(sparseChunkedCols.indexPointers, indexPointers, featureIndexes(1), chunkSize)
  }

}
