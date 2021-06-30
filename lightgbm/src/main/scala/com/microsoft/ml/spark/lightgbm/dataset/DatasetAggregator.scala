// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.lightgbm.dataset

import java.util.concurrent.atomic.AtomicLong

import com.microsoft.ml.spark.lightgbm.ColumnParams
import com.microsoft.ml.spark.lightgbm.dataset.DatasetUtils.{addFeaturesToChunkedArray, addGroupColumnRow,
  addInitScoreColumnRow, getRowAsDoubleArray}
import com.microsoft.ml.spark.lightgbm.swig._
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
    val chunkCount = chunkedArray.getChunksCount() - 1
    for (chunk <- 0L until chunkCount) {
      for (inChunkIdx <- 0L until chunkSize) {
        mainArray.setItem(threadRowStartIndex + chunk * chunkSize + inChunkIdx,
          chunkedArray.getItem(chunk, inChunkIdx, defaultVal))
      }
    }
    // Next copy filled values from last chunk only
    val lastChunkCount = chunkedArray.getLastChunkAddCount()
    for (lastChunkIdx <- 0L until lastChunkCount) {
      mainArray.setItem(threadRowStartIndex + chunkCount * chunkSize + lastChunkIdx,
        chunkedArray.getItem(chunkCount, lastChunkIdx, defaultVal))
    }
  }
}

private[lightgbm] abstract class BaseChunkedColumns(columnParams: ColumnParams,
                                                    schema: StructType,
                                                    chunkSize: Int) {
  protected val labels: FloatChunkedArray = new FloatChunkedArray(chunkSize)
  protected val weights: Option[FloatChunkedArray] = columnParams.weightColumn.map {
    _ => new FloatChunkedArray(chunkSize)
  }
  protected val initScores: Option[DoubleChunkedArray] = columnParams.initScoreColumn.map {
    _ => new DoubleChunkedArray(chunkSize)
  }
  protected val groups: ListBuffer[Row] = new ListBuffer[Row]()

  protected var rowCount = 0

  def addRows(rowsIter: Iterator[Row]): Unit = {
    while (rowsIter.hasNext) {
      this.addRow()
      val row = rowsIter.next()
      this.addFeatures(row)
      this.labels.add(row.getDouble(schema.fieldIndex(columnParams.labelColumn)).toFloat)
      columnParams.weightColumn.foreach { col =>
        this.weights.get.add(row.getDouble(schema.fieldIndex(col)).toFloat)
      }

      addInitScoreColumnRow(this.initScores, row, columnParams, schema)
      addGroupColumnRow(row, this.groups, columnParams, schema)
    }
  }

  def addFeatures(row: Row): Unit

  def addRow(): Unit = {
    rowCount += 1
  }

  def release(): Unit = {
    // Clear memory
    labels.delete()
    weights.foreach(_.delete())
    initScores.foreach(_.delete())
  }

  def getRowCount: Long = rowCount

  def getInitScoresCount: Long = this.initScores.map(_.getAddCount()).getOrElse(0L)

  def getWeights: Option[FloatChunkedArray] = weights

  def getInitScores: Option[DoubleChunkedArray] = initScores

  def getLabels: FloatChunkedArray = labels

  def getGroups: ListBuffer[Row] = groups
}

private[lightgbm] final class SparseChunkedColumns(columnParams: ColumnParams, schema: StructType, chunkSize: Int,
                                                   useSingleDataset: Boolean)
  extends BaseChunkedColumns(columnParams, schema, chunkSize) {

  protected var indexes = new IntChunkedArray(chunkSize)
  protected var values = new DoubleChunkedArray(chunkSize)
  protected var indptr = new IntChunkedArray(chunkSize)

  private var numCols = 0

  override def addRows(rowsIter: Iterator[Row]): Unit = {
    if (!useSingleDataset) {
      this.indptr.add(0)
    }
    super.addRows(rowsIter)
  }

  def addFeatures(row: Row): Unit = {
    val sparseVector = row.get(schema.fieldIndex(columnParams.featuresColumn)) match {
      case dense: DenseVector => dense.toSparse
      case sparse: SparseVector => sparse
    }
    sparseVector.values.foreach(this.values.add(_))
    sparseVector.indices.foreach(this.indexes.add(_))
    this.setNumCols(sparseVector.size)
    this.indptr.add(sparseVector.numNonzeros)
  }

  def setNumCols(numCols: Int): Unit = {
    this.numCols = numCols
  }

  def getNumCols: Int = numCols

  def getIndexesCount: Long = indexes.getAddCount()

  def getIndptrCount: Long = indptr.getAddCount()

  def getIndexes: IntChunkedArray = indexes

  def getValues: DoubleChunkedArray = values

  def getIndptr: IntChunkedArray = indptr

  override def release(): Unit = {
    // Clear memory
    super.release()
    indexes.delete()
    values.delete()
    indptr.delete()
  }
}

private[lightgbm] final class DenseChunkedColumns(columnParams: ColumnParams,
                                                  schema: StructType,
                                                  chunkSize: Int,
                                                  val numCols: Int)
  extends BaseChunkedColumns(columnParams, schema, chunkSize) {
  var features = new DoubleChunkedArray(numCols * chunkSize)

  def addFeatures(row: Row): Unit = {
    val rowAsDoubleArray = getRowAsDoubleArray(row, columnParams, schema)
    addFeaturesToChunkedArray(this.features, rowAsDoubleArray)
  }

  override def release(): Unit = {
    // Clear memory
    super.release()
    features.delete()
  }

  def getFeatures: DoubleChunkedArray = features
}

private[lightgbm] abstract class BaseAggregatedColumns(val chunkSize: Int) {
  protected var labelsArray: FloatSwigArray = _
  protected var weightArrayOpt: Option[FloatSwigArray] = None
  protected var initScoreArrayOpt: Option[DoubleSwigArray] = None
  protected var groupColumnValuesArray: Array[Row] = _

  /**
    * Variables for knowing how large full array should be allocated to
    */
  var rowCount = new AtomicLong(0L)
  var initScoreCount = new AtomicLong(0L)

  protected var numCols = 0

  def getNumCols: Int = numCols

  def getNumColsFromChunkedArray(chunkedCols: BaseChunkedColumns): Int

  def incrementCount(chunkedCols: BaseChunkedColumns): Unit = {
    this.rowCount.addAndGet(chunkedCols.getRowCount)
    this.initScoreCount.addAndGet(chunkedCols.getInitScoresCount)
  }

  def addRows(chunkedCols: BaseChunkedColumns): Unit = {
    this.numCols = getNumColsFromChunkedArray(chunkedCols)
  }

  protected def initializeRows(chunkedCols: BaseChunkedColumns): Unit = {
    // this.numCols = numCols
    val rowCount = this.rowCount.get()
    val initScoreCount = this.initScoreCount.get()
    labelsArray = new FloatSwigArray(rowCount)
    weightArrayOpt = chunkedCols.getWeights.map(_ => new FloatSwigArray(rowCount))
    initScoreArrayOpt = chunkedCols.getInitScores.map(_ => new DoubleSwigArray(initScoreCount))
    this.initializeFeatures(chunkedCols, rowCount)
    groupColumnValuesArray = new Array[Row](rowCount.toInt)
  }

  protected def initializeFeatures(chunkedCols: BaseChunkedColumns, rowCount: Long): Unit

  def getLabelsArray: FloatSwigArray = labelsArray

  def getWeightArrayOpt: Option[FloatSwigArray] = weightArrayOpt

  def getInitScoreArrayOpt: Option[DoubleSwigArray] = initScoreArrayOpt

  def getGroupColumnValuesArray: Array[Row] = groupColumnValuesArray
}

private[lightgbm] abstract trait DisjointAggregatedColumns extends BaseAggregatedColumns {
  def addFeatures(chunkedCols: BaseChunkedColumns): Unit

  /** Adds the rows to the internal data structure.
    */
  override def addRows(chunkedCols: BaseChunkedColumns): Unit = {
    super.addRows(chunkedCols)
    initializeRows(chunkedCols)
    // Coalesce to main arrays passed to dataset create
    chunkedCols.getLabels.coalesceTo(this.labelsArray)
    chunkedCols.getWeights.foreach(_.coalesceTo(this.weightArrayOpt.get))
    chunkedCols.getInitScores.foreach(_.coalesceTo(this.initScoreArrayOpt.get))
    this.addFeatures(chunkedCols)
    chunkedCols.getGroups.copyToArray(groupColumnValuesArray)
  }
}

private[lightgbm] abstract trait SyncAggregatedColumns extends BaseAggregatedColumns {
  /**
    * Variables for current thread to use in order to update common arrays in parallel
    */
  protected var threadRowStartIndex = new AtomicLong(0L)
  protected var threadInitScoreStartIndex = new AtomicLong(0L)

  /** Adds the rows to the internal data structure.
    */
  override def addRows(chunkedCols: BaseChunkedColumns): Unit = {
    super.addRows(chunkedCols)
    parallelInitializeRows(chunkedCols)
    parallelizedCopy(chunkedCols)
  }

  private def parallelInitializeRows(chunkedCols: BaseChunkedColumns): Unit = {
    // Initialize arrays if they are not defined - first thread to get here does the initialization for all of them
    if (labelsArray == null) {
      this.synchronized {
        if (labelsArray == null) {
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
        val labelsSize = chunkedCols.getLabels.getAddCount()
        threadRowStartIndex = this.threadRowStartIndex.getAndAdd(labelsSize.toInt)
        val initScoreSize = chunkedCols.getInitScores.map(_.getAddCount())
        initScoreSize.foreach(size => threadInitScoreStartIndex = this.threadInitScoreStartIndex.getAndAdd(size))
        updateThreadLocalIndices(chunkedCols, threadRowStartIndex)
      }
    ChunkedArrayUtils.copyChunkedArray(chunkedCols.getLabels, this.labelsArray, threadRowStartIndex, chunkSize)
    chunkedCols.getWeights.foreach {
      weightChunkedArray =>
        ChunkedArrayUtils.copyChunkedArray(weightChunkedArray, this.weightArrayOpt.get, threadRowStartIndex,
          chunkSize)
    }
    chunkedCols.getInitScores.foreach {
      initScoreChunkedArray =>
        ChunkedArrayUtils.copyChunkedArray(initScoreChunkedArray, this.initScoreArrayOpt.get,
          threadInitScoreStartIndex, chunkSize)
    }
    parallelizeFeaturesCopy(chunkedCols, featureIndexes)
    chunkedCols.getGroups.copyToArray(groupColumnValuesArray, threadRowStartIndex.toInt)
    // rewrite array reference for volatile arrays, see: https://www.javamex.com/tutorials/volatile_arrays.shtml
    this.synchronized {
      groupColumnValuesArray = groupColumnValuesArray
    }
  }
}

private[lightgbm] abstract class BaseDenseAggregatedColumns(chunkSize: Int) extends BaseAggregatedColumns(chunkSize) {
  protected var featuresArray: DoubleSwigArray = _

  def getNumColsFromChunkedArray(chunkedCols: BaseChunkedColumns): Int = {
    chunkedCols.asInstanceOf[DenseChunkedColumns].numCols
  }

  protected def initializeFeatures(chunkedCols: BaseChunkedColumns, rowCount: Long): Unit = {
    featuresArray = new DoubleSwigArray(numCols * rowCount)
  }

  def getFeaturesArray: DoubleSwigArray = featuresArray
}

private[lightgbm] final class DenseAggregatedColumns(chunkSize: Int)
  extends BaseDenseAggregatedColumns(chunkSize) with DisjointAggregatedColumns {

  def addFeatures(chunkedCols: BaseChunkedColumns): Unit = {
    chunkedCols.asInstanceOf[DenseChunkedColumns].getFeatures.coalesceTo(this.featuresArray)
  }
}

/** Defines class for aggregating rows to a single structure before creating the native LightGBMDataset.
  * @param chunkSize The chunk size for the chunked arrays.
  */
private[lightgbm] final class DenseSyncAggregatedColumns(chunkSize: Int)
  extends BaseDenseAggregatedColumns(chunkSize) with SyncAggregatedColumns {
  protected def updateThreadLocalIndices(chunkedCols: BaseChunkedColumns, threadRowStartIndex: Long): List[Long] = {
    List(threadRowStartIndex)
  }

  protected def parallelizeFeaturesCopy(chunkedCols: BaseChunkedColumns, featureIndexes: List[Long]): Unit = {
    ChunkedArrayUtils.copyChunkedArray(chunkedCols.asInstanceOf[DenseChunkedColumns].getFeatures,
      this.featuresArray, featureIndexes(0) * numCols, chunkSize)
  }
}

private[lightgbm] abstract class BaseSparseAggregatedColumns(chunkSize: Int)
  extends BaseAggregatedColumns(chunkSize) {
  protected var indexesArray: IntSwigArray = _
  protected var valuesArray: DoubleSwigArray = _
  protected var indptrArray: IntSwigArray = _

  /**
    * Aggregated variables for knowing how large full array should be allocated to
    */
  protected var indexesCount = new AtomicLong(0L)
  protected var indptrCount = new AtomicLong(0L)

  def getNumColsFromChunkedArray(chunkedCols: BaseChunkedColumns): Int = {
    chunkedCols.asInstanceOf[SparseChunkedColumns].getNumCols
  }

  override def incrementCount(chunkedCols: BaseChunkedColumns): Unit = {
    super.incrementCount(chunkedCols)
    val sparseChunkedCols = chunkedCols.asInstanceOf[SparseChunkedColumns]
    this.indexesCount.addAndGet(sparseChunkedCols.getIndexesCount)
    this.indptrCount.addAndGet(sparseChunkedCols.getIndptrCount)
  }

  protected def initializeFeatures(chunkedCols: BaseChunkedColumns, rowCount: Long): Unit = {
    val indexesCount = this.indexesCount.get()
    val indptrCount = this.indptrCount.get()
    indexesArray = new IntSwigArray(indexesCount)
    valuesArray = new DoubleSwigArray(indexesCount)
    indptrArray = new IntSwigArray(indptrCount)
    indptrArray.setItem(0, 0)
  }

  def getIndexesArray: IntSwigArray = indexesArray

  def getValuesArray: DoubleSwigArray = valuesArray

  def getIndptrArray: IntSwigArray = indptrArray

  def getIndexesCount: Long = this.indexesCount.get()

  def getIndptrCount: Long = this.indptrCount.get()
}


/** Defines class for aggregating rows to a single structure before creating the native LightGBMDataset.
  * @param chunkSize The chunk size for the chunked arrays.
  */
private[lightgbm] final class SparseAggregatedColumns(chunkSize: Int)
  extends BaseSparseAggregatedColumns(chunkSize) with DisjointAggregatedColumns {

  /** Adds the indexes, values and indptr to the internal data structure.
    */
  def addFeatures(chunkedCols: BaseChunkedColumns): Unit = {
    val sparseChunkedColumns = chunkedCols.asInstanceOf[SparseChunkedColumns]
    sparseChunkedColumns.getIndexes.coalesceTo(this.indexesArray)
    sparseChunkedColumns.getValues.coalesceTo(this.valuesArray)
    sparseChunkedColumns.getIndptr.coalesceTo(this.indptrArray)
  }
}

/** Defines class for aggregating rows to a single structure before creating the native LightGBMDataset.
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
    val indexesSize = sparseChunkedCols.getIndexes.getAddCount()
    val threadIndexesStartIndex = this.threadIndexesStartIndex.getAndAdd(indexesSize)

    val indPtrSize = sparseChunkedCols.getIndptr.getAddCount()
    val threadIndPtrStartIndex = this.threadIndptrStartIndex.getAndAdd(indPtrSize)
    List(threadIndexesStartIndex, threadIndPtrStartIndex)
  }

  protected def parallelizeFeaturesCopy(chunkedCols: BaseChunkedColumns, featureIndexes: List[Long]): Unit = {
    val sparseChunkedCols = chunkedCols.asInstanceOf[SparseChunkedColumns]
    val threadIndexesStartIndex = featureIndexes(0)
    val threadIndPtrStartIndex = featureIndexes(1)
    ChunkedArrayUtils.copyChunkedArray(sparseChunkedCols.getIndexes, this.indexesArray,
      threadIndexesStartIndex, chunkSize)
    ChunkedArrayUtils.copyChunkedArray(sparseChunkedCols.getValues, this.valuesArray,
      threadIndexesStartIndex, chunkSize)
    ChunkedArrayUtils.copyChunkedArray(sparseChunkedCols.getIndptr, this.indptrArray,
      threadIndPtrStartIndex, chunkSize)
  }
}
