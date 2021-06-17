// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.lightgbm.dataset

import com.microsoft.ml.spark.lightgbm.ColumnParams
import com.microsoft.ml.spark.lightgbm.swig.{BaseSwigArray, ChunkedArray, DoubleChunkedArray, DoubleSwigArray,
  FloatChunkedArray, FloatSwigArray, IntChunkedArray, IntSwigArray}
import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer

object ChunkedArrayUtils {
  def copyChunkedArray[T](chunkedArray: ChunkedArray[T],
                          mainArray: BaseSwigArray[T],
                          threadRowStartIndex: Long,
                          chunkSize: Long): Unit = {
    // Copy in parallel on each thread
    // First copy full chunks
    val chunkCount = chunkedArray.getChunksCount() - 1
    for (chunk <- 0L until chunkCount) {
      for (in_chunk_idx <- 0L until chunkSize) {
        mainArray.setItem(threadRowStartIndex + chunk * chunkSize + in_chunk_idx,
          chunkedArray.getItem(chunk, in_chunk_idx, (-1).asInstanceOf[T]))
      }
    }
    // Next copy filled values from last chunk only
    val lastChunkCount = chunkedArray.getLastChunkAddCount()
    for (last_chunk_idx <- 0L until lastChunkCount) {
      mainArray.setItem(threadRowStartIndex + chunkCount * chunkSize + last_chunk_idx,
        chunkedArray.getItem(chunkCount, last_chunk_idx, (-1).asInstanceOf[T]))
    }
  }
}

trait AggregatedColumns

class BaseChunkedColumns(columnParams: ColumnParams,
                         chunkSize: Int) extends AggregatedColumns {
  val labelsChunkedArray = new FloatChunkedArray(chunkSize)
  val weightChunkedArrayOpt = columnParams.weightColumn.map {
    _ => new FloatChunkedArray(chunkSize)
  }
  val initScoreChunkedArrayOpt = columnParams.initScoreColumn.map {
    _ => new DoubleChunkedArray(chunkSize)
  }
  val groupColumnValues: ListBuffer[Row] = new ListBuffer[Row]()

  var rowCount = 0

  def release(): Unit = {
    // Clear memory
    labelsChunkedArray.delete()
    weightChunkedArrayOpt.foreach(_.delete())
    initScoreChunkedArrayOpt.foreach(_.delete())
  }
}

class SparseChunkedColumns(columnParams: ColumnParams,
                           chunkSize: Int) extends BaseChunkedColumns(columnParams, chunkSize) {
  var indexesChunkedArray = new IntChunkedArray(chunkSize)
  var valuesChunkedArray = new DoubleChunkedArray(chunkSize)
  var indptrChunkedArray = new IntChunkedArray(chunkSize)

  var numCols = 0

  def setNumCols(numCols: Int): Unit = {
    this.numCols = numCols
  }

  override def release(): Unit = {
    // Clear memory
    super.release()
    indexesChunkedArray.delete()
    valuesChunkedArray.delete()
    indptrChunkedArray.delete()
  }
}

class DenseChunkedColumns(columnParams: ColumnParams,
                          chunkSize: Int,
                          val numCols: Int) extends BaseChunkedColumns(columnParams, chunkSize) {
  var featuresChunkedArray = new DoubleChunkedArray(numCols * chunkSize)

  override def release(): Unit = {
    // Clear memory
    super.release()
    featuresChunkedArray.delete()
  }
}

class BaseAggregatedColumns extends AggregatedColumns {
  var labelsArray: FloatSwigArray = _
  var weightArrayOpt: Option[FloatSwigArray] = None
  var initScoreArrayOpt: Option[DoubleSwigArray] = None
  var groupColumnValuesArray: Array[Row] = _

  /**
    * Variables for knowing how large full array should be allocated to
    */
  @volatile var rowCount = 0L
  @volatile var initScoreCount = 0L

  var numCols = 0L
}

trait BaseDenseAggregatedColumns extends BaseAggregatedColumns {
  var featuresArray: DoubleSwigArray = _

  def incrementCount(rowCount: Long,
                     initScoreCount: Long): Unit

  def addRows(labelsChunkedArray: FloatChunkedArray,
              weightChunkedArrayOpt: Option[FloatChunkedArray],
              initScoreChunkedArrayOpt: Option[DoubleChunkedArray],
              featuresChunkedArray: DoubleChunkedArray,
              groupColumnValues: ListBuffer[Row],
              numCols: Int): Unit
}

class DenseAggregatedColumns(chunkSize: Int) extends BaseDenseAggregatedColumns {
  def incrementCount(rowCount: Long,
                     initScoreCount: Long): Unit = {
    this.rowCount += rowCount
    this.initScoreCount += initScoreCount
  }

  /** Adds the rows to the internal data structure.
    * @param labelsChunkedArray The column of label values.
    * @param weightChunkedArrayOpt The optional column of weights, if specified.
    * @param initScoreChunkedArrayOpt The optional column of initial scores, if specified.
    * @param featuresChunkedArray The features vector.
    * @param groupColumnValues The column of group values, if in ranking scenario.
    */
  def addRows(labelsChunkedArray: FloatChunkedArray,
              weightChunkedArrayOpt: Option[FloatChunkedArray],
              initScoreChunkedArrayOpt: Option[DoubleChunkedArray],
              featuresChunkedArray: DoubleChunkedArray,
              groupColumnValues: ListBuffer[Row],
              numCols: Int): Unit = {
    initializeRows(weightChunkedArrayOpt, initScoreChunkedArrayOpt, numCols)
    // Coalesce to main arrays passed to dataset create
    labelsChunkedArray.coalesceTo(this.labelsArray)
    weightChunkedArrayOpt.foreach(_.coalesceTo(this.weightArrayOpt.get))
    initScoreChunkedArrayOpt.foreach(_.coalesceTo(this.initScoreArrayOpt.get))
    featuresChunkedArray.coalesceTo(this.featuresArray)
    for (index <- 0 until groupColumnValues.length) {
      groupColumnValuesArray(index) = groupColumnValues(index)
    }
  }

  private def initializeRows(weightChunkedArrayOpt: Option[FloatChunkedArray],
                             initScoreChunkedArrayOpt: Option[DoubleChunkedArray],
                             numCols: Int): Unit = {
    this.numCols = numCols
    labelsArray = new FloatSwigArray(this.rowCount)
    weightChunkedArrayOpt.foreach(weightChunkedArray => {
      weightArrayOpt = Some(new FloatSwigArray(this.rowCount))
    })
    initScoreChunkedArrayOpt.foreach(initScoreChunkedArray => {
      initScoreArrayOpt = Some(new DoubleSwigArray(this.initScoreCount))
    })
    featuresArray = new DoubleSwigArray(numCols * this.rowCount)
    groupColumnValuesArray = new Array[Row](this.rowCount.toInt)
  }
}

/** Defines class for aggregating rows to a single structure before creating the native LightGBMDataset.
  * @param chunkSize The chunk size for the chunked arrays.
  */
class DenseSyncAggregatedColumns(chunkSize: Int) extends BaseDenseAggregatedColumns {
  /**
    * Variables for current thread to use in order to update common arrays in parallel
    */
  @volatile var threadRowStartIndex = 0L
  @volatile var threadInitScoreStartIndex = 0L

  def incrementCount(rowCount: Long,
                     initScoreCount: Long): Unit = {
    this.synchronized {
      this.rowCount += rowCount
      this.initScoreCount += initScoreCount
    }
  }

  /** Adds the rows to the internal data structure.
    * @param labelsChunkedArray The column of label values.
    * @param weightChunkedArrayOpt The optional column of weights, if specified.
    * @param initScoreChunkedArrayOpt The optional column of initial scores, if specified.
    * @param featuresChunkedArray The features vector.
    * @param groupColumnValues The column of group values, if in ranking scenario.
    */
  def addRows(labelsChunkedArray: FloatChunkedArray,
              weightChunkedArrayOpt: Option[FloatChunkedArray],
              initScoreChunkedArrayOpt: Option[DoubleChunkedArray],
              featuresChunkedArray: DoubleChunkedArray,
              groupColumnValues: ListBuffer[Row],
              numCols: Int): Unit = {
    parallelInitializeRows(weightChunkedArrayOpt, initScoreChunkedArrayOpt, numCols)
    parallelizedCopy(labelsChunkedArray, weightChunkedArrayOpt, initScoreChunkedArrayOpt, featuresChunkedArray,
      groupColumnValues, numCols)
  }

  private def initializeRows(weightChunkedArrayOpt: Option[FloatChunkedArray],
                             initScoreChunkedArrayOpt: Option[DoubleChunkedArray],
                             numCols: Int): Unit = {
    this.numCols = numCols
    labelsArray = new FloatSwigArray(this.rowCount)
    weightChunkedArrayOpt.foreach { _ =>
      weightArrayOpt = Some(new FloatSwigArray(this.rowCount)) }
    initScoreChunkedArrayOpt.foreach{ _ =>
      initScoreArrayOpt = Some(new DoubleSwigArray(this.initScoreCount)) }
    featuresArray = new DoubleSwigArray(numCols * this.rowCount)
    groupColumnValuesArray = new Array[Row](this.rowCount.toInt)
  }

  private def parallelInitializeRows(weightChunkedArrayOpt: Option[FloatChunkedArray],
                                     initScoreChunkedArrayOpt: Option[DoubleChunkedArray],
                                     numCols: Int): Unit = {
    // Initialize arrays if they are not defined - first thread to get here does the initialization for all of them
    if (labelsArray == null) {
      this.synchronized {
        if (labelsArray == null) {
          initializeRows(weightChunkedArrayOpt, initScoreChunkedArrayOpt, numCols)
        }
      }
    }
  }

  private def parallelizedCopy(labelsChunkedArray: FloatChunkedArray,
                               weightChunkedArrayOpt: Option[FloatChunkedArray],
                               initScoreChunkedArrayOpt: Option[DoubleChunkedArray],
                               featuresChunkedArray: DoubleChunkedArray,
                               groupColumnValues: ListBuffer[Row],
                               numCols: Int): Unit = {
    // Parallelized copy to common arrays
    var threadRowStartIndex = 0L
    var threadInitScoreStartIndex = 0L
    var threadIndexesStartIndex = 0L
    var threadIndPtrStartIndex = 0L
    this.synchronized {
      val labelsSize = labelsChunkedArray.getAddCount()
      threadRowStartIndex = this.threadRowStartIndex
      this.threadRowStartIndex = this.threadRowStartIndex + labelsSize.toInt

      threadInitScoreStartIndex = this.threadInitScoreStartIndex
      val initScoreSize = initScoreChunkedArrayOpt.map(_.getAddCount())
      initScoreSize.foreach(size => this.threadInitScoreStartIndex = this.threadInitScoreStartIndex + size)
    }
    ChunkedArrayUtils.copyChunkedArray(labelsChunkedArray, this.labelsArray, threadRowStartIndex, chunkSize)
    weightChunkedArrayOpt.foreach {
      weightChunkedArray =>
        ChunkedArrayUtils.copyChunkedArray(weightChunkedArray, this.weightArrayOpt.get, threadRowStartIndex,
          chunkSize)
    }
    initScoreChunkedArrayOpt.foreach {
      initScoreChunkedArray =>
        ChunkedArrayUtils.copyChunkedArray(initScoreChunkedArray, this.initScoreArrayOpt.get,
          threadInitScoreStartIndex, chunkSize)
    }
    ChunkedArrayUtils.copyChunkedArray(featuresChunkedArray, this.featuresArray, threadRowStartIndex * numCols,
      chunkSize)
    for (index <- threadRowStartIndex until threadRowStartIndex + groupColumnValues.length) {
      groupColumnValuesArray(index.toInt) = groupColumnValues((index - threadRowStartIndex).toInt)
    }
    // rewrite array reference for volatile arrays, see: https://www.javamex.com/tutorials/volatile_arrays.shtml
    this.synchronized {
      groupColumnValuesArray = groupColumnValuesArray
    }
  }
}

trait BaseSparseAggregatedColumns extends BaseAggregatedColumns {
  var indexesArray: IntSwigArray = _
  var valuesArray: DoubleSwigArray = _
  var indptrArray: IntSwigArray = _

  /**
    * Aggregated variables for knowing how large full array should be allocated to
    */
  @volatile var indexesCount = 0L
  @volatile var indptrCount = 0L

  def setNumCols(numCols: Int): Unit = {
    this.numCols = numCols
  }

  def incrementCount(rowCount: Long,
                     initScoreCount: Long,
                     indexesCount: Long,
                     indptrCount: Long): Unit

  def addRows(labelsChunkedArray: FloatChunkedArray,
              weightChunkedArrayOpt: Option[FloatChunkedArray],
              initScoreChunkedArrayOpt: Option[DoubleChunkedArray],
              indexesChunkedArray: IntChunkedArray,
              valuesChunkedArray: DoubleChunkedArray,
              indptrChunkedArray: IntChunkedArray,
              groupColumnValues: ListBuffer[Row]): Unit
}


/** Defines class for aggregating rows to a single structure before creating the native LightGBMDataset.
  * @param chunkSize The chunk size for the chunked arrays.
  */
class SparseAggregatedColumns(chunkSize: Int) extends BaseSparseAggregatedColumns {
  def incrementCount(rowCount: Long,
                     initScoreCount: Long,
                     indexesCount: Long,
                     indptrCount: Long): Unit = {
    this.rowCount += rowCount
    this.initScoreCount += initScoreCount
    this.indexesCount += indexesCount
    this.indptrCount += indptrCount
  }

  private def initializeRows(weightChunkedArrayOpt: Option[FloatChunkedArray],
                             initScoreChunkedArrayOpt: Option[DoubleChunkedArray]): Unit = {
    labelsArray = new FloatSwigArray(this.rowCount)
    weightChunkedArrayOpt.foreach { _ => weightArrayOpt = Some(new FloatSwigArray(this.rowCount)) }
    initScoreChunkedArrayOpt.foreach { _ => initScoreArrayOpt = Some(new DoubleSwigArray(this.initScoreCount)) }
    indexesArray = new IntSwigArray(this.indexesCount)
    valuesArray = new DoubleSwigArray(this.indexesCount)
    indptrArray = new IntSwigArray(this.indptrCount)
    indptrArray.setItem(0, 0)
    groupColumnValuesArray = new Array[Row](this.rowCount.toInt)
  }

  /** Adds the rows to the internal data structure.
    * @param labelsChunkedArray The column of label values.
    * @param weightChunkedArrayOpt The optional column of weights, if specified.
    * @param initScoreChunkedArrayOpt The optional column of initial scores, if specified.
    * @param indexesChunkedArray The feature SparseVector indexes.
    * @param valuesChunkedArray The feature SparseVector values.
    * @param indptrChunkedArray The feature SparseVector indptr.
    * @param groupColumnValues The column of group values, if in ranking scenario.
    */
  def addRows(labelsChunkedArray: FloatChunkedArray,
              weightChunkedArrayOpt: Option[FloatChunkedArray],
              initScoreChunkedArrayOpt: Option[DoubleChunkedArray],
              indexesChunkedArray: IntChunkedArray,
              valuesChunkedArray: DoubleChunkedArray,
              indptrChunkedArray: IntChunkedArray,
              groupColumnValues: ListBuffer[Row]): Unit = {
    initializeRows(weightChunkedArrayOpt, initScoreChunkedArrayOpt)

    // Coalesce to main arrays passed to dataset create
    labelsChunkedArray.coalesceTo(this.labelsArray)
    weightChunkedArrayOpt.foreach(_.coalesceTo(this.weightArrayOpt.get))
    initScoreChunkedArrayOpt.foreach(_.coalesceTo(this.initScoreArrayOpt.get))
    indexesChunkedArray.coalesceTo(this.indexesArray)
    valuesChunkedArray.coalesceTo(this.valuesArray)
    indptrChunkedArray.coalesceTo(this.indptrArray)
    for (index <- 0 until groupColumnValues.length) {
      groupColumnValuesArray(index) = groupColumnValues(index)
    }
  }
}

/** Defines class for aggregating rows to a single structure before creating the native LightGBMDataset.
  * @param chunkSize The chunk size for the chunked arrays.
  */
class SparseSyncAggregatedColumns(chunkSize: Int) extends BaseSparseAggregatedColumns {
  /**
    * Variables for current thread to use in order to update common arrays in parallel
    */
  @volatile var threadRowStartIndex = 0L
  @volatile var threadInitScoreStartIndex = 0L
  @volatile var threadIndexesStartIndex = 0L
  @volatile var threadIndptrStartIndex = 1L

  def incrementCount(rowCount: Long,
                     initScoreCount: Long,
                     indexesCount: Long,
                     indptrCount: Long): Unit = {
    this.synchronized {
      // Add extra 0 for start of indptr in parallel case
      if (this.indptrCount == 0) {
        this.indptrCount += 1
      }
      this.rowCount += rowCount
      this.initScoreCount += initScoreCount
      this.indexesCount += indexesCount
      this.indptrCount += indptrCount
    }
  }

  private def initializeRows(weightChunkedArrayOpt: Option[FloatChunkedArray],
                             initScoreChunkedArrayOpt: Option[DoubleChunkedArray]): Unit = {
    labelsArray = new FloatSwigArray(this.rowCount)
    weightChunkedArrayOpt.foreach(weightChunkedArray => {
      weightArrayOpt = Some(new FloatSwigArray(this.rowCount))
    })
    initScoreChunkedArrayOpt.foreach(initScoreChunkedArray => {
      initScoreArrayOpt = Some(new DoubleSwigArray(this.initScoreCount))
    })
    indexesArray = new IntSwigArray(this.indexesCount)
    valuesArray = new DoubleSwigArray(this.indexesCount)
    indptrArray = new IntSwigArray(this.indptrCount)
    indptrArray.setItem(0, 0)
    groupColumnValuesArray = new Array[Row](this.rowCount.toInt)
  }

  private def parallelInitializeRows(weightChunkedArrayOpt: Option[FloatChunkedArray],
                                     initScoreChunkedArrayOpt: Option[DoubleChunkedArray]): Unit = {
    // Initialize arrays if they are not defined - first thread to get here does the initialization for all of them
    if (labelsArray == null) {
      this.synchronized {
        if (labelsArray == null) {
          initializeRows(weightChunkedArrayOpt, initScoreChunkedArrayOpt)
        }
      }
    }
  }

  def parallelizedCopy(labelsChunkedArray: FloatChunkedArray,
                       weightChunkedArrayOpt: Option[FloatChunkedArray],
                       initScoreChunkedArrayOpt: Option[DoubleChunkedArray],
                       indexesChunkedArray: IntChunkedArray,
                       valuesChunkedArray: DoubleChunkedArray,
                       indptrChunkedArray: IntChunkedArray,
                       groupColumnValues: ListBuffer[Row]): Unit = {
    // Parallelized copy to common arrays
    var threadRowStartIndex = 0L
    var threadInitScoreStartIndex = 0L
    var threadIndexesStartIndex = 0L
    var threadIndPtrStartIndex = 0L
    this.synchronized {
      val labelsSize = labelsChunkedArray.getAddCount()
      threadRowStartIndex = this.threadRowStartIndex
      this.threadRowStartIndex = this.threadRowStartIndex + labelsSize.toInt

      threadInitScoreStartIndex = this.threadInitScoreStartIndex
      val initScoreSize = initScoreChunkedArrayOpt.map(_.getAddCount())
      initScoreSize.foreach(size => this.threadInitScoreStartIndex = this.threadInitScoreStartIndex + size)

      threadIndexesStartIndex = this.threadIndexesStartIndex
      val indexesSize = indexesChunkedArray.getAddCount()
      this.threadIndexesStartIndex += indexesSize

      threadIndPtrStartIndex = this.threadIndptrStartIndex
      val indPtrSize = indptrChunkedArray.getAddCount()
      this.threadIndptrStartIndex += indPtrSize
    }
    ChunkedArrayUtils.copyChunkedArray(labelsChunkedArray, this.labelsArray, threadRowStartIndex, chunkSize)
    weightChunkedArrayOpt.foreach {
      weightChunkedArray =>
        ChunkedArrayUtils.copyChunkedArray(weightChunkedArray, this.weightArrayOpt.get, threadRowStartIndex,
          chunkSize)
    }
    initScoreChunkedArrayOpt.foreach {
      initScoreChunkedArray =>
        ChunkedArrayUtils.copyChunkedArray(initScoreChunkedArray, this.initScoreArrayOpt.get,
          threadInitScoreStartIndex, chunkSize)
    }
    ChunkedArrayUtils.copyChunkedArray(indexesChunkedArray, this.indexesArray, threadIndexesStartIndex, chunkSize)
    ChunkedArrayUtils.copyChunkedArray(valuesChunkedArray, this.valuesArray, threadIndexesStartIndex, chunkSize)
    ChunkedArrayUtils.copyChunkedArray(indptrChunkedArray, this.indptrArray, threadIndPtrStartIndex, chunkSize)
    for (index <- threadRowStartIndex until threadRowStartIndex + groupColumnValues.length) {
      groupColumnValuesArray(index.toInt) = groupColumnValues((index - threadRowStartIndex).toInt)
    }
    // rewrite array reference for volatile arrays, see: https://www.javamex.com/tutorials/volatile_arrays.shtml
    this.synchronized {
      groupColumnValuesArray = groupColumnValuesArray
    }
  }

  /** Adds the rows to the internal data structure.
    * @param labelsChunkedArray The column of label values.
    * @param weightChunkedArrayOpt The optional column of weights, if specified.
    * @param initScoreChunkedArrayOpt The optional column of initial scores, if specified.
    * @param indexesChunkedArray The feature SparseVector indexes.
    * @param valuesChunkedArray The feature SparseVector values.
    * @param indptrChunkedArray The feature SparseVector indptr.
    * @param groupColumnValues The column of group values, if in ranking scenario.
    */
  def addRows(labelsChunkedArray: FloatChunkedArray,
              weightChunkedArrayOpt: Option[FloatChunkedArray],
              initScoreChunkedArrayOpt: Option[DoubleChunkedArray],
              indexesChunkedArray: IntChunkedArray,
              valuesChunkedArray: DoubleChunkedArray,
              indptrChunkedArray: IntChunkedArray,
              groupColumnValues: ListBuffer[Row]): Unit = {
    parallelInitializeRows(weightChunkedArrayOpt, initScoreChunkedArrayOpt)
    parallelizedCopy(labelsChunkedArray, weightChunkedArrayOpt, initScoreChunkedArrayOpt, indexesChunkedArray,
      valuesChunkedArray, indptrChunkedArray, groupColumnValues)
  }
}
