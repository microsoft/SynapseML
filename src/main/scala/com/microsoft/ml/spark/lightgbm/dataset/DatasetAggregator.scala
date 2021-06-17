// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.lightgbm.dataset

import com.microsoft.ml.lightgbm.{SWIGTYPE_p_double, SWIGTYPE_p_float, SWIGTYPE_p_int, doubleChunkedArray,
  floatChunkedArray, int32ChunkedArray, lightgbmlib}
import com.microsoft.ml.spark.lightgbm.ColumnParams
import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer

object ChunkedArrayUtils {
  def copyFloatChunkedArray(chunkedArray: floatChunkedArray,
                            mainArray: Option[SWIGTYPE_p_float],
                            threadRowStartIndex: Long,
                            chunkSize: Long): Unit = {
    // Copy in parallel on each thread
    // First copy full chunks
    val chunkCount = chunkedArray.get_chunks_count() - 1
    for (chunk <- 0L until chunkCount) {
      for (in_chunk_idx <- 0L until chunkSize) {
        lightgbmlib.floatArray_setitem(mainArray.get,
          threadRowStartIndex + chunk * chunkSize + in_chunk_idx,
          chunkedArray.getitem(chunk, in_chunk_idx, -1))
      }
    }
    // Next copy filled values from last chunk only
    val lastChunkCount = chunkedArray.get_last_chunk_add_count()
    for (last_chunk_idx <- 0L until lastChunkCount) {
      lightgbmlib.floatArray_setitem(mainArray.get,
        threadRowStartIndex + chunkCount * chunkSize + last_chunk_idx,
        chunkedArray.getitem(chunkCount, last_chunk_idx, -1))
    }
  }

  def copyDoubleChunkedArray(chunkedArray: doubleChunkedArray,
                             mainArray: Option[SWIGTYPE_p_double],
                             threadRowStartIndex: Long,
                             chunkSize: Long): Unit = {
    // Copy in parallel on each thread
    // First copy full chunks
    val chunkCount = chunkedArray.get_chunks_count() - 1
    for (chunk <- 0L until chunkCount) {
      for (in_chunk_idx <- 0L until chunkSize) {
        lightgbmlib.doubleArray_setitem(mainArray.get,
          threadRowStartIndex + chunk * chunkSize + in_chunk_idx,
          chunkedArray.getitem(chunk, in_chunk_idx, -1))
      }
    }
    // Next copy filled values from last chunk only
    val lastChunkCount = chunkedArray.get_last_chunk_add_count()
    for (last_chunk_idx <- 0L until lastChunkCount) {
      lightgbmlib.doubleArray_setitem(mainArray.get,
        threadRowStartIndex + chunkCount * chunkSize + last_chunk_idx,
        chunkedArray.getitem(chunkCount, last_chunk_idx, -1))
    }
  }

  def copyIntChunkedArray(chunkedArray: int32ChunkedArray,
                          mainArray: Option[SWIGTYPE_p_int],
                          threadRowStartIndex: Long,
                          chunkSize: Long): Unit = {
    // Copy in parallel on each thread
    // First copy full chunks
    val chunkCount = chunkedArray.get_chunks_count() - 1
    for (chunk <- 0L until chunkCount) {
      for (in_chunk_idx <- 0L until chunkSize) {
        lightgbmlib.intArray_setitem(mainArray.get,
          threadRowStartIndex + chunk * chunkSize + in_chunk_idx,
          chunkedArray.getitem(chunk, in_chunk_idx, -1))
      }
    }
    // Next copy filled values from last chunk only
    val lastChunkCount = chunkedArray.get_last_chunk_add_count()
    for (last_chunk_idx <- 0L until lastChunkCount) {
      lightgbmlib.intArray_setitem(mainArray.get,
        threadRowStartIndex + chunkCount * chunkSize + last_chunk_idx,
        chunkedArray.getitem(chunkCount, last_chunk_idx, -1))
    }
  }
}

trait AggregatedColumns

class SparseChunkedColumns(columnParams: ColumnParams,
                           chunkSize: Int) extends AggregatedColumns {
  val labelsChunkedArray = new floatChunkedArray(chunkSize)
  val weightChunkedArrayOpt = columnParams.weightColumn.map {
    _ => new floatChunkedArray(chunkSize)
  }
  val initScoreChunkedArrayOpt = columnParams.initScoreColumn.map {
    _ => new doubleChunkedArray(chunkSize)
  }
  var indexesChunkedArray = new int32ChunkedArray(chunkSize)
  var valuesChunkedArray = new doubleChunkedArray(chunkSize)
  var indptrChunkedArray = new int32ChunkedArray(chunkSize)
  val groupColumnValues: ListBuffer[Row] = new ListBuffer[Row]()

  var rowCount = 0
  var numCols = 0

  def setNumCols(numCols: Int): Unit = {
    this.numCols = numCols
  }

  def release(): Unit = {
    clearSparseArrays(labelsChunkedArray, weightChunkedArrayOpt, initScoreChunkedArrayOpt,
      indexesChunkedArray, valuesChunkedArray, indptrChunkedArray)
  }

  def clearSparseArrays(labelsChunkedArray: floatChunkedArray,
                        weightChunkedArrayOpt: Option[floatChunkedArray],
                        initScoreChunkedArrayOpt: Option[doubleChunkedArray],
                        indexesChunkedArray: int32ChunkedArray,
                        valuesChunkedArray: doubleChunkedArray,
                        indptrChunkedArray: int32ChunkedArray): Unit = {
    // Clear memory
    labelsChunkedArray.delete()
    weightChunkedArrayOpt.foreach(_.delete())
    initScoreChunkedArrayOpt.foreach(_.delete())
    indexesChunkedArray.delete()
    valuesChunkedArray.delete()
    indptrChunkedArray.delete()
  }
}

class DenseChunkedColumns(columnParams: ColumnParams,
                          chunkSize: Int,
                          val numCols: Int) extends AggregatedColumns {
  val labelsChunkedArray = new floatChunkedArray(chunkSize)
  val weightChunkedArrayOpt = columnParams.weightColumn.map {
    _ => new floatChunkedArray(chunkSize)
  }
  val initScoreChunkedArrayOpt = columnParams.initScoreColumn.map {
    _ => new doubleChunkedArray(chunkSize)
  }
  var featuresChunkedArray = new doubleChunkedArray(numCols * chunkSize)
  val groupColumnValues: ListBuffer[Row] = new ListBuffer[Row]()

  var rowCount = 0

  def release(): Unit = {
    clearDenseArrays(labelsChunkedArray, weightChunkedArrayOpt, initScoreChunkedArrayOpt,
      featuresChunkedArray)
  }

  def clearDenseArrays(labelsChunkedArray: floatChunkedArray,
                       weightChunkedArrayOpt: Option[floatChunkedArray],
                       initScoreChunkedArrayOpt: Option[doubleChunkedArray],
                       featuresChunkedArray: doubleChunkedArray): Unit = {
    // Clear memory
    labelsChunkedArray.delete()
    weightChunkedArrayOpt.foreach(_.delete())
    initScoreChunkedArrayOpt.foreach(_.delete())
    featuresChunkedArray.delete()
  }
}

trait BaseDenseAggregatedColumns extends AggregatedColumns {
  var labelsArray: Option[SWIGTYPE_p_float] = None
  var weightArrayOpt: Option[SWIGTYPE_p_float] = None
  var initScoreArrayOpt: Option[SWIGTYPE_p_double] = None
  var featuresArray: Option[SWIGTYPE_p_double] = None
  var groupColumnValuesArray: Array[Row] = _

  /**
    * Variables for knowing how large full array should be allocated to
    */
  @volatile var rowCount = 0L
  @volatile var initScoreCount = 0L

  var numCols = 0L

  def incrementCount(rowCount: Long,
                     initScoreCount: Long): Unit

  def addRows(labelsChunkedArray: floatChunkedArray,
              weightChunkedArrayOpt: Option[floatChunkedArray],
              initScoreChunkedArrayOpt: Option[doubleChunkedArray],
              featuresChunkedArray: doubleChunkedArray,
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
  def addRows(labelsChunkedArray: floatChunkedArray,
              weightChunkedArrayOpt: Option[floatChunkedArray],
              initScoreChunkedArrayOpt: Option[doubleChunkedArray],
              featuresChunkedArray: doubleChunkedArray,
              groupColumnValues: ListBuffer[Row],
              numCols: Int): Unit = {
    initializeRows(weightChunkedArrayOpt, initScoreChunkedArrayOpt, numCols)
    // Coalesce to main arrays passed to dataset create
    labelsChunkedArray.coalesce_to(this.labelsArray.get)
    weightChunkedArrayOpt.foreach {
      weightChunkedArray =>
        weightChunkedArray.coalesce_to(this.weightArrayOpt.get)
    }
    initScoreChunkedArrayOpt.foreach {
      initScoreChunkedArray =>
        initScoreChunkedArray.coalesce_to(this.initScoreArrayOpt.get)
    }
    featuresChunkedArray.coalesce_to(this.featuresArray.get)
    for (index <- 0 until groupColumnValues.length) {
      groupColumnValuesArray(index) = groupColumnValues(index)
    }
  }

  private def initializeRows(weightChunkedArrayOpt: Option[floatChunkedArray],
                             initScoreChunkedArrayOpt: Option[doubleChunkedArray],
                             numCols: Int): Unit = {
    this.numCols = numCols
    labelsArray = Some(lightgbmlib.new_floatArray(this.rowCount))
    weightChunkedArrayOpt.foreach(weightChunkedArray => {
      weightArrayOpt = Some(lightgbmlib.new_floatArray(this.rowCount))
    })
    initScoreChunkedArrayOpt.foreach(initScoreChunkedArray => {
      initScoreArrayOpt = Some(lightgbmlib.new_doubleArray(this.initScoreCount))
    })
    featuresArray = Some(lightgbmlib.new_doubleArray(numCols * this.rowCount))
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
  def addRows(labelsChunkedArray: floatChunkedArray,
              weightChunkedArrayOpt: Option[floatChunkedArray],
              initScoreChunkedArrayOpt: Option[doubleChunkedArray],
              featuresChunkedArray: doubleChunkedArray,
              groupColumnValues: ListBuffer[Row],
              numCols: Int): Unit = {
    parallelInitializeRows(weightChunkedArrayOpt, initScoreChunkedArrayOpt, numCols)
    parallelizedCopy(labelsChunkedArray, weightChunkedArrayOpt, initScoreChunkedArrayOpt, featuresChunkedArray,
      groupColumnValues, numCols)
  }

  private def initializeRows(weightChunkedArrayOpt: Option[floatChunkedArray],
                             initScoreChunkedArrayOpt: Option[doubleChunkedArray],
                             numCols: Int): Unit = {
    this.numCols = numCols
    labelsArray = Some(lightgbmlib.new_floatArray(this.rowCount))
    weightChunkedArrayOpt.foreach(weightChunkedArray => {
      weightArrayOpt = Some(lightgbmlib.new_floatArray(this.rowCount))
    })
    initScoreChunkedArrayOpt.foreach(initScoreChunkedArray => {
      initScoreArrayOpt = Some(lightgbmlib.new_doubleArray(this.initScoreCount))
    })
    featuresArray = Some(lightgbmlib.new_doubleArray(numCols * this.rowCount))
    groupColumnValuesArray = new Array[Row](this.rowCount.toInt)
  }

  private def parallelInitializeRows(weightChunkedArrayOpt: Option[floatChunkedArray],
                                     initScoreChunkedArrayOpt: Option[doubleChunkedArray],
                                     numCols: Int): Unit = {
    // Initialize arrays if they are not defined - first thread to get here does the initialization for all of them
    if (!labelsArray.isDefined) {
      this.synchronized {
        if (!labelsArray.isDefined) {
          initializeRows(weightChunkedArrayOpt, initScoreChunkedArrayOpt, numCols)
        }
      }
    }
  }

  private def parallelizedCopy(labelsChunkedArray: floatChunkedArray,
                               weightChunkedArrayOpt: Option[floatChunkedArray],
                               initScoreChunkedArrayOpt: Option[doubleChunkedArray],
                               featuresChunkedArray: doubleChunkedArray,
                               groupColumnValues: ListBuffer[Row],
                               numCols: Int): Unit = {
    // Parallelized copy to common arrays
    var threadRowStartIndex = 0L
    var threadInitScoreStartIndex = 0L
    var threadIndexesStartIndex = 0L
    var threadIndPtrStartIndex = 0L
    this.synchronized {
      val labelsSize = labelsChunkedArray.get_add_count()
      threadRowStartIndex = this.threadRowStartIndex
      this.threadRowStartIndex = this.threadRowStartIndex + labelsSize.toInt

      threadInitScoreStartIndex = this.threadInitScoreStartIndex
      val initScoreSize = initScoreChunkedArrayOpt.map(_.get_add_count())
      initScoreSize.foreach(size => this.threadInitScoreStartIndex = this.threadInitScoreStartIndex + size)
    }
    ChunkedArrayUtils.copyFloatChunkedArray(labelsChunkedArray, this.labelsArray, threadRowStartIndex, chunkSize)
    weightChunkedArrayOpt.foreach {
      weightChunkedArray =>
        ChunkedArrayUtils.copyFloatChunkedArray(weightChunkedArray, this.weightArrayOpt, threadRowStartIndex,
          chunkSize)
    }
    initScoreChunkedArrayOpt.foreach {
      initScoreChunkedArray =>
        ChunkedArrayUtils.copyDoubleChunkedArray(initScoreChunkedArray, this.initScoreArrayOpt,
          threadInitScoreStartIndex, chunkSize)
    }
    ChunkedArrayUtils.copyDoubleChunkedArray(featuresChunkedArray, this.featuresArray, threadRowStartIndex * numCols,
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

trait BaseSparseAggregatedColumns extends AggregatedColumns {
  var labelsArray: Option[SWIGTYPE_p_float] = None
  var weightArrayOpt: Option[SWIGTYPE_p_float] = None
  var initScoreArrayOpt: Option[SWIGTYPE_p_double] = None
  var indexesArray: Option[SWIGTYPE_p_int] = None
  var valuesArray: Option[SWIGTYPE_p_double] = None
  var indptrArray: Option[SWIGTYPE_p_int] = None
  var groupColumnValuesArray: Array[Row] = _

  /**
    * Aggregated variables for knowing how large full array should be allocated to
    */
  @volatile var rowCount = 0L
  @volatile var initScoreCount = 0L
  @volatile var indexesCount = 0L
  @volatile var indptrCount = 0L

  @volatile var numCols: Int = 0

  def setNumCols(numCols: Int): Unit = {
    this.numCols = numCols
  }

  def incrementCount(rowCount: Long,
                     initScoreCount: Long,
                     indexesCount: Long,
                     indptrCount: Long): Unit

  def addRows(labelsChunkedArray: floatChunkedArray,
              weightChunkedArrayOpt: Option[floatChunkedArray],
              initScoreChunkedArrayOpt: Option[doubleChunkedArray],
              indexesChunkedArray: int32ChunkedArray,
              valuesChunkedArray: doubleChunkedArray,
              indptrChunkedArray: int32ChunkedArray,
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

  private def initializeRows(weightChunkedArrayOpt: Option[floatChunkedArray],
                             initScoreChunkedArrayOpt: Option[doubleChunkedArray]): Unit = {
    labelsArray = Some(lightgbmlib.new_floatArray(this.rowCount))
    weightChunkedArrayOpt.foreach(weightChunkedArray => {
      weightArrayOpt = Some(lightgbmlib.new_floatArray(this.rowCount))
    })
    initScoreChunkedArrayOpt.foreach(initScoreChunkedArray => {
      initScoreArrayOpt = Some(lightgbmlib.new_doubleArray(this.initScoreCount))
    })
    indexesArray = Some(lightgbmlib.new_intArray(this.indexesCount))
    valuesArray = Some(lightgbmlib.new_doubleArray(this.indexesCount))
    indptrArray = Some(lightgbmlib.new_intArray(this.indptrCount))
    lightgbmlib.intArray_setitem(indptrArray.get, 0, 0)
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
  def addRows(labelsChunkedArray: floatChunkedArray,
              weightChunkedArrayOpt: Option[floatChunkedArray],
              initScoreChunkedArrayOpt: Option[doubleChunkedArray],
              indexesChunkedArray: int32ChunkedArray,
              valuesChunkedArray: doubleChunkedArray,
              indptrChunkedArray: int32ChunkedArray,
              groupColumnValues: ListBuffer[Row]): Unit = {
    initializeRows(weightChunkedArrayOpt, initScoreChunkedArrayOpt)

    // Coalesce to main arrays passed to dataset create
    labelsChunkedArray.coalesce_to(this.labelsArray.get)
    weightChunkedArrayOpt.foreach {
      weightChunkedArray =>
        weightChunkedArray.coalesce_to(this.weightArrayOpt.get)
    }
    initScoreChunkedArrayOpt.foreach {
      initScoreChunkedArray =>
        initScoreChunkedArray.coalesce_to(this.initScoreArrayOpt.get)
    }
    indexesChunkedArray.coalesce_to(this.indexesArray.get)
    valuesChunkedArray.coalesce_to(this.valuesArray.get)
    indptrChunkedArray.coalesce_to(this.indptrArray.get)
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

  private def initializeRows(weightChunkedArrayOpt: Option[floatChunkedArray],
                             initScoreChunkedArrayOpt: Option[doubleChunkedArray]): Unit = {
    labelsArray = Some(lightgbmlib.new_floatArray(this.rowCount))
    weightChunkedArrayOpt.foreach(weightChunkedArray => {
      weightArrayOpt = Some(lightgbmlib.new_floatArray(this.rowCount))
    })
    initScoreChunkedArrayOpt.foreach(initScoreChunkedArray => {
      initScoreArrayOpt = Some(lightgbmlib.new_doubleArray(this.initScoreCount))
    })
    indexesArray = Some(lightgbmlib.new_intArray(this.indexesCount))
    valuesArray = Some(lightgbmlib.new_doubleArray(this.indexesCount))
    indptrArray = Some(lightgbmlib.new_intArray(this.indptrCount))
    lightgbmlib.intArray_setitem(indptrArray.get, 0, 0)
    groupColumnValuesArray = new Array[Row](this.rowCount.toInt)
  }

  private def parallelInitializeRows(weightChunkedArrayOpt: Option[floatChunkedArray],
                                     initScoreChunkedArrayOpt: Option[doubleChunkedArray]): Unit = {
    // Initialize arrays if they are not defined - first thread to get here does the initialization for all of them
    if (!labelsArray.isDefined) {
      this.synchronized {
        if (!labelsArray.isDefined) {
          initializeRows(weightChunkedArrayOpt, initScoreChunkedArrayOpt)
        }
      }
    }
  }

  def parallelizedCopy(labelsChunkedArray: floatChunkedArray,
                       weightChunkedArrayOpt: Option[floatChunkedArray],
                       initScoreChunkedArrayOpt: Option[doubleChunkedArray],
                       indexesChunkedArray: int32ChunkedArray,
                       valuesChunkedArray: doubleChunkedArray,
                       indptrChunkedArray: int32ChunkedArray,
                       groupColumnValues: ListBuffer[Row]): Unit = {
    // Parallelized copy to common arrays
    var threadRowStartIndex = 0L
    var threadInitScoreStartIndex = 0L
    var threadIndexesStartIndex = 0L
    var threadIndPtrStartIndex = 0L
    this.synchronized {
      val labelsSize = labelsChunkedArray.get_add_count()
      threadRowStartIndex = this.threadRowStartIndex
      this.threadRowStartIndex = this.threadRowStartIndex + labelsSize.toInt

      threadInitScoreStartIndex = this.threadInitScoreStartIndex
      val initScoreSize = initScoreChunkedArrayOpt.map(_.get_add_count())
      initScoreSize.foreach(size => this.threadInitScoreStartIndex = this.threadInitScoreStartIndex + size)

      threadIndexesStartIndex = this.threadIndexesStartIndex
      val indexesSize = indexesChunkedArray.get_add_count()
      this.threadIndexesStartIndex += indexesSize

      threadIndPtrStartIndex = this.threadIndptrStartIndex
      val indPtrSize = indptrChunkedArray.get_add_count()
      this.threadIndptrStartIndex += indPtrSize
    }
    ChunkedArrayUtils.copyFloatChunkedArray(labelsChunkedArray, this.labelsArray, threadRowStartIndex, chunkSize)
    weightChunkedArrayOpt.foreach {
      weightChunkedArray =>
        ChunkedArrayUtils.copyFloatChunkedArray(weightChunkedArray, this.weightArrayOpt, threadRowStartIndex,
          chunkSize)
    }
    initScoreChunkedArrayOpt.foreach {
      initScoreChunkedArray =>
        ChunkedArrayUtils.copyDoubleChunkedArray(initScoreChunkedArray, this.initScoreArrayOpt,
          threadInitScoreStartIndex, chunkSize)
    }
    ChunkedArrayUtils.copyIntChunkedArray(indexesChunkedArray, this.indexesArray, threadIndexesStartIndex, chunkSize)
    ChunkedArrayUtils.copyDoubleChunkedArray(valuesChunkedArray, this.valuesArray, threadIndexesStartIndex, chunkSize)
    ChunkedArrayUtils.copyIntChunkedArray(indptrChunkedArray, this.indptrArray, threadIndPtrStartIndex, chunkSize)
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
  def addRows(labelsChunkedArray: floatChunkedArray,
              weightChunkedArrayOpt: Option[floatChunkedArray],
              initScoreChunkedArrayOpt: Option[doubleChunkedArray],
              indexesChunkedArray: int32ChunkedArray,
              valuesChunkedArray: doubleChunkedArray,
              indptrChunkedArray: int32ChunkedArray,
              groupColumnValues: ListBuffer[Row]): Unit = {
    parallelInitializeRows(weightChunkedArrayOpt, initScoreChunkedArrayOpt)
    parallelizedCopy(labelsChunkedArray, weightChunkedArrayOpt, initScoreChunkedArrayOpt, indexesChunkedArray,
      valuesChunkedArray, indptrChunkedArray, groupColumnValues)
  }
}
