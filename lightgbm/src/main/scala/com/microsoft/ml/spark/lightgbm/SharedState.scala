// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.lightgbm

import java.util.concurrent.CountDownLatch

import com.microsoft.ml.spark.lightgbm.dataset.DatasetUtils._
import com.microsoft.ml.spark.lightgbm.dataset._
import com.microsoft.ml.spark.lightgbm.params.TrainParams
import org.apache.spark.ml.linalg.{DenseVector, SparseVector}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.slf4j.Logger

class SharedState(columnParams: ColumnParams,
                  schema: StructType,
                  trainParams: TrainParams) {
  val mainExecutorWorker: Long = LightGBMUtils.getTaskId
  val useSingleDataset: Boolean = trainParams.executionParams.useSingleDatasetMode
  val chunkSize: Int = trainParams.executionParams.chunkSize
  val matrixType: String = trainParams.executionParams.matrixType

  lazy val denseAggregatedColumns: BaseDenseAggregatedColumns = new DenseSyncAggregatedColumns(chunkSize)

  lazy val sparseAggregatedColumns: BaseSparseAggregatedColumns = new SparseSyncAggregatedColumns(chunkSize)

  def getArrayType(rowsIter: Iterator[Row], matrixType: String): (Iterator[Row], Boolean) = {
    if (matrixType == "auto") {
      sampleRowsForArrayType(rowsIter, schema, columnParams)
    } else if (matrixType == "sparse") {
      (rowsIter: Iterator[Row], true)
    } else if (matrixType == "dense") {
      (rowsIter: Iterator[Row], false)
    } else {
      throw new Exception(s"Invalid parameter matrix type specified: ${matrixType}")
    }
  }

  def prep(iter: Iterator[Row]): BaseChunkedColumns = {
    val (concatRowsIter: Iterator[Row], isSparseHere: Boolean) = getArrayType(iter, matrixType)

    // Note: the first worker sets "is sparse", other workers read it
    linkIsSparse(isSparseHere)

    if (!isSparse.get) {
      val headRow = concatRowsIter.next()
      val rowAsDoubleArray = getRowAsDoubleArray(headRow, columnParams, schema)
      val numCols = rowAsDoubleArray.length
      val rowsIter = Iterator(headRow) ++ concatRowsIter
      val denseChunkedColumns = new DenseChunkedColumns(columnParams, schema, chunkSize, numCols)
      denseChunkedColumns.addRows(rowsIter)
      denseChunkedColumns
    } else {
      val sparseChunkedColumns = new SparseChunkedColumns(columnParams, schema, chunkSize, useSingleDataset)
      sparseChunkedColumns.addRows(concatRowsIter)
      sparseChunkedColumns
    }
  }

  def merge(ts: BaseChunkedColumns): BaseAggregatedColumns = {
    val isSparseVal = isSparse.get
    val aggregatedColumns = if (!isSparseVal) {
      if (useSingleDataset) denseAggregatedColumns
      else new DenseAggregatedColumns(chunkSize)
    } else {
      if (useSingleDataset) sparseAggregatedColumns
      else new SparseAggregatedColumns(chunkSize)
    }
    aggregatedColumns.incrementCount(ts)
    if (useSingleDataset) {
      arrayProcessedSignal.countDown()
      arrayProcessedSignal.await()
    }
    aggregatedColumns.addRows(ts)
    ts.release()
    aggregatedColumns
  }

  @volatile var isSparse: Option[Boolean] = None
  def linkIsSparse(isSparse: Boolean): Unit = {
    if (this.isSparse.isEmpty) {
      this.synchronized {
        if (this.isSparse.isEmpty) {
          this.isSparse = Some(isSparse)
        }
      }
    }
  }

  @volatile var arrayProcessedSignal: CountDownLatch = new CountDownLatch(0)
  def incrementArrayProcessedSignal(log: Logger): Int = {
    this.synchronized {
      val count = arrayProcessedSignal.getCount.toInt + 1
      arrayProcessedSignal = new CountDownLatch(count)
      log.info(s"Task incrementing ArrayProcessedSignal to $count")
      count
    }
  }

  @volatile var doneSignal: CountDownLatch = new CountDownLatch(0)
  def incrementDoneSignal(log: Logger): Unit = {
    this.synchronized {
      val count = doneSignal.getCount.toInt + 1
      doneSignal = new CountDownLatch(count)
      log.info(s"Task incrementing DoneSignal to $count")
    }
  }

  @volatile var helperStartSignal: CountDownLatch = new CountDownLatch(1)
}
