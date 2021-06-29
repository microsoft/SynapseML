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
  val mainExecutorWorker: Long = LightGBMUtils.getTaskId()
  val useSingleDataset: Boolean = trainParams.executionParams.useSingleDatasetMode
  val chunkSize: Int = trainParams.executionParams.chunkSize
  val matrixType: String = trainParams.executionParams.matrixType

  lazy val denseAggregatedColumns: BaseDenseAggregatedColumns = if (useSingleDataset) {
    new DenseSyncAggregatedColumns(chunkSize)
  } else {
    new DenseAggregatedColumns(chunkSize)
  }

  lazy val sparseAggregatedColumns: BaseSparseAggregatedColumns = if (useSingleDataset) {
    new SparseSyncAggregatedColumns(chunkSize)
  } else {
    new SparseAggregatedColumns(chunkSize)
  }

  def prep(iter: Iterator[Row]): AggregatedColumns = {
    var (concatRowsIter: Iterator[Row], isSparse: Boolean) = getArrayType(iter,
      columnParams, schema, matrixType)
    // Note: the first worker sets "is sparse", other workers read it
    linkIsSparse(isSparse)

    val aggregatedColumns =
      if (!isSparse) {
        val headRow = concatRowsIter.next()
        val rowAsDoubleArray = getRowAsDoubleArray(headRow, columnParams, schema)
        val numCols = rowAsDoubleArray.length
        copyRowsToDenseChunkedColumns(headRow, concatRowsIter, columnParams, schema, chunkSize, numCols)
      } else {
        copyRowsToSparseChunkedColumns(concatRowsIter, columnParams, schema, chunkSize)
      }
    aggregatedColumns
  }

  def merge(ts: Seq[AggregatedColumns]): AggregatedColumns = {
    val isSparseVal = isSparse.get
    if (!isSparseVal) {
      val dcc = ts.asInstanceOf[Seq[DenseChunkedColumns]](0)
      val rowCount = dcc.getRowCount
      val initScoreCount = dcc.initScores.map(_.getAddCount()).getOrElse(0L)
      denseAggregatedColumns.incrementCount(rowCount, initScoreCount)
      if (useSingleDataset) {
        arrayProcessedSignal.countDown()
        arrayProcessedSignal.await()
      }
      denseAggregatedColumns.addRows(dcc.labels, dcc.weights,
        dcc.initScores, dcc.featuresChunkedArray, dcc.groups, dcc.numCols)
      dcc.release()
      denseAggregatedColumns
    } else {
      val scc = ts.asInstanceOf[Seq[SparseChunkedColumns]](0)
      val rowCount = scc.getRowCount
      val initScoreCount = scc.initScores.map(_.getAddCount()).getOrElse(0L)
      val indexesCount = scc.indexesChunkedArray.getAddCount()
      val indptrCount = scc.indptrChunkedArray.getAddCount()
      sparseAggregatedColumns.incrementCount(rowCount, initScoreCount, indexesCount, indptrCount)
      if (useSingleDataset) {
        arrayProcessedSignal.countDown()
        arrayProcessedSignal.await()
      }
      sparseAggregatedColumns.setNumCols(scc.getNumCols)
      sparseAggregatedColumns.addRows(scc.labels, scc.weights,
        scc.initScores, scc.indexesChunkedArray, scc.valuesChunkedArray, scc.indptrChunkedArray,
        scc.groups)
      scc.release()
      sparseAggregatedColumns
    }
  }

  def copyRowsToDenseChunkedColumns(headRow: Row,
                                    rowsIter: Iterator[Row],
                                    columnParams: ColumnParams,
                                    schema: StructType,
                                    chunkSize: Int,
                                    numCols: Int): DenseChunkedColumns = {
    val denseChunkedColumns = new DenseChunkedColumns(columnParams, chunkSize, numCols)
    while (rowsIter.hasNext || denseChunkedColumns.getRowCount == 0) {
      val row = if (denseChunkedColumns.getRowCount == 0) headRow else rowsIter.next()
      denseChunkedColumns.addRow()
      denseChunkedColumns.labels.add(row.getDouble(schema.fieldIndex(columnParams.labelColumn)).toFloat)
      columnParams.weightColumn.foreach { col =>
        denseChunkedColumns.weights.get.add(row.getDouble(schema.fieldIndex(col)).toFloat)
      }
      val rowAsDoubleArray = getRowAsDoubleArray(row, columnParams, schema)
      addFeaturesToChunkedArray(denseChunkedColumns.featuresChunkedArray, rowAsDoubleArray)
      addInitScoreColumnRow(denseChunkedColumns.initScores, row, columnParams, schema)
      addGroupColumnRow(row, denseChunkedColumns.groups, columnParams, schema)
    }
    denseChunkedColumns
  }

  def copyRowsToSparseChunkedColumns(rowsIter: Iterator[Row],
                                     columnParams: ColumnParams,
                                     schema: StructType,
                                     chunkSize: Int): SparseChunkedColumns = {
    val sparseChunkedColumns = new SparseChunkedColumns(columnParams, chunkSize)
    if (!useSingleDataset) {
      sparseChunkedColumns.indptrChunkedArray.add(0)
    }
    while (rowsIter.hasNext) {
      sparseChunkedColumns.addRow()
      val row = rowsIter.next()
      sparseChunkedColumns.labels.add(row.getDouble(schema.fieldIndex(columnParams.labelColumn)).toFloat)
      columnParams.weightColumn.foreach { col =>
        sparseChunkedColumns.weights.get.add(row.getDouble(schema.fieldIndex(col)).toFloat)
      }
      val sparseVector = row.get(schema.fieldIndex(columnParams.featuresColumn)) match {
        case dense: DenseVector => dense.toSparse
        case sparse: SparseVector => sparse
      }
      sparseVector.values.foreach(sparseChunkedColumns.valuesChunkedArray.add(_))
      sparseVector.indices.foreach(sparseChunkedColumns.indexesChunkedArray.add(_))
      sparseChunkedColumns.setNumCols(sparseVector.size)
      sparseChunkedColumns.indptrChunkedArray.add(sparseVector.numNonzeros)
      addInitScoreColumnRow(sparseChunkedColumns.initScores, row, columnParams, schema)
      addGroupColumnRow(row, sparseChunkedColumns.groups, columnParams, schema)
    }
    sparseChunkedColumns
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
      val count = arrayProcessedSignal.getCount().toInt + 1
      arrayProcessedSignal = new CountDownLatch(count)
      log.info(s"Task incrementing ArrayProcessedSignal to $count")
      count
    }
  }

  @volatile var doneSignal: CountDownLatch = new CountDownLatch(0)
  def incrementDoneSignal(log: Logger): Unit = {
    this.synchronized {
      val count = doneSignal.getCount().toInt + 1
      doneSignal = new CountDownLatch(count)
      log.info(s"Task incrementing DoneSignal to $count")
    }
  }

  @volatile var helperStartSignal: CountDownLatch = new CountDownLatch(1)
}
