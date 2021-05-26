// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.lightgbm.dataset

import com.microsoft.ml.lightgbm.{doubleChunkedArray, floatChunkedArray}
import com.microsoft.ml.spark.lightgbm.ColumnParams
import com.microsoft.ml.spark.lightgbm.dataset.DatasetUtils.{addFeaturesToChunkedArray, addGroupColumnRow,
  addInitScoreColumnRow, getRowAsDoubleArray}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.{ArrayBuffer, ListBuffer}


/**
  * Defines class for aggregating rows to a single structure before creating the native LightGBMDataset.
  * @param synchronized If true, locks when adding rows to the SparseDatasetAggregator structure.
  */
class SparseDatasetAggregator(synchronized: Boolean) {
  val labelsArray = ArrayBuffer.empty[Double]
  var weightArrayOpt: Option[ArrayBuffer[Double]] = None
  var initScoreArrayOpt: Option[ArrayBuffer[Double]] = None
  var featuresArray = ArrayBuffer.empty[SparseVector]
  val groupColumnValuesArray: ListBuffer[Row] = new ListBuffer[Row]()
  var rowCount: Int = 0

  /** Adds the rows to the internal data structure.
    * @param labels The column of label values.
    * @param weightsOpt The optional column of weights, if specified.
    * @param initScoresOpt The optional column of initial scores, if specified.
    * @param features The column of feature vectors as SparseVector.
    * @param groupColumnValues The column of group values, if in ranking scenario.
    */
  def addRows(labels: Array[Double], weightsOpt: Option[Array[Double]],
              initScoresOpt: Option[Array[Double]],
              features: Array[SparseVector],
              groupColumnValues: ListBuffer[Row]): Unit = {
    if (synchronized) {
      this.synchronized {
        innerAddRows(labels, weightsOpt, initScoresOpt, features, groupColumnValues)
      }
    } else {
      innerAddRows(labels, weightsOpt, initScoresOpt, features, groupColumnValues)
    }
  }

  /** Adds the rows to the internal data structure.
    * @param labels The column of label values.
    * @param weightsOpt The optional column of weights, if specified.
    * @param initScoresOpt The optional column of initial scores, if specified.
    * @param features The column of feature vectors as SparseVector.
    * @param groupColumnValues The column of group values, if in ranking scenario.
    */
  private def innerAddRows(labels: Array[Double], weightsOpt: Option[Array[Double]],
                           initScoresOpt: Option[Array[Double]],
                           features: Array[SparseVector],
                           groupColumnValues: ListBuffer[Row]): Unit = {
    rowCount += labels.length
    labelsArray ++= labels
    weightsOpt.foreach { weights =>
      if (weightArrayOpt.isEmpty) {
        val weightArray = ArrayBuffer.empty[Double]
        weightArray ++= weights
        weightArrayOpt = Some(weightArray)
      } else {
        weightArrayOpt.get ++= weights
      }
    }
    initScoresOpt.foreach { initScores =>
      if (initScoreArrayOpt.isEmpty) {
        val initScoresArray = ArrayBuffer.empty[Double]
        initScoresArray ++= initScores
        initScoreArrayOpt = Some(initScoresArray)
      } else {
        initScoreArrayOpt.get ++= initScores
      }
    }
    featuresArray ++= features
    groupColumnValuesArray ++= groupColumnValues
  }
}

/** Defines class for aggregating rows to a single structure before creating the native LightGBMDataset.
  * @param columnParams The column parameters.
  * @param chunkSize The chunk size for the chunked arrays.
  * @param numCols The number of columns in the dataset.
  * @param schema The schema.
  * @param synchronized If true, locks when adding rows to the DenseDatasetAggregator structure.
  */
class DenseDatasetAggregator(columnParams: ColumnParams, chunkSize: Int, numCols: Int,
                             schema: StructType, synchronized: Boolean) {
  val labelsChunkedArray = new floatChunkedArray(chunkSize)
  val weightChunkedArrayOpt = columnParams.weightColumn.map {
    _ => new floatChunkedArray(chunkSize)
  }
  val initScoreChunkedArrayOpt = columnParams.initScoreColumn.map {
    _ => new doubleChunkedArray(chunkSize)
  }
  var featuresChunkedArray = new doubleChunkedArray(numCols * chunkSize)
  val groupColumnValues: ListBuffer[Row] = new ListBuffer[Row]()
  var rowCount: Int = 0

  /** Adds the rows to the internal data structure.
    * @param rowsIter The rows from the dataset as an iterator.
    */
  def addRows(rowsIter: Iterator[Row]): Unit = {
    if (synchronized) {
      this.synchronized {
        innerAddRow(rowsIter)
      }
    } else {
      innerAddRow(rowsIter)
    }
  }

  /** Adds the rows to the internal data structure.
    * @param rowsIter The rows from the dataset as an iterator.
    */
  private def innerAddRow(rowsIter: Iterator[Row]): Unit = {
    // TODO: This can be optimized, right now all of the tasks will be locked on this
    // Note: We can't do this out of order though otherwise it will break ranker group column
    while (rowsIter.hasNext) {
      rowCount += 1
      val row = rowsIter.next()
      labelsChunkedArray.add(row.getDouble(schema.fieldIndex(columnParams.labelColumn)).toFloat)
      columnParams.weightColumn.map { col =>
        weightChunkedArrayOpt.get.add(row.getDouble(schema.fieldIndex(col)).toFloat)
      }
      val rowAsDoubleArray = getRowAsDoubleArray(row, columnParams, schema)
      addFeaturesToChunkedArray(featuresChunkedArray, rowAsDoubleArray)
      addInitScoreColumnRow(initScoreChunkedArrayOpt, row, columnParams, schema)
      addGroupColumnRow(row, groupColumnValues, columnParams, schema)
    }
  }
}
