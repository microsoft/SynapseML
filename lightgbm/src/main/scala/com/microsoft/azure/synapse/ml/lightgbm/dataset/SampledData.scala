// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.dataset

import org.apache.spark.ml.linalg.{DenseVector, SparseVector}
import org.apache.spark.sql.Row
import com.microsoft.azure.synapse.ml.lightgbm.swig._
import com.microsoft.ml.lightgbm._

/** SampledData: Encapsulates the sampled data need to initialize a LightGBM dataset.
  * .
  * LightGBM expects sampled data to be an array of vectors, where each feature column
  * has a sparse representation of non-zero values (i.e. indexes and data vector). It also needs
  * a #features sized array of element count per feature to know how long each column is.
  * .
  * Since we create sampled data as a self-contained set with ONLY sampled data and nothing else,
  * the indexes are trivial (0 until #elements). We don't need to maintain original raw indexes. LightGBM
  * only uses this data to get distributions, and does not care about raw row indexes.
  * .
  * This class manages keeping all the indexing in sync so callers can just push rows of data into it
  * and retrieve the resulting pointers at the end.
  * .
  * Note: sample data row count is not expected to exceed max(Int), so we index with Ints.
  */
case class SampledData(numRows: Int, numCols: Int) {

  // Allocate full arrays for each feature column, but we will push only non-zero values and
  // keep track of actual counts in rowCounts array
  val sampleData = new DoublePointerSwigArray(numCols)
  val sampleIndexes = new IntPointerSwigArray(numCols)
  val rowCounts = new IntSwigArray(numCols)

  // Initialize column vectors (might move some of this to inside XPointerSwigArray)
  (0 until numCols).foreach(col => {
    rowCounts.setItem(col, 0) // Initialize as 0-rowCount columns

    sampleData.setItem(col, new DoubleSwigArray(numRows))
    sampleIndexes.setItem(col, new IntSwigArray(numRows))
  })

  // Store non-zero elements in arrays given a feature value row
  def pushRow(rowData: Row, index: Int, featureColName: String): Unit = {
    val data = rowData.getAs[Any](featureColName)
    data match {
      case sparse: SparseVector => pushRow(sparse, index)
      case dense: DenseVector => pushRow(dense, index)
      case _ => throw new IllegalArgumentException("Unknown row data type to push")
    }
  }

  // Store non-zero elements in arrays given a dense feature value row
  def pushRow(rowData: DenseVector, index: Int): Unit = pushRow(rowData.values, index)

  // Store non-zero elements in arrays given a dense feature value array
  def pushRow(rowData: Array[Double], index: Int): Unit = {
    require(rowData.length <= numCols, s"Row is too large for sample data.  size should be $numCols" +
                                     s", but is ${rowData.length}")
    (0 until numCols).foreach(col => pushRowElementIfNotZero(col, rowData(col), index))
  }

  // Store non-zero elements in arrays given a sparse feature value row
  def pushRow(rowData: SparseVector, index: Int): Unit = {
    require(rowData.size <= numCols, s"Row is too large for sample data.  size should be $numCols" +
                                     s", but is ${rowData.size}")
    (0 until rowData.numActives).foreach(i =>
      pushRowElementIfNotZero(rowData.indices(i), rowData.values(i), index))
  }

  private def pushRowElementIfNotZero(col: Int, value: Double, index: Int): Unit = {
    if (value != 0.0) {
      val nextIndex = rowCounts.getItem(col)
      sampleData.pushElement(col, nextIndex, value)
      sampleIndexes.pushElement(col, nextIndex, index)
      rowCounts.setItem(col, nextIndex + 1) // increment row count
    }
  }

  def getSampleData: SWIGTYPE_p_p_double = {
    sampleData.array
  }

  def getSampleIndices: SWIGTYPE_p_p_int = {
    sampleIndexes.array
  }

  def getRowCounts: SWIGTYPE_p_int = {
    rowCounts.array
  }

  def delete(): Unit = {
    sampleData.delete()
    sampleIndexes.delete()
    rowCounts.delete()
  }
}
