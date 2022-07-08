// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.dataset

import com.microsoft.azure.synapse.ml.lightgbm.ColumnParams
import com.microsoft.ml.lightgbm.{doubleChunkedArray, floatChunkedArray}
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.{DenseVector, SparseVector}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

object DatasetUtils {

  case class CardinalityTriplet[T](groupCounts: List[Int], currentValue: T, currentCount: Int)

  def countCardinality[T](input: Seq[T]): Array[Int] = {
    val default: T = null.asInstanceOf[T]

    val cardinalityTriplet = input.foldLeft(CardinalityTriplet(List.empty[Int], default, 0)) {
      case (listValue: CardinalityTriplet[T], currentValue) =>

        if (listValue.groupCounts.isEmpty && listValue.currentCount == 0) {
          // Base case, keep list as empty and set cardinality to 1
          CardinalityTriplet(listValue.groupCounts, currentValue, 1)
        }
        else if (listValue.currentValue == currentValue) {
          // Encountered same value
          CardinalityTriplet(listValue.groupCounts, currentValue, listValue.currentCount + 1)
        }
        else {
          // New value, need to reset counter and add new cardinality to list
          CardinalityTriplet(listValue.currentCount :: listValue.groupCounts, currentValue, 1)
        }
    }

    val groupCardinality = (cardinalityTriplet.currentCount :: cardinalityTriplet.groupCounts).reverse.toArray
    groupCardinality
  }


  /**
    * Get whether to use dense or sparse data, using configuration and/or data sampling.
    *
    * @param rowsIter        Iterator of rows.
    * @param matrixType      Matrix type as configured by user..
    * @param featuresColumn  The name of the features column.
    * @return A reconstructed iterator with the same original rows and whether the matrix should be sparse or dense.
    */
  def getArrayType(rowsIter: Iterator[Row], matrixType: String, featuresColumn: String): (Iterator[Row], Boolean) = {
    if (matrixType == "auto") {
      sampleRowsForArrayType(rowsIter, featuresColumn)
    } else if (matrixType == "sparse") {
      (rowsIter, true)
    } else if (matrixType == "dense") {
      (rowsIter, false)
    } else {
      throw new Exception(s"Invalid parameter matrix type specified: ${matrixType}")
    }
  }

  /**
    * Sample the first several rows to determine whether to construct sparse or dense matrix in lightgbm native code.
    *
    * @param rowsIter        Iterator of rows.
    * @param featuresColumn  The name of the features column.
    * @return A reconstructed iterator with the same original rows and whether the matrix should be sparse or dense.
    */
  def sampleRowsForArrayType(rowsIter: Iterator[Row], featuresColumn: String): (Iterator[Row], Boolean) = {
    val numSampledRows = 10
    val sampleRows = rowsIter.take(numSampledRows).toArray
    val numDense = sampleRows
      .map(row => row.getAs[Any](featuresColumn).isInstanceOf[DenseVector])
      .count(value => value)
    val numSparse = sampleRows.length - numDense
    // recreate the iterator
    (sampleRows.toIterator ++ rowsIter, numSparse > numDense)
  }

  def getRowAsDoubleArray(row: Row, columnParams: ColumnParams): Array[Double] = {
    row.getAs[Any](columnParams.featuresColumn) match {
      case dense: DenseVector => dense.toArray
      case sparse: SparseVector => sparse.toArray
    }
  }

  def validateGroupColumn(col: String, schema: StructType): Unit = {
    val datatype = schema(col).dataType
    if (datatype != org.apache.spark.sql.types.IntegerType
      && datatype != org.apache.spark.sql.types.LongType
      && datatype != org.apache.spark.sql.types.StringType) {
      throw new IllegalArgumentException(
        s"group column $col must be of type Long, Int or String but is ${datatype.typeName}")
    }
  }
}
