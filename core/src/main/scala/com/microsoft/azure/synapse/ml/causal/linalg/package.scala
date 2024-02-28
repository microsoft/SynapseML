// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.causal

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, SparseVector => BSV}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, Dataset}

package object linalg {
  type DVector = Dataset[VectorEntry]
  type DMatrix = Dataset[MatrixEntry]

  implicit class BDMConverter(val matrix: BDM[Double]) {
    private lazy val spark = org.apache.spark.sql.SparkSession.active
    import spark.implicits._

    def toDMatrix: DMatrix = {
      matrix.iterator.map {
        case ((i, j), value) => (i, j, value)
      }.toSeq.toDF("i", "j", "value").as[MatrixEntry]
    }
  }

  implicit class BDVConverter(val vector: BDV[Double]) {
    private lazy val spark = org.apache.spark.sql.SparkSession.active
    import spark.implicits._

    def toDVector: DVector = {
      vector.iterator.toSeq.toDF("i", "value").as[VectorEntry]
    }
  }

  implicit class DMatrixConverter(val matrix: DMatrix)(implicit matrixOps: MatrixOps[DMatrix, DVector]) {
    def toBreeze: BDM[Double] = {
      val size = matrixOps.size(matrix)
      val bdm: BDM[Double] = BDM.zeros[Double](size._1.toInt, size._2.toInt)
      matrix.collect.foreach {
        case MatrixEntry(i, j, value) =>
          bdm(i.toInt, j.toInt) = value
      }
      bdm
    }
  }

  implicit class DVectorConverter(val vector: DVector)(implicit vectorOps: VectorOps[DVector]) {
    def toBreeze: BDV[Double] = {
      val values = vector.collect.map(entry => (entry.i.toInt, entry.value)).toSeq
      BSV(values.length)(values: _*).toDenseVector
    }
  }

  implicit class DataFrameConverter(val df: Dataset[_]) {
    import df.sparkSession.implicits._

    def toDMatrix(rowIdxCol: String, columnIdxCol: String, valueCol: String): DMatrix = {
      toDMatrix(col(rowIdxCol), col(columnIdxCol), col(valueCol))
    }

    def toDMatrix(rowIdxCol: Column, columnIdxCol: Column, valueCol: Column): DMatrix = {
      df.select(rowIdxCol.as("i"), columnIdxCol.as("j"), valueCol.as("value"))
        .as[MatrixEntry]
    }

    def toDVector(idxCol: String, valueCol: String): DVector = {
      df.select(col(idxCol).as("i"), col(valueCol).as("value"))
        .as[VectorEntry]
    }

    def toDVector(idxCol: Column, valueCol: Column): DVector = {
      df.select(idxCol.as("i"), valueCol.as("value"))
        .as[VectorEntry]
    }
  }
}
