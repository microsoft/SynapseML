// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.lightgbm.dataset

import com.microsoft.ml.lightgbm.{doubleChunkedArray, floatChunkedArray}
import com.microsoft.ml.spark.lightgbm.{ColumnParams, LightGBMUtils}
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import com.microsoft.ml.spark.lightgbm.params.TrainParams
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.linalg.{DenseVector, SparseVector}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.slf4j.Logger

import scala.collection.mutable.ListBuffer

object DatasetUtils {
  def getArrayType(rowsIter: Iterator[Row],
                   columnParams: ColumnParams,
                   schema: StructType,
                   matrixType: String): (Iterator[Row], Boolean) = {
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

  def generateDataset(rowsIter: Iterator[Row], columnParams: ColumnParams,
                      referenceDataset: Option[LightGBMDataset], schema: StructType,
                      log: Logger, trainParams: TrainParams): Option[LightGBMDataset] = {
    val (concatRowsIter: Iterator[Row], isSparse: Boolean) = getArrayType(rowsIter, columnParams, schema,
      trainParams.executionParams.matrixType)
    var datasetPtr: Option[LightGBMDataset] = None
    if (!isSparse) {
      datasetPtr = aggregateDenseStreamedData(concatRowsIter, columnParams, referenceDataset, schema, log, trainParams)
      // Validate generated dataset has the correct number of rows and cols
      datasetPtr.get.validateDataset()
    } else {
      val rows = concatRowsIter.toArray
      val numRows = rows.length
      val labels = rows.map(row => row.getDouble(schema.fieldIndex(columnParams.labelColumn)))
      val rowsAsSparse = rows.map(row => row.get(schema.fieldIndex(columnParams.featuresColumn)) match {
        case dense: DenseVector => dense.toSparse
        case sparse: SparseVector => sparse
      })
      val numCols = rowsAsSparse(0).size
      val slotNames = getSlotNames(schema, columnParams.featuresColumn, numCols, trainParams)
      log.info(s"LightGBM task generating sparse dataset with $numRows rows and $numCols columns")
      datasetPtr = Some(LightGBMUtils.generateSparseDataset(rowsAsSparse, referenceDataset, slotNames, trainParams))
      // Validate generated dataset has the correct number of rows and cols
      datasetPtr.get.validateDataset()
      datasetPtr.get.addFloatField(labels, "label", numRows)
      columnParams.weightColumn.foreach { col =>
        val weights = rows.map(row => row.getDouble(schema.fieldIndex(col)))
        datasetPtr.get.addFloatField(weights, "weight", numRows)
      }
      addInitScoreColumn(rows, columnParams.initScoreColumn, datasetPtr, numRows, schema)
      addGroupColumn(rows, columnParams.groupColumn, datasetPtr, numRows, schema, None)
    }
    datasetPtr
  }

  trait CardinalityType[T]

  object CardinalityTypes {

    implicit object LongType extends CardinalityType[Long]

    implicit object IntType extends CardinalityType[Int]

    implicit object StringType extends CardinalityType[String]

  }

  import CardinalityTypes._


  case class CardinalityTriplet[T](groupCounts: List[Int], currentValue: T, currentCount: Int)

  def countCardinality[T](input: Seq[T])(implicit ev: CardinalityType[T]): Array[Int] = {
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

  def addInitScoreColumnRow(initScoreChunkedArrayOpt: Option[doubleChunkedArray], row: Row,
                            columnParams: ColumnParams, schema: StructType): Unit = {
    columnParams.initScoreColumn.foreach { col =>
      val field = schema.fields(schema.fieldIndex(col))
      if (field.dataType == VectorType) {
        val initScores = row.get(schema.fieldIndex(col)).asInstanceOf[DenseVector]
        // Note: rows * # classes in multiclass case
        initScores.values.foreach { rowValue =>
          initScoreChunkedArrayOpt.get.add(rowValue)
        }
      } else {
        val initScore = row.getDouble(schema.fieldIndex(col))
        initScoreChunkedArrayOpt.get.add(initScore)
      }
    }
  }

  def addGroupColumnRow(row: Row, groupColumnValues: ListBuffer[Row],
                        columnParams: ColumnParams, schema: StructType): Unit = {
    columnParams.groupColumn.foreach { col =>
      val colIdx = schema.fieldIndex(col)
      groupColumnValues.append(Row(row.get(colIdx)))
    }
  }

  /**
    * Sample the first several rows to determine whether to construct sparse or dense matrix in lightgbm native code.
    * @param rowsIter  Iterator of rows.
    * @param schema The schema.
    * @param columnParams The column parameters.
    * @return A reconstructed iterator with the same original rows and whether the matrix should be sparse or dense.
    */
  def sampleRowsForArrayType(rowsIter: Iterator[Row], schema: StructType,
                             columnParams: ColumnParams): (Iterator[Row], Boolean) = {
    val numSampledRows = 10
    val sampleRows = rowsIter.take(numSampledRows).toArray
    val numDense = sampleRows.map(row =>
      row.get(schema.fieldIndex(columnParams.featuresColumn)).isInstanceOf[DenseVector]).filter(value => value).length
    val numSparse = sampleRows.length - numDense
    // recreate the iterator
    (sampleRows.toIterator ++ rowsIter, numSparse > numDense)
  }

  def getRowAsDoubleArray(row: Row, columnParams: ColumnParams, schema: StructType): Array[Double] = {
    row.get(schema.fieldIndex(columnParams.featuresColumn)) match {
      case dense: DenseVector => dense.toArray
      case sparse: SparseVector => sparse.toDense.toArray
    }
  }

  def addFeaturesToChunkedArray(featuresChunkedArrayOpt: Option[doubleChunkedArray], numCols: Int,
                  rowAsDoubleArray: Array[Double]): Unit = {
    featuresChunkedArrayOpt.foreach { featuresChunkedArray =>
      rowAsDoubleArray.foreach { doubleVal =>
        featuresChunkedArray.add(doubleVal)
      }
    }
  }

  def addGroupColumn(rows: Array[Row], groupColumn: Option[String],
                     datasetPtr: Option[LightGBMDataset], numRows: Int,
                     schema: StructType, overrideIdx: Option[Int]): Unit = {
    validateGroupColumn(groupColumn, schema)
    groupColumn.foreach { col =>
      val datatype = schema.fields(schema.fieldIndex(col)).dataType
      val colIdx = if (overrideIdx.isEmpty) schema.fieldIndex(col) else overrideIdx.get

      // Convert to distinct count (note ranker should have sorted within partition by group id)
      // We use a triplet of a list of cardinalities, last unique value and unique value count
      val groupCardinality = datatype match {
        case org.apache.spark.sql.types.IntegerType => countCardinality(rows.map(row => row.getInt(colIdx)))
        case org.apache.spark.sql.types.LongType => countCardinality(rows.map(row => row.getLong(colIdx)))
        case org.apache.spark.sql.types.StringType => countCardinality(rows.map(row => row.getString(colIdx)))
      }

      datasetPtr.get.addIntField(groupCardinality, "group", groupCardinality.length)
    }
  }

  def addInitScoreColumn(rows: Array[Row], initScoreColumn: Option[String],
                         datasetPtr: Option[LightGBMDataset], numRows: Int, schema: StructType): Unit = {
    initScoreColumn.foreach { col =>
      val field = schema.fields(schema.fieldIndex(col))
      if (field.dataType == VectorType) {
        val initScores = rows.map(row => row.get(schema.fieldIndex(col)).asInstanceOf[DenseVector])
        // Calculate # rows * # classes in multiclass case
        val initScoresLength = initScores.length
        val totalLength = initScoresLength * initScores(0).size
        val flattenedInitScores = new Array[Double](totalLength)
        initScores.zipWithIndex.foreach { case (rowVector, rowIndex) =>
          rowVector.values.zipWithIndex.foreach { case (rowValue, colIndex) =>
            flattenedInitScores(colIndex * initScoresLength + rowIndex) = rowValue
          }
        }
        datasetPtr.get.addDoubleField(flattenedInitScores, "init_score", numRows)
      } else {
        val initScores = rows.map(row => row.getDouble(schema.fieldIndex(col)))
        datasetPtr.get.addDoubleField(initScores, "init_score", numRows)
      }
    }
  }

  def releaseArrays(labelsChunkedArray: floatChunkedArray, weightChunkedArrayOpt: Option[floatChunkedArray],
                    initScoreChunkedArrayOpt: Option[doubleChunkedArray]): Unit = {
    labelsChunkedArray.release()
    weightChunkedArrayOpt.foreach(_.release())
    initScoreChunkedArrayOpt.foreach(_.release())
  }

  def aggregateDenseStreamedData(rowsIter: Iterator[Row], columnParams: ColumnParams,
                                 referenceDataset: Option[LightGBMDataset], schema: StructType,
                                 log: Logger, trainParams: TrainParams): Option[LightGBMDataset] = {
    var numRows = 0
    val chunkSize = trainParams.executionParams.chunkSize
    val labelsChunkedArray = new floatChunkedArray(chunkSize)
    val weightChunkedArrayOpt = columnParams.weightColumn.map { _ => new floatChunkedArray(chunkSize) }
    val initScoreChunkedArrayOpt = columnParams.initScoreColumn.map { _ => new doubleChunkedArray(chunkSize) }
    var featuresChunkedArrayOpt: Option[doubleChunkedArray] = None
    val groupColumnValues: ListBuffer[Row] = new ListBuffer[Row]()
    try {
      var numCols = 0
      while (rowsIter.hasNext) {
        val row = rowsIter.next()
        numRows += 1
        labelsChunkedArray.add(row.getDouble(schema.fieldIndex(columnParams.labelColumn)).toFloat)
        columnParams.weightColumn.map { col =>
          weightChunkedArrayOpt.get.add(row.getDouble(schema.fieldIndex(col)).toFloat)
        }
        val rowAsDoubleArray = getRowAsDoubleArray(row, columnParams, schema)
        numCols = rowAsDoubleArray.length
        if (featuresChunkedArrayOpt.isEmpty) {
          featuresChunkedArrayOpt = Some(new doubleChunkedArray(numCols * chunkSize))
        }
        addFeaturesToChunkedArray(featuresChunkedArrayOpt, numCols, rowAsDoubleArray)
        addInitScoreColumnRow(initScoreChunkedArrayOpt, row, columnParams, schema)
        addGroupColumnRow(row, groupColumnValues, columnParams, schema)
      }

      val slotNames = getSlotNames(schema, columnParams.featuresColumn, numCols, trainParams)
      log.info(s"LightGBM task generating dense dataset with $numRows rows and $numCols columns")
      val datasetPtr = Some(LightGBMUtils.generateDenseDataset(numRows, numCols, featuresChunkedArrayOpt.get,
        referenceDataset, slotNames, trainParams, chunkSize))
      datasetPtr.get.addFloatField(labelsChunkedArray, "label", numRows)

      weightChunkedArrayOpt.foreach(datasetPtr.get.addFloatField(_, "weight", numRows))
      initScoreChunkedArrayOpt.foreach(datasetPtr.get.addDoubleField(_, "init_score", numRows))
      val overrideGroupIndex = Some(0)
      addGroupColumn(groupColumnValues.toArray, columnParams.groupColumn, datasetPtr, numRows, schema,
        overrideGroupIndex)
      datasetPtr
    } finally {
      releaseArrays(labelsChunkedArray, weightChunkedArrayOpt, initScoreChunkedArrayOpt)
    }
  }

  def validateGroupColumn(groupColumn: Option[String], schema: StructType): Unit = {
    groupColumn.foreach { col =>
      val datatype = schema.fields(schema.fieldIndex(col)).dataType

      if (datatype != org.apache.spark.sql.types.IntegerType
        && datatype != org.apache.spark.sql.types.LongType
        && datatype != org.apache.spark.sql.types.StringType) {
        throw new IllegalArgumentException(
          s"group column $col must be of type Long, Int or String but is ${datatype.typeName}")
      }
    }
  }

  def getSlotNames(schema: StructType, featuresColumn: String, numCols: Int,
                   trainParams: TrainParams): Option[Array[String]] = {
    if (trainParams.featureNames.nonEmpty) {
      Some(trainParams.featureNames)
    } else {
      val featuresSchema = schema.fields(schema.fieldIndex(featuresColumn))
      val metadata = AttributeGroup.fromStructField(featuresSchema)
      if (metadata.attributes.isEmpty) None
      else if (metadata.attributes.get.isEmpty) None
      else {
        val colnames = (0 until numCols).map(_.toString).toArray
        metadata.attributes.get.foreach {
          case attr =>
            attr.index.foreach(index => colnames(index) = attr.name.getOrElse(index.toString))
        }
        Some(colnames)
      }
    }
  }
}
