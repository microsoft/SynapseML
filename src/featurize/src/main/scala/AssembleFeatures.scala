// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.io._

import com.microsoft.ml.spark.schema.{CategoricalColumnInfo, DatasetExtensions}
import com.microsoft.ml.spark.schema.DatasetExtensions._
import org.apache.hadoop.fs.Path
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.feature._
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, _}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.{BitSet, HashSet}

private object AssembleFeaturesUtilities
{
  private val tokenizedColumnName = "tokenizedFeatures"
  private val hashedFeaturesColumnName = "hashedFeatures"
  private val selectedFeaturesColumnName = "selectedFeatures"

  def getTokenizedColumnName(dataset:DataFrame): String = {
    dataset.withDerivativeCol(tokenizedColumnName)
  }

  def getHashedFeaturesColumnName(dataset:DataFrame): String = {
    dataset.withDerivativeCol(hashedFeaturesColumnName)
  }

  def getSelectedFeaturesColumnName(dataset:DataFrame): String = {
    dataset.withDerivativeCol(selectedFeaturesColumnName)
  }

  def hashStringColumns(nonMissingDataset: DataFrame, colNamesToHash: ListBuffer[String],
                        hashingTransform: HashingTF): DataFrame = {
    val tokenizeFunc = udf((cols: Seq[String]) => cols
      .filter(str => str != null && !str.isEmpty)
      .flatMap(str => str.toLowerCase.split("\\s")))
    val cols = array(colNamesToHash.map(x => col(x)): _*)
    val combinedData = nonMissingDataset.withColumn(hashingTransform.getInputCol, tokenizeFunc(cols))
    hashingTransform.transform(combinedData)
  }

  def isNumeric(dataType: DataType): Boolean = dataType == IntegerType ||
    dataType == BooleanType ||
    dataType == LongType ||
    dataType == ByteType ||
    dataType == ShortType ||
    dataType == FloatType
}

/**
  * Class containing the list of column names to perform special featurization steps for.
  * colNamesToHash - List of column names to hash.
  * colNamesToDuplicateForMissings - List of column names containing doubles to duplicate
  *                                   so we can remove missing values from them.
  * colNamesToTypes - Map of column names to their types.
  * colNamesToCleanMissings - List of column names to clean missing values from (ignore).
  * colNamesToVectorize - List of column names to vectorize using FastVectorAssembler.
  * categoricalColumns - List of categorical columns to pass through or turn into indicator array.
  * conversionColumnNamesMap - Map from old column names to new.
  */
@SerialVersionUID(0L)
class ColumnNamesToFeaturize extends Serializable {
  val colNamesToHash                  = ListBuffer[String]()
  val colNamesToDuplicateForMissings  = ListBuffer[String]()
  val colNamesToTypes                 = mutable.Map[String, DataType]()
  val vectorColumnsToAdd              = ListBuffer[String]()
  val colNamesToCleanMissings         = ListBuffer[String]()
  val colNamesToVectorize             = ListBuffer[String]()
  val categoricalColumns              = mutable.Map[String, String]()
  val conversionColumnNamesMap        = mutable.Map[String, String]()
}

object AssembleFeatures extends DefaultParamsReadable[AssembleFeatures]

/**
  * Creates a vector column of features from a collection of feature columns
  * @param uid
  */
class AssembleFeatures(override val uid: String) extends Estimator[AssembleFeaturesModel]
  with HasFeaturesCol with MMLParams {

  def this() = this(Identifiable.randomUID("AssembleFeatures"))

  setDefault(featuresCol -> "features")

  /** Columns to featurize
    * @group param
    */
  val columnsToFeaturize: StringArrayParam =
    new StringArrayParam(this, "columnsToFeaturize", "columns to featurize", array => true)

  /** @group getParam */
  final def getColumnsToFeaturize: Array[String] = $(columnsToFeaturize)

  /** @group setParam */
  def setColumnsToFeaturize(value: Array[String]): this.type = set(columnsToFeaturize, value)

  /**
    * Categorical columns are one-hot encoded when true; default is true
    * @group param
    */
  val oneHotEncodeCategoricals: Param[Boolean] = BooleanParam(this,
    "oneHotEncodeCategoricals",
    "one hot encode categoricals",
    true)

  /** @group getParam */
  final def getOneHotEncodeCategoricals: Boolean = $(oneHotEncodeCategoricals)

  /** @group setParam */
  def setOneHotEncodeCategoricals(value: Boolean): this.type = set(oneHotEncodeCategoricals, value)

  /**
    * Number of features to has string columns tos
    * @group param
    */
  val numberOfFeatures: IntParam =
    IntParam(this, "numberOfFeatures", "number of features to hash string columns to")

  /** @group getParam */
  final def getNumberOfFeatures: Int = $(numberOfFeatures)

  /** @group setParam */
  def setNumberOfFeatures(value: Int): this.type = set(numberOfFeatures, value)

  /**
    * Creates a vector column of features from a collection of feature columns
    *
    * @param dataset The input dataset to fit.
    * @return The model that will return the original dataset with assembled features as a vector.
    */
  override def fit(dataset: Dataset[_]): AssembleFeaturesModel = {
    val columnNamesToFeaturize = new ColumnNamesToFeaturize

    val columnsToFeaturize = HashSet[String](getColumnsToFeaturize: _*)

    val columns = dataset.columns

    val allIntermediateCols = new mutable.HashSet[String]()
    allIntermediateCols ++= columns

    val datasetAsDf = dataset.toDF()

    // Remap and convert columns prior to training
    columns.foreach {
      col => if (columnsToFeaturize.contains(col)) {
        val unusedColumnName = DatasetExtensions.findUnusedColumnName(col)(allIntermediateCols)
        allIntermediateCols += unusedColumnName

        // Find out if column is categorical
        // If using non-tree learner, one-hot encode them
        // Otherwise, pass attributes directly to train classifier,
        // but move categoricals to beginning for superior
        // runtime and to avoid spark bug
        val categoricalInfo = new CategoricalColumnInfo(datasetAsDf, col)
        val isCategorical = categoricalInfo.isCategorical
        if (isCategorical) {
          val oheColumnName = DatasetExtensions.findUnusedColumnName("TmpOHE_" + unusedColumnName)(allIntermediateCols)
          columnNamesToFeaturize.categoricalColumns += unusedColumnName -> oheColumnName
        }

        dataset.schema(col).dataType match {
          case _ @ (dataType: DataType) if dataType == DoubleType
            || dataType == FloatType => {
            columnNamesToFeaturize.colNamesToTypes += unusedColumnName -> dataType
            // For double and float columns, will always need to remove possibly NaN values
            columnNamesToFeaturize.colNamesToCleanMissings += unusedColumnName
            columnNamesToFeaturize.conversionColumnNamesMap += col -> unusedColumnName
          }
          case _ @ (dataType: DataType) if (AssembleFeaturesUtilities.isNumeric(dataType)) => {
            // Convert all numeric columns to same type double to feed them as a vector to the learner
            if (dataset.schema(col).nullable) {
              columnNamesToFeaturize.colNamesToCleanMissings += unusedColumnName
            }
            columnNamesToFeaturize.colNamesToTypes += unusedColumnName -> dataType
            columnNamesToFeaturize.conversionColumnNamesMap += col -> unusedColumnName
          }
          case _: StringType => {
            // Hash string columns
            columnNamesToFeaturize.colNamesToHash += col
            columnNamesToFeaturize.colNamesToTypes += col -> StringType
          }
          case _ @ (dataType: DataType) if dataType.typeName == "vector" || dataType.isInstanceOf[VectorUDT] => {
            columnNamesToFeaturize.vectorColumnsToAdd += unusedColumnName
            // For double columns, will always need to remove possibly NaN values
            columnNamesToFeaturize.colNamesToCleanMissings += unusedColumnName
            columnNamesToFeaturize.colNamesToTypes += unusedColumnName -> dataType
            columnNamesToFeaturize.conversionColumnNamesMap += col -> unusedColumnName
          }
        }
      }
    }
    val colNamesToVectorizeWithoutHashOneHot: List[String] = getColumnsToVectorize(columnNamesToFeaturize,
      columnNamesToFeaturize.conversionColumnNamesMap.keys.toSeq)

    // Tokenize the string columns
    val (transform: Option[HashingTF], colNamesToVectorize: List[String], nonZeroColumns: Option[Array[Int]]) =
      if (columnNamesToFeaturize.colNamesToHash.isEmpty)
        (None, colNamesToVectorizeWithoutHashOneHot, None)
      else {
        val hashingTransform = new HashingTF()
          .setInputCol(AssembleFeaturesUtilities.getTokenizedColumnName(datasetAsDf))
          .setOutputCol(AssembleFeaturesUtilities.getHashedFeaturesColumnName(datasetAsDf))
          .setNumFeatures(getNumberOfFeatures)

        // Hash data for the vectorizer, to determine which slots are non-zero and should be kept
        val hashedData = AssembleFeaturesUtilities.hashStringColumns(datasetAsDf,
          columnNamesToFeaturize.colNamesToHash,
          hashingTransform)
        val encoder = Encoders.kryo[BitSet]
        val bitset = hashedData.select(hashingTransform.getOutputCol)
          .map(row => toBitSet(row.getAs[SparseVector](0).indices))(encoder)
          .reduce(_ | _)

        val nonZeroColumns: Array[Int] = bitset.toArray

        val colsToVectorize =
          colNamesToVectorizeWithoutHashOneHot :+ AssembleFeaturesUtilities.getSelectedFeaturesColumnName(datasetAsDf)

        (Some(hashingTransform),
          colsToVectorize,
          Some(nonZeroColumns))
      }

    columnNamesToFeaturize.colNamesToVectorize ++= colNamesToVectorize

    val vectorAssembler = new FastVectorAssembler()
      .setInputCols(colNamesToVectorize.toArray)
      .setOutputCol(getFeaturesCol)

    new AssembleFeaturesModel(uid, columnNamesToFeaturize, transform,
      nonZeroColumns, vectorAssembler, $(oneHotEncodeCategoricals))
  }

  private def getColumnsToVectorize(columnNamesToFeaturize: ColumnNamesToFeaturize,
                                    columnsToFeaturize: Seq[String]): List[String] = {
    val categoricalColumnNames =
      if ($(oneHotEncodeCategoricals)) {
        columnNamesToFeaturize.categoricalColumns.values
      } else {
        columnNamesToFeaturize.categoricalColumns.keys
      }

    val newColumnNames =
      columnsToFeaturize.map(oldColName => columnNamesToFeaturize.conversionColumnNamesMap(oldColName))

    val colNamesToVectorizeWithoutHash = (categoricalColumnNames.toList
      ::: newColumnNames.toList)
      .distinct

    // If one hot encoding, remove the columns we are converting from the list to vectorize
    val colNamesToVectorizeWithoutHashOneHot =
      if ($(oneHotEncodeCategoricals)) {
        colNamesToVectorizeWithoutHash.filter {
          !columnNamesToFeaturize.categoricalColumns.contains(_)
        }
      } else {
        colNamesToVectorizeWithoutHash
      }
    colNamesToVectorizeWithoutHashOneHot
  }

  def toBitSet(indices: Array[Int]): BitSet = {
    indices.foldLeft(BitSet())((bitset, index) => bitset + index)
  }

  override def copy(extra: ParamMap): Estimator[AssembleFeaturesModel] = {
    new AssembleFeatures()
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType =
    schema.add(new StructField(getFeaturesCol, VectorType))

}

/**
  * Model produced by [[AssembleFeatures]].
  */
class AssembleFeaturesModel(val uid: String,
                     val columnNamesToFeaturize: ColumnNamesToFeaturize,
                     val hashingTransform: Option[HashingTF],
                     val nonZeroColumns: Option[Array[Int]],
                     val vectorAssembler: FastVectorAssembler,
                     val oneHotEncodeCategoricals: Boolean)
  extends Model[AssembleFeaturesModel] with Params with MLWritable {

  /** @group getParam */
  final def getFeaturesColumn: String = vectorAssembler.getOutputCol

  override def write: MLWriter = new AssembleFeaturesModel.AssembleFeaturesModelWriter(uid,
    columnNamesToFeaturize,
    hashingTransform,
    nonZeroColumns,
    vectorAssembler,
    oneHotEncodeCategoricals)

  override def copy(extra: ParamMap): AssembleFeaturesModel =
    new AssembleFeaturesModel(uid,
      columnNamesToFeaturize,
      hashingTransform,
      nonZeroColumns,
      vectorAssembler,
      oneHotEncodeCategoricals)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val transformedDataset = dataset.select(
      dataset.columns.flatMap {
        col => {
          val dataType = dataset.schema(col).dataType
          if (!dataType.isInstanceOf[StringType]
            && columnNamesToFeaturize.colNamesToHash.contains(col)) {
            throw new Exception("Invalid column type specified during score, should be string for column: " + col)
          }

          if (!columnNamesToFeaturize.conversionColumnNamesMap.contains(col)) {
            Seq(dataset(col))
          } else {
            val colType = columnNamesToFeaturize.colNamesToTypes(columnNamesToFeaturize.conversionColumnNamesMap(col))
            if (colType != dataType) {
              throw new Exception(s"Invalid column type specified during score, should be $colType for column: " + col)
            }

            // Convert all columns to same type double to feed them as a vector to the learner
            dataType match {
              case _ @ (dataType: DataType) if (AssembleFeaturesUtilities.isNumeric(dataType)) => {
                Seq(dataset(col),
                  dataset(col).cast(DoubleType).as(columnNamesToFeaturize.conversionColumnNamesMap(col),
                    dataset.schema(col).metadata))
              }
              case _: DoubleType => {
                Seq(dataset(col),
                  dataset(col).as(columnNamesToFeaturize.conversionColumnNamesMap(col),
                    dataset.schema(col).metadata))
              }
              case _ @ (dataType: DataType) if dataType.typeName == "vector" || dataType.isInstanceOf[VectorUDT] => {
                Seq(dataset(col),
                  dataset(col).as(columnNamesToFeaturize.conversionColumnNamesMap(col),
                    dataset.schema(col).metadata))
              }
              case default => Seq(dataset(col))
            }
          }
        }
      }: _*
    )

    // Drop all rows with missing values
    val nonMissingDataset = transformedDataset.na.drop(columnNamesToFeaturize.colNamesToCleanMissings)
    // Tokenize the string columns
    val stringFeaturizedData: DataFrame =
      if (columnNamesToFeaturize.colNamesToHash.isEmpty) nonMissingDataset
      else {
        val hashedData = AssembleFeaturesUtilities.hashStringColumns(nonMissingDataset,
          columnNamesToFeaturize.colNamesToHash,
          hashingTransform.get)

        val vectorSlicer = new VectorSlicer().setIndices(nonZeroColumns.get)
          .setInputCol(hashingTransform.get.getOutputCol)
          .setOutputCol(columnNamesToFeaturize.colNamesToVectorize.last)
        // Run count based feature selection on the hashed data
        val countBasedFeatureSelectedColumns = vectorSlicer.transform(hashedData)
        // Remove the intermediate columns tokenized and hashed
        countBasedFeatureSelectedColumns
          .drop(hashingTransform.get.getInputCol)
          .drop(hashingTransform.get.getOutputCol)
      }
    var columnsToDrop = vectorAssembler.getInputCols
    // One-hot encode categoricals
    val oheData =
      if (oneHotEncodeCategoricals && !columnNamesToFeaturize.categoricalColumns.isEmpty) {
        val ohe = new OneHotEncoder()
        val inputColsKeys = columnNamesToFeaturize.categoricalColumns.keys
        val outputColsKeys = columnNamesToFeaturize.categoricalColumns.values
        val inputCols = inputColsKeys.mkString(",")
        val outputCols = outputColsKeys.mkString(",")
        val oheAdapter =
          new MultiColumnAdapter().setBaseTransformer(ohe).setInputCols(inputCols).setOutputCols(outputCols)
        columnsToDrop = columnsToDrop.union(columnNamesToFeaturize.categoricalColumns.keys.toSeq)
        oheAdapter.transform(stringFeaturizedData)
      } else {
        stringFeaturizedData
      }

    val vectorizedData = vectorAssembler.transform(oheData)

    // Drop the vector assembler intermediate columns
    vectorizedData.drop(columnsToDrop: _*)
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType =
    schema.add(new StructField(getFeaturesColumn, VectorType))

}

object AssembleFeaturesModel extends MLReadable[AssembleFeaturesModel] {

  private val hashingTransformPart = "hashingTransform"
  private val vectorAssemblerPart = "vectorAssembler"
  private val columnNamesToFeaturizePart = "columnNamesToFeaturize"
  private val nonZeroColumnsPart = "nonZeroColumns"
  private val dataPart = "data"

  override def read: MLReader[AssembleFeaturesModel] = new AssembleFeaturesModelReader

  override def load(path: String): AssembleFeaturesModel = super.load(path)

  /** [[MLWriter]] instance for [[AssembleFeaturesModel]] */
  private[AssembleFeaturesModel]
  class AssembleFeaturesModelWriter(val uid: String,
                             val columnNamesToFeaturize: ColumnNamesToFeaturize,
                             val hashingTransform: Option[HashingTF],
                             val nonZeroColumns: Option[Array[Int]],
                             val vectorAssembler: FastVectorAssembler,
                             val oneHotEncodeCategoricals: Boolean)
    extends MLWriter {
    private case class Data(uid: String, oneHotEncodeCategoricals: Boolean)

    override protected def saveImpl(path: String): Unit = {
      val overwrite = this.shouldOverwrite
      val qualPath = PipelineUtilities.makeQualifiedPath(sc, path)
      // Required in order to allow this to be part of an ML pipeline
      PipelineUtilities.saveMetadata(uid,
        AssembleFeaturesModel.getClass.getName.replace("$", ""),
        new Path(path, "metadata").toString,
        sc,
        overwrite)

      val dataPath = new Path(qualPath, dataPart).toString

      // Save data
      val data = Data(uid, oneHotEncodeCategoricals)
      // save the hashing transform
      if (!hashingTransform.isEmpty) {
        val hashingTransformPath = new Path(qualPath, hashingTransformPart).toString
        val writer =
          if (overwrite) hashingTransform.get.write.overwrite()
          else hashingTransform.get.write
        writer.save(hashingTransformPath)
      }
      // save the vector assembler
      val vectorAssemblerPath = new Path(qualPath, vectorAssemblerPart).toString
      val writer =
        if (overwrite) vectorAssembler.write.overwrite()
        else vectorAssembler.write
      writer.save(vectorAssemblerPath)

      // save the column names to featurize
      ObjectUtilities.writeObject(columnNamesToFeaturize, qualPath, columnNamesToFeaturizePart, sc, overwrite)

      // save the nonzero columns
      ObjectUtilities.writeObject(nonZeroColumns, qualPath, nonZeroColumnsPart, sc, overwrite)

      val saveMode =
        if (overwrite) SaveMode.Overwrite
        else SaveMode.ErrorIfExists
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.mode(saveMode).parquet(dataPath)
    }
  }

  private class AssembleFeaturesModelReader
    extends MLReader[AssembleFeaturesModel] {
    override def load(path: String): AssembleFeaturesModel = {
      val qualPath = PipelineUtilities.makeQualifiedPath(sc, path)
      // load the uid and one hot encoding param
      val dataPath = new Path(qualPath, dataPart).toString
      val data = sparkSession.read.format("parquet").load(dataPath)
      val Row(uid: String, oneHotEncodeCategoricals: Boolean) =
        data.select("uid", "oneHotEncodeCategoricals").head()

      // load the hashing transform
      val hashingPath = new Path(qualPath, hashingTransformPart).toString
      val hashingTransform =
        if (new File(hashingPath).exists()) Some(HashingTF.load(hashingPath))
        else None

      // load the vector assembler
      val vectorAssemblerPath = new Path(qualPath, vectorAssemblerPart).toString
      val vectorAssembler = FastVectorAssembler.load(vectorAssemblerPath)

      // load the column names to featurize
      val columnNamesToFeaturize =
        ObjectUtilities.loadObject[ColumnNamesToFeaturize](qualPath, columnNamesToFeaturizePart, sc)

      // load the nonzero columns
      val nonZeroColumns = ObjectUtilities.loadObject[Option[Array[Int]]](qualPath, nonZeroColumnsPart, sc)

      new AssembleFeaturesModel(uid,
        columnNamesToFeaturize,
        hashingTransform,
        nonZeroColumns,
        vectorAssembler,
        oneHotEncodeCategoricals)
    }
  }

}
