// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.schema

import com.microsoft.azure.synapse.ml.core.schema.SchemaConstants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

/** Schema modification and information retrieval methods. */
object SparkSchema {

  /** Sets the label column name.
    *
    * @param dataset    The dataset to set the label column name on.
    * @param modelName  The model name.
    * @param columnName The column name to set as the label.
    * @param scoreValueKindModel The model type.
    * @return The modified dataset.
    */
  def setLabelColumnName: (DataFrame, String, String, String) => DataFrame =
    setColumnName(TrueLabelsColumn)

  /** Gets the label column name.
    *
    * @param dataset   The dataset to get the label column from.
    * @param modelName The model to retrieve the label column from.
    * @return The label column name.
    */
  def getLabelColumnName(dataset: DataFrame, modelName: String): String =
    getScoreColumnKindColumn(TrueLabelsColumn)(dataset.schema, modelName)

  /** Gets the spark prediction column name.
    *
    * @param dataset   The dataset to get the spark prediction column from.
    * @param modelName The model to retrieve the spark prediction column from.
    * @return The spark prediction column name.
    */
  def getSparkPredictionColumnName(dataset: DataFrame, modelName: String): String =
    getScoreColumnKindColumn(SparkPredictionColumn)(dataset.schema, modelName)

  /** Gets the spark raw prediction column name.
    *
    * @param dataset   The dataset to get the spark raw prediction column from.
    * @param modelName The model to retrieve the spark raw prediction column from.
    * @return The spark raw prediction column name.
    */
  def getSparkRawPredictionColumnName(dataset: DataFrame, modelName: String): String =
    getScoreColumnKindColumn(SparkRawPredictionColumn)(dataset.schema, modelName)

  /** Gets the spark probability column name.
    *
    * @param dataset   The dataset to get the spark probability column from.
    * @param modelName The model to retrieve the spark probability column from.
    * @return The spark probability column name.
    */
  def getSparkProbabilityColumnName(dataset: DataFrame, modelName: String): String =
    getScoreColumnKindColumn(SparkProbabilityColumn)(dataset.schema, modelName)

  /** Gets the label column name.
    *
    * @param dataset   The dataset to get the label column from.
    * @param modelName The model to retrieve the label column from.
    * @return The label column name.
    */
  def getLabelColumnName: (StructType, String) => String =
    getScoreColumnKindColumn(TrueLabelsColumn)

  /** Gets the spark prediction column name.
    *
    * @param dataset   The dataset to get the spark prediction column from.
    * @param modelName The model to retrieve the spark prediction column from.
    * @return The spark prediction column name.
    */
  def getSparkPredictionColumnName: (StructType, String) => String =
    getScoreColumnKindColumn(SparkPredictionColumn)

  /** Gets the spark raw prediction column name.
    *
    * @param dataset   The dataset to get the spark raw prediction column from.
    * @param modelName The model to retrieve the spark raw prediction column from.
    * @return The spark raw prediction column name.
    */
  def getSparkRawPredictionColumnName: (StructType, String) => String =
    getScoreColumnKindColumn(SparkRawPredictionColumn)

  /** Gets the spark probability column name.
    *
    * @param dataset   The dataset to get the spark probability column from.
    * @param modelName The model to retrieve the spark probability column from.
    * @return The spark probability column name.
    */
  def getSparkProbabilityColumnName: (StructType, String) => String =
    getScoreColumnKindColumn(SparkProbabilityColumn)

  /** Gets the score value kind or null if it does not exist from a dataset.
    *
    * @param dataset   The dataset to get the score column kind column name from.
    * @param modelName The model to retrieve the score column kind column name from.
    * @param columnName The column to retrieve the score value kind from.
    * @return
    */
  def getScoreValueKind(dataset: DataFrame, modelName: String, columnName: String): String = {
    getScoreValueKind(dataset.schema, modelName, columnName)
  }

  /** Gets the score value kind or null if it does not exist from the schema.
    *
    * @param schema   The schema to get the score column kind column name from.
    * @param modelName The model to retrieve the score column kind column name from.
    * @param columnName The column to retrieve the score value kind from.
    * @return
    */
  def getScoreValueKind(schema: StructType, modelName: String, columnName: String): String = {
    val metadata = schema(columnName).metadata
    if (metadata == null) null  //scalastyle:ignore null
    else getMetadataFromModule(metadata, modelName, ScoreValueKind)
  }

  /** Sets the score column kind.
    *
    * @param scoreColumnKindColumn The score column kind column.
    * @param dataset               The dataset to set the score column kind on.
    * @param modelName             The model name.
    * @param columnName            The column name to set as the specified score column kind.
    * @param scoreValueKindModel   The model type.
    * @return
    */
  private def setColumnName(scoreColumnKindColumn: String)
                           (dataset: DataFrame, modelName: String,
                            columnName: String, scoreValueKindModel: String): DataFrame = {
    dataset.withColumn(columnName,
      dataset.col(columnName).as(columnName,
        updateMetadata(dataset.schema(columnName).metadata,
          scoreColumnKindColumn, scoreValueKindModel, modelName)))
  }

  /** Sets the column kind without renaming column name.
    *
    * @param dataset               The dataset to set the column kind on.
    * @param modelName             The model name.
    * @param columnName            The column name to set as the specified column kind.
    * @param modelKind             The model type.
    * @return
    */
  def updateColumnMetadata(dataset: DataFrame, modelName: String,
                           columnName: String, modelKind: String): DataFrame =
    setColumnName(columnName)(dataset, modelName, columnName, modelKind)

  /** Gets the score column kind column name or null if it does not exist.
    *
    * @param scoreColumnKindColumn The score column kind to retrieve.
    * @param schema   The schema to get the score column kind column name from.
    * @param modelName The model to retrieve the score column kind column name from.
    * @return
    */
  private def getScoreColumnKindColumn(scoreColumnKindColumn: String)
                                      (schema: StructType, modelName: String): String = {
    val structField = schema.find {
      case StructField(_, _, _, metadata) =>
        getMetadataFromModule(metadata, modelName, ScoreColumnKind) == scoreColumnKindColumn
    }
    if (structField.isEmpty) null else structField.get.name  //scalastyle:ignore null
  }

  private def updateMetadata(metadata: Metadata, scoreColumnKindColumn: String,
                             scoreValueKindModel: String, moduleName: String): Metadata = {
    val mmltagMetadata =
      if (metadata.contains(MMLTag)) metadata.getMetadata(MMLTag)
      else null  //scalastyle:ignore null
    val moduleNameMetadata =
      if (mmltagMetadata != null && mmltagMetadata.contains(moduleName))
        mmltagMetadata.getMetadata(moduleName)
      else null  //scalastyle:ignore null

    val moduleMetadataBuilder = new MetadataBuilder()
    if (mmltagMetadata != null && moduleNameMetadata != null) {  //scalastyle:ignore null
      moduleMetadataBuilder.withMetadata(moduleNameMetadata)
    }
    moduleMetadataBuilder.putString(ScoreColumnKind, scoreColumnKindColumn)
    moduleMetadataBuilder.putString(ScoreValueKind, scoreValueKindModel)

    val moduleBuilder = new MetadataBuilder()
    if (mmltagMetadata != null) {
      moduleBuilder.withMetadata(mmltagMetadata)
    }
    moduleBuilder.putMetadata(moduleName, moduleMetadataBuilder.build())

    new MetadataBuilder()
      .withMetadata(metadata)
      .putMetadata(MMLTag, moduleBuilder.build())
      .build()
  }

  private def getMetadataFromModule(colMetadata: Metadata, moduleName: String, tag: String): String = {
    if (!colMetadata.contains(MMLTag)) null  //scalastyle:ignore null
    else {
      val mlTagMetadata = colMetadata.getMetadata(MMLTag)
      if (!mlTagMetadata.contains(moduleName)) null  //scalastyle:ignore null
      else {
        val modelMetadata = mlTagMetadata.getMetadata(moduleName)
        if (!modelMetadata.contains(tag)) null  //scalastyle:ignore null
        else modelMetadata.getString(tag)
      }
    }
  }

  /** Find if the given column is a string */
  def isString(df: DataFrame, column: String): Boolean = {
    df.schema(column).dataType == DataTypes.StringType
  }

  /** Find if the given column is numeric */
  def isNumeric(df: DataFrame, column: String): Boolean = {
    df.schema(column).dataType.isInstanceOf[NumericType]
  }

  /** Find if the given column is boolean */
  def isBoolean(df: DataFrame, column: String): Boolean = {
    df.schema(column).dataType.isInstanceOf[BooleanType]
  }

  /** Find if the given column is Categorical; use CategoricalColumnInfo for more details */
  def isCategorical(df: DataFrame, column: String): Boolean = {
    val info = new CategoricalColumnInfo(df, column)
    info.isCategorical
  }

}
