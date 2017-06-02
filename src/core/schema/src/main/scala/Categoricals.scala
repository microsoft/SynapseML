// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.schema

/**
  * Contains objects and functions to manipulate Categoricals
  */
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.ml.attribute._
import org.apache.spark.sql.functions.udf
import SchemaConstants._

import scala.reflect.ClassTag

object CategoricalUtilities {

  /**
    * Sets the given levels on the column.
    * @return The modified dataset.
    */
  def setLevels(dataset: DataFrame, column: String, levels: Array[_]): DataFrame = {
    if (levels == null) dataset
    else dataset.withColumn(column,
      dataset.col(column).as(column,
        updateLevelsMetadata(dataset.schema(column).metadata,
          levels,
          getCategoricalTypeForValue(levels.head))))
  }

  /**
    * Update the levels on the existing metadata.
    * @param existingMetadata The existing metadata to add to.
    * @param levels The levels to add to the metadata.
    * @param dataType The datatype of the levels.
    * @return The new metadata.
    */
  def updateLevelsMetadata(existingMetadata: Metadata, levels: Array[_], dataType: DataType): Metadata = {
    val bldr =
    if (existingMetadata.contains(MMLTag)) {
      new MetadataBuilder().withMetadata(existingMetadata.getMetadata(MMLTag))
    } else {
      new MetadataBuilder()
    }
    bldr.putBoolean(Ordinal, false)
    dataType match {
      case DataTypes.StringType  => bldr.putStringArray(ValuesString, levels.asInstanceOf[Array[String]])
      case DataTypes.DoubleType  => bldr.putDoubleArray(ValuesDouble, levels.asInstanceOf[Array[Double]])
      // Ints require special treatment, because Spark does not have putIntArray yet:
      case DataTypes.IntegerType => bldr.putLongArray(ValuesInt, levels.asInstanceOf[Array[Int]].map(_.toLong))
      case DataTypes.LongType    => bldr.putLongArray(ValuesLong, levels.asInstanceOf[Array[Long]])
      case DataTypes.BooleanType => bldr.putBooleanArray(ValuesBool, levels.asInstanceOf[Array[Boolean]])
      case _           => throw new UnsupportedOperationException("Unsupported categorical data type: " + dataType)
    }
    val metadata = bldr.build()

    new MetadataBuilder().withMetadata(existingMetadata).putMetadata(MMLTag, metadata).build()
  }

  /**
    * Gets the levels from the dataset.
    * @param schema The schema to get the levels from.
    * @param column The column to retrieve metadata levels from.
    * @return The levels.
    */
  def getLevels(schema: StructType, column: String): Option[Array[_]] = {
    val metadata = schema(column).metadata

    if (metadata.contains(MMLTag)) {
      val dataType: Option[DataType] = getDataType(metadata)
      if (dataType.isEmpty) None
      else {
        dataType.get match {
          case DataTypes.StringType => Some(getMap[String](metadata).levels)
          case DataTypes.LongType => Some(getMap[Long](metadata).levels)
          case DataTypes.IntegerType => Some(getMap[Int](metadata).levels)
          case DataTypes.DoubleType => Some(getMap[Double](metadata).levels)
          case DataTypes.BooleanType => Some(getMap[Boolean](metadata).levels)
          case default => throw new UnsupportedOperationException("Unknown categorical type: " + default.typeName)
        }
      }
    } else {
      None
    }
  }

  /**
    * Gets the number of levels from the dataset.
    * @param dataset The dataset to get the levels count from.
    * @param column The column to retrieve metadata levels count from.
    * @return The number of levels.
    */
  def getLevelCount(dataset: DataFrame, column: String): Option[Int] = {
    val metadata = dataset.schema(column).metadata

    if (metadata.contains(MMLTag)) {
      val dataType: Option[DataType] = getDataType(metadata)

      if (dataType.isEmpty) None
      else {
        val numLevels =
          dataType.get match {
            case DataTypes.StringType => getMap[String](metadata).numLevels
            case DataTypes.LongType => getMap[Long](metadata).numLevels
            case DataTypes.IntegerType => getMap[Int](metadata).numLevels
            case DataTypes.DoubleType => getMap[Double](metadata).numLevels
            case DataTypes.BooleanType => getMap[Boolean](metadata).numLevels
            case default => throw new UnsupportedOperationException("Unknown categorical type: " + default.typeName)
          }
        Option(numLevels)
      }
    } else {
      None
    }
  }

  /**
    * Get the map of array of T from the metadata.
    *
    * @param ct Implicit class tag.
    * @param metadata The metadata to retrieve from.
    * @tparam T The type of map to retrieve.
    * @return The map of array of T.
    */
  def getMap[T](metadata: Metadata)(implicit ct: ClassTag[T]): CategoricalMap[T] = {
    val data =
      if (metadata.contains(MMLTag)) {
        metadata.getMetadata(MMLTag)
      } else if (metadata.contains(MLlibTag)) {
        metadata.getMetadata(MLlibTag)
      } else {
        sys.error("Invalid metadata to retrieve map from")
      }

    val categoricalMap = implicitly[ClassTag[T]] match  {
      case ClassTag.Int => new CategoricalMap[Int](data.getLongArray(ValuesInt).map(_.toInt))
      case ClassTag.Double => new CategoricalMap[Double](data.getDoubleArray(ValuesDouble))
      case ClassTag.Boolean => new CategoricalMap[Boolean](data.getBooleanArray(ValuesBool))
      case ClassTag.Long => new CategoricalMap[Long](data.getLongArray(ValuesLong))
      case _ => new CategoricalMap[String](data.getStringArray(ValuesString))
    }
    categoricalMap.asInstanceOf[CategoricalMap[T]]
  }

  /**
    * Get a type for the given value.
    * @param value The value to get the type from.
    * @tparam T The generic type of the value.
    * @return The DataType based on the value.
    */
  def getCategoricalTypeForValue[T](value: T): DataType = {
    value match {
      // Complicated type matching is requred to get around type erasure
      case _: String  => DataTypes.StringType
      case _: Double  => DataTypes.DoubleType
      case _: Int     => DataTypes.IntegerType
      case _: Long    => DataTypes.LongType
      case _: Boolean => DataTypes.BooleanType
      case _          => throw new UnsupportedOperationException("Unsupported categorical data type")
    }
  }

  private def getDataType(metadata: Metadata): Option[DataType] = {
    val columnMetadata = metadata.getMetadata(MMLTag)
    val dataType =
      if (columnMetadata.contains(ValuesString)) Some(DataTypes.StringType)
      else if (columnMetadata.contains(ValuesLong)) Some(DataTypes.LongType)
      else if (columnMetadata.contains(ValuesInt)) Some(DataTypes.IntegerType)
      else if (columnMetadata.contains(ValuesLong)) Some(DataTypes.LongType)
      else if (columnMetadata.contains(ValuesDouble)) Some(DataTypes.DoubleType)
      else if (columnMetadata.contains(ValuesBool)) Some(DataTypes.BooleanType)
      else None
    dataType
  }

}

/**
  * A wrapper around level maps: Map[T -> Int] and Map[Int -> T] that converts
  *   the data to/from Spark Metadata in both MLib and AzreML formats.
  * @param levels  The level values are assumed to be already sorted as needed
  * @param isOrdinal  A flag that indicates if the data are ordinal
  * @tparam T  Input levels could be String, Double, Int, Long, Boolean
  */
class CategoricalMap[T](val levels: Array[T], val isOrdinal: Boolean = false) extends Serializable {
  //TODO: handle NULL values

  require(levels.distinct.size == levels.size, "Categorical levels are not unique.")
  require(!levels.isEmpty, "Levels should not be empty")

  /** total number of level */
  val numLevels = levels.length //TODO: add the maximum possible number of levels?

  /** Spark DataType correspondint to type T */
  val dataType = CategoricalUtilities.getCategoricalTypeForValue(levels.head)

  /** Maps levels to the corresponding integer index */
  private lazy val levelToIndex: Map[T, Int] = levels.zipWithIndex.toMap

  /** Returns the index of the given level, can throw */
  def getIndex(level: T): Int = levelToIndex(level)

  /** Returns the index of a given level as Option; does not throw */
  def getIndexOption(level: T): Option[Int] = levelToIndex.get(level)

  /** Checks if the given level exists */
  def hasLevel(level: T): Boolean = levelToIndex.contains(level)

  /** Returns the level of the given index; can throw */
  def getLevel(index: Int): T = levels(index)

  /** Returns the level of the given index as Option; does not throw */
  def getLevelOption(index: Int): Option[T] =
    if (index < 0 || index >= numLevels) None else Some(levels(index))

  /** Stores levels in Spark Metadata in either MLlib format */
  private def toMetadataMllib(existingMetadata: Metadata): Metadata = {
    require(!isOrdinal, "Cannot save Ordinal data in MLlib Nominal format currently," +
                        " because it does not have a public constructor that accepts Ordinal")

    // Currently, MLlib converts all non-string categorical values to string;
    // see org.apache.spark.ml.feature.StringIndexer
    val strLevels = levels.map(_.toString).asInstanceOf[Array[String]]

    NominalAttribute.defaultAttr.withValues(strLevels).toMetadata(existingMetadata)
  }

  /** Stores levels in Spark Metadata in MML format */
  private def toMetadataMML(existingMetadata: Metadata): Metadata = {
    CategoricalUtilities.updateLevelsMetadata(existingMetadata, levels, dataType)
  }

  /** Add categorical levels to existing Spark Metadata
    * @param existingMetadata [tag, categorical metadata] pair is added to existingMetadata,
    *   where tag is either MLlib or MML
    * @param mmlStyle MML (true) or MLlib metadata (false)
    */
  def toMetadata(existingMetadata: Metadata, mmlStyle: Boolean): Metadata = {

    // assert that metadata does not have data with this tag
    def assertNoTag(tag: String) =
      assert(!existingMetadata.contains(tag),
             //TODO: add tests to ensure
             s"Metadata already contains the tag $tag; all the data are eraised")

    if (mmlStyle) {
      assertNoTag(MMLTag)
      toMetadataMML(existingMetadata)
    } else {
      assertNoTag(MLlibTag)
      toMetadataMllib(existingMetadata)
    }
  }

  /** Add categorical levels and in either MML or MLlib style metadata
    * @param mmlStyle MML (true) or MLlib metadata (false)
    */
  def toMetadata(mmlStyle: Boolean): Metadata = toMetadata(Metadata.empty, mmlStyle)

}

/**
  * Extract categorical info from the DataFrame column
  * @param df dataframe
  * @param column column name
  */
class CategoricalColumnInfo(df: DataFrame, column: String) {

  private val columnSchema   = df.schema(column)
  private val metadata = columnSchema.metadata

  /** Get the basic info: whether the column is categorical or not, actual type of the column, etc */
  val (isCategorical, isMML, isOrdinal, dataType) = {

    val notCategorical = (false, false, false, NullType)

    if (columnSchema.dataType != DataTypes.IntegerType
      && columnSchema.dataType != DataTypes.DoubleType) notCategorical
    else if (metadata.contains(MMLTag)) {
      val columnMetadata = metadata.getMetadata(MMLTag)

      if (!columnMetadata.contains(Ordinal)) notCategorical
      else {
        val isOrdinal = columnMetadata.getBoolean(Ordinal)

        val dataType =
          if      (columnMetadata.contains(ValuesString)) DataTypes.StringType
          else if (columnMetadata.contains(ValuesLong))   DataTypes.LongType
          else if (columnMetadata.contains(ValuesInt))    DataTypes.IntegerType
          else if (columnMetadata.contains(ValuesLong))   DataTypes.LongType
          else if (columnMetadata.contains(ValuesDouble)) DataTypes.DoubleType
          else if (columnMetadata.contains(ValuesBool))   DataTypes.BooleanType
          else throw new Exception("Unrecognized datatype in MML metadata")

        (true, true, isOrdinal, dataType)
      }
    }
    else if (metadata.contains(MLlibTag)) {
      val columnMetadata = metadata.getMetadata(MLlibTag)
      // nominal metadata has ["type" -> "nominal"] pair
      val isCategorical = columnMetadata.contains(MLlibTypeTag) &&
                          columnMetadata.getString(MLlibTypeTag) == AttributeType.Nominal.name

      if (!isCategorical) notCategorical
      else {
        val isOrdinal = if (columnMetadata.contains(Ordinal)) columnMetadata.getBoolean(Ordinal) else false
        val dataType =
          if (columnMetadata.contains(ValuesString)) DataTypes.StringType
          else throw new UnsupportedOperationException("nominal attribute does not contain string levels")
        (true, false, isOrdinal, dataType)
      }
    } else
      notCategorical
  }

}
