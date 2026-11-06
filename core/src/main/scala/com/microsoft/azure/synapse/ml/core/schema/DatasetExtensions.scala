// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.schema

import org.apache.spark.ml.linalg.{DenseVector, SparseVector}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

/** Contains methods for manipulating spark dataframes and datasets. */
object DatasetExtensions {

  implicit class MMLDataFrame(val df: Dataset[_]) extends AnyVal {
    /** Finds an unused column name given initial column name in the given schema.
      * The unused column name will be given prefix with a number appended to it, eg "testColumn_5".
      * There will be an underscore between the column name and the number appended.
      *
      * @return The unused column name.
      */
    def withDerivativeCol(prefix: String): String = {
      findUnusedColumnName(prefix, df)
    }

    /** Gets the column values as the given type.
      * @param colName The column name to retrieve from.
      * @tparam T The type to retrieve.
      * @return The sequence of values in the column.
      */
    def getColAs[T](colName: String): Seq[T] = {
      df.select(colName).collect.map(_.getAs[T](0))
    }

    /** Gets the spark sparse vector column.
      * @return The spark sparse vector column.
      */
    def getSVCol: String => Seq[SparseVector] = getColAs[SparseVector]

    /** Gets the spark dense vector column.
      * @return The spark dense vector column.
      */
    def getDVCol: String => Seq[DenseVector] = getColAs[DenseVector]
  }

  /** Finds an unused column name given initial column name and a list of existing column names.
    * The unused column name will be given prefix with a number appended to it, eg "testColumn_5".
    * There will be an underline between the column name and the number appended.
    *
    * @return The unused column name.
    */
  def findUnusedColumnName(prefix: String)(columnNames: scala.collection.Set[String]): String = {
    findUnusedColumnName(prefix, columnNames, SQLConf.get.resolver)
  }

  def findUnusedColumnName(prefix: String,
                           columnNames: scala.collection.Set[String],
                           resolver: (String, String) => Boolean): String = {
    val stream = Iterator(prefix) ++ Iterator.from(1, 1).map(prefix + "_" + _)
    stream.dropWhile(candidate => columnNames.exists(resolver(_, candidate))).next()
  }

  def findUnusedColumnName(prefix: String, schema: StructType): String = {
    findUnusedColumnName(prefix)(schema.fieldNames.toSet)
  }

  def resolverFor(df: Dataset[_]): (String, String) => Boolean = {
    if (df.sparkSession.conf.get("spark.sql.caseSensitive", "false").toBoolean) {
      (left: String, right: String) => left == right
    } else {
      (left: String, right: String) => left.equalsIgnoreCase(right)
    }
  }

  def findUnusedColumnName(prefix: String, df: Dataset[_]): String = {
    findUnusedColumnName(prefix, df.columns.toSet, resolverFor(df))
  }
}
