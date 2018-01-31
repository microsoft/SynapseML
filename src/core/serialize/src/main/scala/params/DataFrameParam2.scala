// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.param

import org.apache.spark.sql.{DataFrame, SparkSession}

/** Param for DataFrame.  Needed as spark has explicit params for many different
  * types but not DataFrame.
  */
class DataFrameParam2(parent: Params, name: String, doc: String, isValid: DataFrame => Boolean)
  extends ComplexParam[DataFrame](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  override def jsonEncode(value: DataFrame): String = {
    value.toJSON.collect.mkString("[", ",", "]")
  }

  override def jsonDecode(json: String): DataFrame = {
    val spark = SparkSession
      .builder()
      .getOrCreate()
    import spark.implicits._
    spark.read.json(Seq(json).toDS)
  }

}

object DataFrameParam2 {
  private lazy val emptydf: DataFrame = {
    lazy val spark = SparkSession.builder().master("local[*]").getOrCreate()
    lazy val df = spark.sqlContext.emptyDataFrame
    df
  }

  def getEmptyDF(): DataFrame = emptydf
}
