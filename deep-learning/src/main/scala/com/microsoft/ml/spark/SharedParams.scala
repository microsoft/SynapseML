// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import com.microsoft.CNTK.DataType
import com.microsoft.ml.spark.core.schema.DatasetExtensions.findUnusedColumnName
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.param.{BooleanParam, MapParam, Params}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types._
import spray.json.DefaultJsonProtocol._
import org.apache.spark.ml.linalg.{Vector => SVector}

trait HasFeedFetchMaps extends Params {
  val feedDict: MapParam[String, String] = new MapParam[String, String](
    this,
    "feedDict",
    " Provide a map from CNTK/ONNX model input variable names (keys) to column names of the input dataframe (values)"
  )

  def setFeedDict(value: Map[String, String]): this.type = set(feedDict, value)

  def setFeedDict(k: String, v: String): this.type = set(feedDict, Map(k -> v))

  def getFeedDict: Map[String, String] = $(feedDict)

  val fetchDict: MapParam[String, String] = new MapParam[String, String](
    this,
    "fetchDict",
    "Provide a map from column names of the output dataframe (keys) to CNTK/ONNX model output variable names (values)"
  )

  def setFetchDict(value: Map[String, String]): this.type = set("fetchDict", value)

  def setFetchDict(k: String, v: String): this.type = set(fetchDict, Map(k -> v))

  def getFetchDict: Map[String, String] = $(fetchDict)
}
