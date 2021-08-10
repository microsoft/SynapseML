// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.param.{MapParam, Param, Params, StringStringMapParam}
import spray.json.DefaultJsonProtocol._

trait HasFeedFetchDicts extends Params {
  val feedDict: StringStringMapParam = new StringStringMapParam(
    this,
    "feedDict",
    " Provide a map from CNTK/ONNX model input variable names (keys) to column names of the input dataframe (values)"
  )

  def setFeedDict(value: Map[String, String]): this.type = set(feedDict, value)

  def setFeedDict(k: String, v: String): this.type = set(feedDict, Map(k -> v))

  def getFeedDict: Map[String, String] = $(feedDict)

  val fetchDict: StringStringMapParam = new StringStringMapParam(
    this,
    "fetchDict",
    "Provide a map from column names of the output dataframe (keys) to CNTK/ONNX model output variable names (values)"
  )

  def setFetchDict(value: Map[String, String]): this.type = set("fetchDict", value)

  def setFetchDict(k: String, v: String): this.type = set(fetchDict, Map(k -> v))

  def getFetchDict: Map[String, String] = $(fetchDict)
}
