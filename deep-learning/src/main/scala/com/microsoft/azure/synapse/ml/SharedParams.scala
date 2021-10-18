// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml

import org.apache.spark.ml.param.{MapParam, Param, Params}
import spray.json.DefaultJsonProtocol._

trait HasFeedFetchDicts extends Params {
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
