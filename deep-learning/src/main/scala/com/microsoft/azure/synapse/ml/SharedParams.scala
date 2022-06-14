// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml

import com.microsoft.azure.synapse.ml.param.StringStringMapParam
import org.apache.spark.ml.param.Params

import scala.collection.JavaConverters._

trait HasFeedFetchDicts extends Params {
  val feedDict: StringStringMapParam = new StringStringMapParam(
    this,
    "feedDict",
    " Provide a map from CNTK/ONNX model input variable names (keys) to column names of the input dataframe (values)"
  )

  def setFeedDict(value: Map[String, String]): this.type = set(feedDict, value)

  def setFeedDict(value: java.util.HashMap[String, String]): this.type = set(feedDict, value.asScala.toMap)

  def setFeedDict(k: String, v: String): this.type = set(feedDict, Map(k -> v))

  def getFeedDict: Map[String, String] = $(feedDict)

  val fetchDict: StringStringMapParam = new StringStringMapParam(
    this,
    "fetchDict",
    "Provide a map from column names of the output dataframe (keys) to CNTK/ONNX model output variable names (values)"
  )

  def setFetchDict(value: Map[String, String]): this.type = set(fetchDict, value)

  def setFetchDict(value: java.util.HashMap[String, String]): this.type = set(fetchDict, value.asScala.toMap)

  def setFetchDict(k: String, v: String): this.type = set(fetchDict, Map(k -> v))

  def getFetchDict: Map[String, String] = $(fetchDict)
}
