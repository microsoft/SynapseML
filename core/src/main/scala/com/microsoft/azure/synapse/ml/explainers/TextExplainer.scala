// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.explainers

import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.param.shared.HasInputCol
import org.apache.spark.sql.DataFrame

/**
  * Common preprocessing logic for text explainers
  */
trait TextExplainer {
  self: LocalExplainer
    with HasInputCol
    with HasTokensCol =>

  protected override def preprocess(df: DataFrame): DataFrame = {
    new Tokenizer().setInputCol(getInputCol).setOutputCol(getTokensCol).transform(df)
  }
}
