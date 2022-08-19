// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.explainers

import com.microsoft.azure.synapse.ml.image.{HasCellSize, HasModifier, SuperpixelTransformer}
import org.apache.spark.ml.param.shared.HasInputCol
import org.apache.spark.sql.DataFrame

/**
  * Common preprocessing logic for image explainers
  */
trait ImageExplainer {
  self: LocalExplainer
    with HasCellSize
    with HasModifier
    with HasInputCol
    with HasSuperpixelCol =>

  protected override def preprocess(df: DataFrame): DataFrame = {
    // Dataframe with new column containing superpixels (Array[Cluster]) for each row (image to explain)
    new SuperpixelTransformer()
      .setCellSize(getCellSize)
      .setModifier(getModifier)
      .setInputCol(getInputCol)
      .setOutputCol(getSuperpixelCol)
      .transform(df)
  }
}
