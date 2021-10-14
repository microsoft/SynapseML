// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.explainers

import com.microsoft.ml.spark.lime.{SuperpixelTransformer, SLICParams}
import org.apache.spark.ml.param.shared.HasInputCol
import org.apache.spark.sql.DataFrame

/**
  * Common preprocessing logic for image explainers
  */
trait ImageExplainer {
  self: LocalExplainer
    with SLICParams
    with HasInputCol
    with HasSuperpixelCol =>

  protected override def preprocess(df: DataFrame): DataFrame = {
    // Dataframe with new column containing superpixels (Array[Cluster]) for each row (image to explain)
    this.copyValues(new SuperpixelTransformer())
      .setOutputCol(getSuperpixelCol)
      .transform(df)
  }
}
