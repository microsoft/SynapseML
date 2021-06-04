// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.explainers

import org.apache.spark.sql.DataFrame

trait HasBackgroundData {
  protected var backgroundData: Option[DataFrame] = None

  final def setBackgroundDataset(backgroundDataset: DataFrame): this.type = {
    this.backgroundData = Some(backgroundDataset)
    this
  }
}
