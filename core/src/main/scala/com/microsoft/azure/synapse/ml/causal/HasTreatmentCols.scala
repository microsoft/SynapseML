// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.causal

import org.apache.spark.ml.param.{Params, StringArrayParam}

/** Ordered continuous treatments for joint, partially linear Double ML. */
trait HasTreatmentCols extends Params {
  val treatmentCols = new StringArrayParam(this, "treatmentCols", "ordered continuous treatment columns")

  def getTreatmentCols: Array[String] = $(treatmentCols).clone()

  /** Set the ordered treatments. Mutually exclusive with treatmentCol at fit time.
    * @group setParam
    */
  def setTreatmentCols(value: Array[String]): this.type = set(treatmentCols, value.clone())
}
