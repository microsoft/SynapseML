// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.lightgbm

import com.microsoft.ml.spark.core.serialize.ComplexParam
import org.apache.spark.ml.param.Params

/** Custom ComplexParam for LightGBMBooster, to make it settable on the LightGBM models.
  */
class LightGBMBoosterParam(parent: Params, name: String, doc: String,
                        isValid: LightGBMBooster => Boolean)

  extends ComplexParam[LightGBMBooster](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, {_ => true})

}
