// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.params

import com.microsoft.azure.synapse.ml.core.serialize.ComplexParam
import org.apache.spark.ml.param.Params

/** Param for FObjTrait.  Needed as spark has explicit params for many different
  * types but not FObjTrait.
  */
class FObjParam(parent: Params, name: String, doc: String,
                isValid: FObjTrait => Boolean)

  extends ComplexParam[FObjTrait](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, {_ => true})
}
