// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.param

/** Param for Estimator.  Needed as spark has explicit params for many different
  * types but not Estimator.
  */
class ArrayParamMapParam(parent: Params, name: String, doc: String, isValid: Array[ParamMap] => Boolean)
  extends ComplexParam[Array[ParamMap]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

}
