// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.param

import com.microsoft.ml.spark.core.serialize.ComplexParam

/** Param for Array of ParamMaps.  Needed as spark has explicit params for many different
  * types but not Array of ParamMaps.
  */
class ArrayParamMapParam(parent: Params, name: String, doc: String, isValid: Array[ParamMap] => Boolean)
  extends ComplexParam[Array[ParamMap]](parent, name, doc, isValid)
    with WrappableParam[Array[ParamMap]] {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  override def dotnetValue(v: Array[ParamMap]): String =
    throw new NotImplementedError("No translation found for complex parameter")

  override def dotnetParamInfo: String = "ParamMap[]"

}
