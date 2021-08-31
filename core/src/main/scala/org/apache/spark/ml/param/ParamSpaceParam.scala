// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.param

import com.microsoft.ml.spark.core.serialize.ComplexParam

/** Represents the parameter values.
  */
abstract class ParamSpace {
  def paramMaps: Iterator[ParamMap]
}

/** Param for ParamSpace.  Needed as spark has explicit com.microsoft.ml.spark.core.serialize.params for many different
  * types but not ParamSpace.
  */
class ParamSpaceParam(parent: Params, name: String, doc: String, isValid: ParamSpace => Boolean)
    extends ComplexParam[ParamSpace](parent, name, doc, isValid) with WrappableParam[ParamSpace] {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  override def dotnetValue(v: ParamSpace): String =
    throw new NotImplementedError("No translation found for complex parameter")

  override def dotnetType: String = "object"

}
