// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.param

/**
  * Represents the parameter values.
  */
abstract class ParamSpace {
  def paramMaps: Iterator[ParamMap]
}

/** Param for ParamSpace.  Needed as spark has explicit params for many different
  * types but not ParamSpace.
  */
class ParamSpaceParam(parent: Params, name: String, doc: String, isValid: ParamSpace => Boolean)
  extends ComplexParam[ParamSpace](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

}
