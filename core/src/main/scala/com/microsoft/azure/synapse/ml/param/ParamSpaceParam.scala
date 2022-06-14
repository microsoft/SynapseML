// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.param

import com.microsoft.azure.synapse.ml.core.serialize.ComplexParam
import org.apache.spark.ml.param.{ParamMap, Params}

/** Represents the parameter values.
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
    this(parent, name, doc, (_: ParamSpace) => true)

}
