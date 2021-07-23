// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.param

import com.microsoft.ml.spark.core.serialize.ComplexParam
import org.apache.spark.ml.evaluation.Evaluator

/** Param for Evaluator.  Needed as spark has explicit com.microsoft.ml.spark.core.serialize.params for many different
  * types but not Evaluator.
  */
class EvaluatorParam(parent: Params, name: String, doc: String, isValid: Evaluator => Boolean)
  extends ComplexParam[Evaluator](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

}
