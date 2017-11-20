// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.param

import org.apache.spark.sql.expressions.UserDefinedFunction

/** Param for UserDefinedFunction.  Needed as spark has explicit params for many different
  * types but not UserDefinedFunction.
  */
class UDFParam(parent: Params, name: String, doc: String, isValid: UserDefinedFunction => Boolean)
  extends ComplexParam[UserDefinedFunction](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

}
