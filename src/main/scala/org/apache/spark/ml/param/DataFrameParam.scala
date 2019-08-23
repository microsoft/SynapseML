// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.param

import com.microsoft.ml.spark.core.serialize.ComplexParam
import org.apache.spark.sql.DataFrame

/** Param for DataFrame.  Needed as spark has explicit com.microsoft.ml.spark.core.serialize.params for many different
  * types but not DataFrame.
  */
class DataFrameParam(parent: Params, name: String, doc: String, isValid: DataFrame => Boolean)
  extends ComplexParam[DataFrame](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)
}
