// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.param

import com.microsoft.azure.synapse.ml.core.serialize.ComplexParam
import org.apache.spark.ml.param.Params
import org.apache.spark.sql.types.{DataType, StructType}

/** Param for DataType */
class DataTypeParam(parent: Params, name: String, doc: String, isValid: DataType => Boolean)
  extends ComplexParam[DataType](parent, name, doc, isValid) with WrappableParam[DataType] {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, (_: DataType) => true)

}
