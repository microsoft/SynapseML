// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.param

import com.microsoft.azure.synapse.ml.core.serialize.ComplexParam
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{ParamPair, Params}

import scala.collection.JavaConverters._

/** Param for Array of Models. */
class TransformerArrayParam(parent: Params, name: String, doc: String, isValid: Array[Transformer] => Boolean)
  extends ComplexParam[Array[Transformer]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, (_: Array[Transformer]) => true)

  /** Creates a param pair with the given value (for Java). */
  def w(value: java.util.List[Transformer]): ParamPair[Array[Transformer]] = w(value.asScala.toArray)

}
