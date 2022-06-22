// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.param

import com.microsoft.azure.synapse.ml.core.serialize.ComplexParam
import com.microsoft.azure.synapse.ml.nn.{BallTree, ConditionalBallTree}
import org.apache.spark.ml.param.Params

/** Param for a BallTree.
  */
class BallTreeParam(parent: Params, name: String, doc: String, isValid: BallTree[_] => Boolean)
  extends ComplexParam[BallTree[_]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, (_: BallTree[_]) => true)

}

class ConditionalBallTreeParam(parent: Params,
                               name: String,
                               doc: String,
                               isValid: ConditionalBallTree[_, _] => Boolean)
  extends ComplexParam[ConditionalBallTree[_, _]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, (_: ConditionalBallTree[_, _]) => true)

}
