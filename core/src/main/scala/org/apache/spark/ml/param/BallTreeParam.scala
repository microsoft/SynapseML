// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.param

import com.microsoft.ml.spark.core.serialize.ComplexParam
import com.microsoft.ml.spark.nn.{BallTree, ConditionalBallTree}

/** Param for a BallTree.
  */
class BallTreeParam(parent: Params, name: String, doc: String, isValid: BallTree[_] => Boolean)
  extends ComplexParam[BallTree[_]](parent, name, doc, isValid) with WrappableParam[BallTree[_]] {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  override def dotnetValue(v: BallTree[_]): String =
    throw new NotImplementedError("No translation found for complex parameter")

  override def dotnetType: String = "object"

}

class ConditionalBallTreeParam(parent: Params,
                               name: String,
                               doc: String,
                               isValid: ConditionalBallTree[_, _] => Boolean)
  extends ComplexParam[ConditionalBallTree[_, _]](parent, name, doc, isValid)
    with WrappableParam[ConditionalBallTree[_, _]]{

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  override def dotnetValue(v: ConditionalBallTree[_, _]): String =
    throw new NotImplementedError("No translation found for complex parameter")

  override def dotnetType: String = "object"

}
