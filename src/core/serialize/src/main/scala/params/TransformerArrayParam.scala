// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.param

import org.apache.spark.ml.Transformer
import scala.collection.JavaConverters._

/** Param for Array of Models. */
class TransformerArrayParam(parent: Params, name: String, doc: String, isValid: Array[Transformer] => Boolean)
  extends ComplexParam[Array[Transformer]](parent, name, doc, isValid) {

    def this(parent: Params, name: String, doc: String) =
      this(parent, name, doc, ParamValidators.alwaysTrue)

    /** Creates a param pair with the given value (for Java). */
    def w(value: java.util.List[Transformer]): ParamPair[Array[Transformer]] = w(value.asScala.toArray)

  }
