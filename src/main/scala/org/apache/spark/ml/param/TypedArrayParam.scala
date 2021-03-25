// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.param

import spray.json.JsonFormat
import spray.json.DefaultJsonProtocol._
import scala.collection.JavaConverters._


class TypedArrayParam[T](parent: Params,
                         name: String,
                         doc: String,
                         isValid: Seq[T] => Boolean = ParamValidators.alwaysTrue)
                        (@transient implicit val dataFormat: JsonFormat[T])
  extends JsonEncodableParam[Seq[T]](parent, name, doc, isValid) {
  type ValueType = T

  def w(v: java.util.ArrayList[T]): ParamPair[Seq[T]] = w(v.asScala)

}
