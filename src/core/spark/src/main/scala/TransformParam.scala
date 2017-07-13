// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.param

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.util.Identifiable
import scala.collection.JavaConverters._

/** Param for Transformer.  Needed as spark has explicit params for many different
  * types but not Transformer.
  */
class TransformerParam(parent: String, name: String, doc: String, isValid: Transformer => Boolean)
  extends Param[Transformer](parent, name, doc, isValid) {

  def this(parent: String, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  def this(parent: Identifiable, name: String, doc: String, isValid: Transformer => Boolean) =
    this(parent.uid, name, doc, isValid)

  def this(parent: Identifiable, name: String, doc: String) =
    this(parent.uid, name, doc)

  /** Creates a param pair with the given value (for Java). */
  override def w(value: Transformer): ParamPair[Transformer] =
    super.w(value)

  override def jsonEncode(value: Transformer): String = {
    throw new NotImplementedError("The transform cannot be encoded.")
  }

  override def jsonDecode(json: String): Transformer = {
    throw new NotImplementedError("The transform cannot be decoded.")
  }

}

/** Param for Array of Models. */
class TransformerArrayParam(parent: String, name: String, doc: String, isValid: Array[Transformer] => Boolean)
  extends Param[Array[Transformer]](parent, name, doc, isValid) {

  def this(parent: String, name: String, doc: String) =

    this(parent, name, doc, ParamValidators.alwaysTrue)

  def this(parent: Identifiable, name: String, doc: String, isValid: Array[Transformer] => Boolean) =

    this(parent.uid, name, doc, isValid)

  def this(parent: Identifiable, name: String, doc: String) = this(parent.uid, name, doc)

  /** Creates a param pair with the given value (for Java). */
  def w(value: java.util.List[Transformer]): ParamPair[Array[Transformer]] = w(value.asScala.toArray)

  override def jsonEncode(value: Array[Transformer]): String = {
    throw new NotImplementedError("The transform cannot be encoded.")
  }

  override def jsonDecode(json: String): Array[Transformer] = {
    throw new NotImplementedError("The transform cannot be decoded.")
  }

}
