// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.param

import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection.JavaConverters._

/** Param for Map of String to Seq of String. */
class MapParam[K, V](parent: Params, name: String, doc: String, isValid: Map[K, V] => Boolean)
                    (@transient implicit val fk: JsonFormat[K], @transient implicit val fv: JsonFormat[V])
  extends Param[Map[K, V]](parent, name, doc, isValid) with CollectionFormats {

  def this(parent: Params, name: String, doc: String)(implicit fk: JsonFormat[K], fv: JsonFormat[V]) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  /** Creates a param pair with the given value (for Java). */
  def w(value: java.util.HashMap[K, V]): ParamPair[Map[K, V]] = {
    this.->(value.asScala.toMap)
  }

  override def jsonEncode(value: Map[K, V]): String = {
    value.toJson.compactPrint
  }

  override def jsonDecode(json: String): Map[K, V] = {
    json.parseJson.convertTo[Map[K, V]]
  }

}

class StringStringMapParam(parent: Params, name: String, doc: String, isValid: Map[String, String] => Boolean)
  extends Param[Map[String, String]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  /** Creates a param pair with a `java.util.HashMap` of values (for Java and Python). */
  def w(value: java.util.HashMap[String, String]): ParamPair[Map[String, String]] = {
    this.->(value.asScala.toMap)
  }

  override def jsonEncode(value: Map[String, String]): String = {
    value.toJson.compactPrint
  }

  override def jsonDecode(json: String): Map[String, String] = {
    json.parseJson.convertTo[Map[String, String]]
  }
}

class StringIntMapParam(parent: Params, name: String, doc: String, isValid: Map[String, Int] => Boolean)
  extends Param[Map[String, Int]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  /** Creates a param pair with a `java.util.HasMap` of values (for Java and Python). */
  def w(value: java.util.HashMap[String, Int]): ParamPair[Map[String, Int]] = {
    this.->(value.asScala.toMap)
  }

  override def jsonEncode(value: Map[String, Int]): String = {
    value.toJson.compactPrint
  }

  override def jsonDecode(json: String): Map[String, Int] = {
    json.parseJson.convertTo[Map[String, Int]]
  }
}
