// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.param

import org.apache.spark.ml.param.{Param, ParamPair, Params}
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection.JavaConverters._

/** Param for Map of String to Seq of String. */
class MapParam[K, V](parent: Params, name: String, doc: String, isValid: Map[K, V] => Boolean)
                    (@transient implicit val fk: JsonFormat[K], @transient implicit val fv: JsonFormat[V])
  extends Param[Map[K, V]](parent, name, doc, isValid) with CollectionFormats
    with WrappableParam[Map[K, V]] {

  def this(parent: Params, name: String, doc: String)(implicit fk: JsonFormat[K], fv: JsonFormat[V]) =
    this(parent, name, doc, (_: Map[K, V]) => true)

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

  protected def valuesType = "object"

}

class StringStringMapParam(parent: Params, name: String, doc: String, isValid: Map[String, String] => Boolean)
  extends MapParam[String, String](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, (_: Map[String, String]) => true)

  override protected def valuesType = "string"

}

class StringIntMapParam(parent: Params, name: String, doc: String, isValid: Map[String, Int] => Boolean)
  extends MapParam[String, Int](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, (_: Map[String, Int]) => true)

  override protected def valuesType = "int"

}
