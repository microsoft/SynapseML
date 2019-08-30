// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.param

import org.apache.spark.annotation.DeveloperApi
import org.json4s.{DefaultFormats, _}
import org.json4s.jackson.JsonMethods.{compact, parse, render}

import scala.collection.JavaConverters._

/** :: DeveloperApi ::
  * Specialized generic version of `Param[Array[_]]` for Java.
  */
@DeveloperApi
class ArrayParam(parent: Params, name: String, doc: String, isValid: Array[_] => Boolean)
  extends Param[Array[_]](parent, name, doc, isValid) {

    def this(parent: Params, name: String, doc: String) =
      this(parent, name, doc, ParamValidators.alwaysTrue)

    /** Creates a param pair with a list of values (for Java and Python). */
    def w(value: java.util.List[_]): ParamPair[Array[_]] = w(value.asScala.toArray)

    override def jsonEncode(value: Array[_]): String = {
      import org.json4s.JsonDSL._
      value match {
        case intArr: Array[Int] => compact(render(intArr.toSeq))
        case dbArr: Array[Double] => compact(render(dbArr.toSeq))
        case strArr: Array[String] => compact(render(strArr.toSeq))
        case blArr: Array[Boolean] => compact(render(blArr.toSeq))
        case intArr: Array[Integer] => compact(render(intArr.map(_.toLong).toSeq))
        case _ =>
          throw new IllegalArgumentException("Internal type not json serializable")
      }
    }

    override def jsonDecode(json: String): Array[_] = {
      implicit val formats: DefaultFormats.type = DefaultFormats
      parse(json).extract[Seq[_]].toArray
    }
  }
