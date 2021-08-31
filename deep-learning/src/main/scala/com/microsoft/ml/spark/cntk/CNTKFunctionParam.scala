// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cntk

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import com.microsoft.CNTK.SerializableFunction
import com.microsoft.ml.spark.core.env.StreamUtilities
import com.microsoft.ml.spark.core.serialize.ComplexParam
import com.microsoft.ml.spark.core.utils.ParamEquality
import org.apache.spark.ml.param.{Params, WrappableParam}
import org.scalactic.TripleEquals._

/** Param for ByteArray.  Needed as spark has explicit com.microsoft.ml.spark.core.serialize.params for many different
  * types but not ByteArray.
  */
class CNTKFunctionParam(parent: Params, name: String, doc: String,
                        isValid: SerializableFunction => Boolean)

  extends ComplexParam[SerializableFunction](parent, name, doc, isValid)
    with ParamEquality[SerializableFunction] with WrappableParam[SerializableFunction] {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, { _ => true })

  override def assertEquality(v1: Any, v2: Any): Unit = {
    def toByteArray(value: Any): Array[Byte] = {
      StreamUtilities.using(new ByteArrayOutputStream()) { stream =>
        StreamUtilities.using(new ObjectOutputStream(stream)) { oos =>
          oos.writeObject(value)
        }
        stream.toByteArray
      }.get
    }

    (v1, v2) match {
      case (f1: SerializableFunction, f2: SerializableFunction) =>
        assert(f1.fvar.getOutput.getName == f2.fvar.getOutput.getName)
        assert(f1.fvar.getInputs.get(0).getName == f2.fvar.getInputs.get(0).getName)
        // TODO make this check more robust
      case _ =>
        throw new AssertionError("Values did not have DataFrame type")
    }
  }

  override def dotnetValue(v: SerializableFunction): String = s"""${name}Param"""

  override def dotnetType: String = "object"

}
