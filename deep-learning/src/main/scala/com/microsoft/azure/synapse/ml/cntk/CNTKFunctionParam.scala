// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cntk

import com.microsoft.CNTK.SerializableFunction
import com.microsoft.azure.synapse.ml.core.env.StreamUtilities
import com.microsoft.azure.synapse.ml.core.serialize.ComplexParam
import com.microsoft.azure.synapse.ml.core.utils.ParamEquality
import org.apache.spark.ml.param.Params

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

/** Param for ByteArray.  Needed as spark has explicit params for many different
  * types but not ByteArray.
  */
class CNTKFunctionParam(parent: Params, name: String, doc: String,
                        isValid: SerializableFunction => Boolean)

  extends ComplexParam[SerializableFunction](parent, name, doc, isValid)
    with ParamEquality[SerializableFunction] {

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

  override def dotnetSetter(dotnetClassName: String, capName: String, dotnetClassWrapperName: String): String = {
    name match {
      case "model" =>
        s"""|public $dotnetClassName SetModelLocation(string value) =>
            |    $dotnetClassWrapperName(Reference.Invoke("setModelLocation", value));
            |""".stripMargin
      case _ => super.dotnetSetter(dotnetClassName, capName, dotnetClassWrapperName)
    }
  }

}
