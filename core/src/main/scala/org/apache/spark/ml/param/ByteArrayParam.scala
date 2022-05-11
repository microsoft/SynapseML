// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.param

import com.microsoft.azure.synapse.ml.core.serialize.ComplexParam

/** Param for ByteArray.  Needed as spark has explicit params for many different
  * types but not ByteArray.
  */
class ByteArrayParam(parent: Params, name: String, doc: String, isValid: Array[Byte] => Boolean)
    extends ComplexParam[Array[Byte]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  override def dotnetTestValue(v: Array[Byte]): String =
    s"""new byte[]
       |    ${DotnetWrappableParam.dotnetDefaultRender(v, this)}""".stripMargin

}
