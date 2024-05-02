// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.codegen

import com.microsoft.azure.synapse.ml.codegen.CodegenConfigProtocol._
import com.microsoft.azure.synapse.ml.codegen.PyCodegen.pyGen
import com.microsoft.azure.synapse.ml.codegen.RCodegen.rGen
import org.apache.commons.io.FileUtils
import spray.json._

import java.io.File

object CodeGenUtils {
  def clean(dir: File): Unit = if (dir.exists()) FileUtils.forceDelete(dir)

  def toDir(f: File): File = new File(f, File.separator)
}

object CodeGen {

  import CodeGenUtils._

  def main(args: Array[String]): Unit = {
    val conf = args.head.parseJson.convertTo[CodegenConfig]
    clean(conf.packageDir)
    rGen(conf)
    pyGen(conf)
  }
}
