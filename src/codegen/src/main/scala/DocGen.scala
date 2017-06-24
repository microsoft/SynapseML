// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.codegen

import java.io.File
import org.apache.commons.lang3.StringUtils

import com.microsoft.ml.spark.FileUtilities._
import Config._

/** Generate .rst file for each Python file inorder to autogenerate API documentation.
  * This generation should be run before the __init__.py file is generated, and before the Python is zipped
  * into the zip file.
  */
object DocGen {

  protected def rstFileLines(modules: String): String = {
    s"""|Pyspark Library
        |===============
        |
        |.. toctree::
        |   :maxdepth: 4
        |
        |$modules
        |""".stripMargin
  }

  protected def contentsString(name: String)=
    s"""|$name
        |${"=" * name.length ()}
        |
        |.. automodule:: $name
        |    :members:
        |    :undoc-members:
        |    :show-inheritance:
        |""".stripMargin

  def genRstFiles(): Unit = {
    // Generate a modules.rst file that lists all the .py files to be included in API documentation
    val moduleString = allFiles(toZipDir, _.getName.endsWith(".py"))
      .map(f => s"   ${f.getName.dropRight(3)}\n").mkString("")
    writeFile(new File(inDocDir, "modules.rst"), rstFileLines(moduleString))

    // Generate .rst file for each PySpark wrapper - for documentation generation
    allFiles(toZipDir, _.getName.endsWith(".py"))
        .foreach{x => writeFile(new File(inDocDir, x.getName.dropRight(3) + ".rst"),
          contentsString(x.getName.dropRight(3)))
        }
  }

}

