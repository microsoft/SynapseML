// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cognitive

object ParamBoilerplateGen extends App {

  def genMethods(name: String, description: String, typeName: String, isRequired: Boolean = false): String = {
    val capitalName = name.apply(0).toString.capitalize + name.tail
    s"""
       |val $name: ServiceParam[$typeName] = new ServiceParam[$typeName](
       |  this, "$name",
       |  "$description",
       |  isRequired=$isRequired)
       |
       |def get$capitalName: $typeName = getScalarParam($name)
       |
       |def set$capitalName(v: $typeName): this.type = setScalarParam($name, v)
       |
       |def get${capitalName}Col: String = getVectorParam($name)
       |
       |def set${capitalName}Col(v: String): this.type = setVectorParam($name, v)
       |""".stripMargin
  }

  val Entries = Seq(
    ("foo", "An example Parameter", "Int"),
    ("bar", "Another example Parameter", "Double")
  )

  println(Entries.map(t => genMethods(t._1, t._2, t._3)).mkString("\n"))
}
