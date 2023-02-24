// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.spark

import com.microsoft.azure.synapse.ml.core.test.base.TestBase

class VerifyFunctions extends TestBase {
  import spark.implicits._

  val df = Seq(
    (1, "Bob"),
    (2, "Mandy")
  ).toDF("x", "yz")

  def exprValidate(expr: String, expected: String*): Unit = {
    val output = df.select(Functions.template(expr)).collect().map(r => r.getString(0))

    assert(output === expected)
  }

  test("Template Parser Literals") {
    exprValidate("a", "a", "a")
    exprValidate("ab", "ab", "ab")
    exprValidate("", "", "")
  }

  test("Template Parser Variable x") { exprValidate("{x}", "1", "2") }

  test("Template Parser Variable _x") { exprValidate(" {x}", " 1", " 2") }

  test("Template Parser Variable x_") { exprValidate("{x} ", "1 ", "2 ") }

  test("Template Parser Variable _yz_") { exprValidate(" {yz} ", " Bob ", " Mandy ")}

  test("Template Parser Variable xyz") { exprValidate("{x}{yz}", "1Bob", "2Mandy")}

  test("Template Parser Variable _xyz") { exprValidate(" {x}{yz}", " 1Bob", " 2Mandy")}

  test("Template Parser Variable xyz_)") { exprValidate("{x}{yz} ", "1Bob ", "2Mandy ")}

  test("Template Parser Variable _x_yz_)") { exprValidate(" {x} {yz} ", " 1 Bob ", " 2 Mandy ")}

  test("Template Parser Variable Qx") { exprValidate("{{{x}", "{1", "{2")}

  test("Template Parser Variable QxQ") { exprValidate("{{{x}}}", "{1}", "{2}")}

  test("Template Parser Variable xQ") { exprValidate("{x}}}", "1}", "2}")}

  test("Template Parser Variable Exception B_") { assertThrows[IllegalArgumentException] { exprValidate("{") } }

  // ignoring this case for now
  // test("Template Parser Variable Exception _B") { assertThrows[IllegalArgumentException] { exprValidate("}") } }

  test("Template Parser Variable empty") { exprValidate("{}", "", "") }
}
