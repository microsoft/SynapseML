// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame

class UnicodeNormalizeSuite extends TestBase with TransformerFuzzing[UnicodeNormalize] {
  val INPUT_COL = "words1"
  val OUTPUT_COL = "norm1"

  val wordDF = session.createDataFrame(Seq(
    ("Schön", 1),
    ("Scho\u0308n", 1),
    (null, 1)))
    .toDF(INPUT_COL, "dummy")

  val expectedResultComposed = session.createDataFrame(Seq(
    ("Schön", 1, "schön"),
    ("Scho\u0308n", 1, "schön"),
    (null, 1, null)))
    .toDF(INPUT_COL, "dummy", OUTPUT_COL)

  val expectedResultDecomposed = session.createDataFrame(Seq(
    ("Schön", 1, "sch\u0308n"),
    ("Scho\u0308n", 1, "sch\u0308n"),
    (null, 1, null)))
    .toDF(INPUT_COL, "dummy", OUTPUT_COL)

  private def testForm(form: String, expected: DataFrame) = {
    val unicodeNormalize = new UnicodeNormalize()
      .setForm(form)
      .setInputCol(INPUT_COL)
      .setOutputCol(OUTPUT_COL)
    val result = unicodeNormalize.transform(wordDF)
    assert(verifyResult(result, expected))
  }

  test("Check for NFC forms") { testForm("NFC", expectedResultComposed) }

  test("Check for NFKC forms") { testForm("NFKC", expectedResultComposed) }

  test("Check for NFD forms") { testForm("NFD", expectedResultDecomposed) }

  test("Check for NFKD forms") { testForm("NFKD", expectedResultDecomposed) }

  def testObjects(): Seq[TestObject[UnicodeNormalize]] = List(new TestObject(
    new UnicodeNormalize().setInputCol("words").setOutputCol("out"), makeBasicDF()))

  override def reader: MLReadable[_] = UnicodeNormalize

}
