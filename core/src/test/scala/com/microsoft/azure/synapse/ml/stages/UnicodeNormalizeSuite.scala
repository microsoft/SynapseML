// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.stages

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.spark.ml.param.Param
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame

class UnicodeNormalizeSuite extends TestBase with TransformerFuzzing[UnicodeNormalize] {
  val inputCol = "words1"
  val outputCol = "norm1"

  //scalastyle:off null
  lazy val wordDF = spark.createDataFrame(Seq(
    ("Schön", 1),
    ("Scho\u0308n", 1),
    (null, 1)))
    .toDF(inputCol, "dummy")

  lazy val expectedResultComposed = spark.createDataFrame(Seq(
    ("Schön", 1, "schön"),
    ("Scho\u0308n", 1, "schön"),
    (null, 1, null)))
    .toDF(inputCol, "dummy", outputCol)

  lazy val expectedResultDecomposed = spark.createDataFrame(Seq(
    ("Schön", 1, "sch\u0308n"),
    ("Scho\u0308n", 1, "sch\u0308n"),
    (null, 1, null)))
    .toDF(inputCol, "dummy", outputCol)
  //scalastyle:on null

  private def testForm(form: String, expected: DataFrame) = {
    val unicodeNormalize = new UnicodeNormalize()
      .setForm(form)
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
    val result = unicodeNormalize.transform(wordDF)
    assert(verifyResult(result, expected))
  }

  test("Check for NFC forms") { testForm("NFC", expectedResultComposed) }

  test("Check for NFKC forms") { testForm("NFKC", expectedResultComposed) }

  test("Check for NFD forms") { testForm("NFD", expectedResultDecomposed) }

  test("Check for NFKD forms") { testForm("NFKD", expectedResultDecomposed) }

  test("setForm rejects an invalid form at set time") {
    // An unvalidated form used to be accepted here and only fail later inside the UDF,
    // on the executors, where the cause is far harder to attribute.
    assertThrows[IllegalArgumentException](new UnicodeNormalize().setForm("NOT_A_FORM"))
  }

  test("setForm accepts every supported form") {
    java.text.Normalizer.Form.values().foreach { f =>
      assert(new UnicodeNormalize().setForm(f.name).getForm === f.name)
    }
  }

  def testObjects(): Seq[TestObject[UnicodeNormalize]] = List(new TestObject(
    new UnicodeNormalize().setInputCol("words").setOutputCol("out"), makeBasicDF()))

  // The generic fuzzer probes String setters with "foo"; form only accepts Normalizer.Form names.
  override def getterSetterParamExamples(pipelineStage: UnicodeNormalize): Map[Param[_], Any] =
    Map[Param[_], Any]((pipelineStage.form, "NFC"))

  override def reader: MLReadable[_] = UnicodeNormalize

}
