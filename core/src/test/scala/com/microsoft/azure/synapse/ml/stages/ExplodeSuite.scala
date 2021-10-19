// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.stages

import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame

class ExplodeSuite extends TransformerFuzzing[Explode] {

  import spark.implicits._
  lazy val df: DataFrame = Seq(
    (0, Seq("guitars", "drums")),
    (1, Seq("piano")),
    (2, Seq()))
    .toDF("numbers", "words")

  lazy val t: Explode = new Explode().setInputCol("words").setOutputCol("exploded")

  test("Basic usage") {
    val df2 = t.transform(df)
    df2.show()
    assert(df2.columns.length == 3)
    assert(df2.count() == 3)
    assert(df2.select("exploded").collect().map(_.getString(0))===Array("guitars", "drums", "piano"))
  }

  override def testObjects(): Seq[TestObject[Explode]] = Seq(new TestObject(t, df))

  override def reader: MLReadable[_] = Explode

}
