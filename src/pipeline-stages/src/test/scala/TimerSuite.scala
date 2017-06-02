// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.param.ParamMap

class CacherSuite extends TestBase {

  import session.implicits._

  val df = Seq(
    (0, "guitars", "drums"),
    (1, "piano", "trumpet"),
    (2, "bass", "cymbals"),
    (3, "guitars", "drums"),
    (4, "piano", "trumpet"),
    (5, "bass", "cymbals"),
    (6, "guitars", "drums"),
    (7, "piano", "trumpet"),
    (8, "bass", "cymbals"),
    (9, "guitars", "drums"),
    (10, "piano", "trumpet"),
    (11, "bass", "cymbals")
  ).toDF("numbers", "words", "more")

  test("Be the identity operation") {
    val df2 = new Cacher().transform(df)
    assert(df2.collect() === df.collect())
  }

}