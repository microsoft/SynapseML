// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.schema

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.core.schema.DatasetExtensions._

class VerifyDatasetExtensions extends TestBase {

  test("findUnusedColumnName returns prefix when not in set") {
    assert(findUnusedColumnName("col")(Set("other")) === "col")
  }

  test("findUnusedColumnName returns prefix_1 when prefix is taken") {
    assert(findUnusedColumnName("col")(Set("col")) === "col_1")
  }

  test("findUnusedColumnName returns prefix_2 when prefix and prefix_1 are taken") {
    assert(findUnusedColumnName("col")(Set("col", "col_1")) === "col_2")
  }

  test("findUnusedColumnName with empty set returns prefix") {
    assert(findUnusedColumnName("col")(Set.empty[String]) === "col")
  }

  test("getColAs retrieves column values from DataFrame") {
    import spark.implicits._
    val df = Seq(1, 2, 3).toDF("num")
    val result = df.getColAs[Int]("num")
    assert(result === Seq(1, 2, 3))
  }

  test("withDerivativeCol returns unused column name for DataFrame") {
    import spark.implicits._
    val df = Seq((1, "a")).toDF("col", "col_1")
    assert(df.withDerivativeCol("col") === "col_2")
    assert(df.withDerivativeCol("newcol") === "newcol")
  }

}
