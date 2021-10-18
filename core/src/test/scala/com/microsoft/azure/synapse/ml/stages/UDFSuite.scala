// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.stages

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.sql.DataFrame

class UDFSuite extends TestBase {
  import spark.implicits._

  def makeFakeData(rows: Int, size: Int): DataFrame = {
    List
      .fill(rows)(List.fill(size)(0.0).toArray)
      .toDF("arr")
      .withColumn("vec", udfs.to_vector("arr"))
  }

  test("get value at") {
    val df = makeFakeData(5, 8)
      .withColumn("vec0", udfs.get_value_at("vec", 0))
    assert(df.select("vec0").first().getDouble(0) == 0)
  }
}
