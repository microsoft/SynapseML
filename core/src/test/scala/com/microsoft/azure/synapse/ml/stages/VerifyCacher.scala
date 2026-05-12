// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.stages

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.storage.StorageLevel

class VerifyCacher extends TestBase {
  import spark.implicits._

  test("transform with disable=false caches the dataframe") {
    val df = Seq(1, 2, 3).toDF("x")
    val cacher = new Cacher().setDisable(false)
    val result = cacher.transform(df)
    result.count()
    assert(result.storageLevel !== StorageLevel.NONE)
    result.unpersist()
  }

  test("transform with disable=true does not cache") {
    val df = Seq(4, 5, 6).toDF("y")
    val cacher = new Cacher().setDisable(true)
    val result = cacher.transform(df)
    assert(result.storageLevel === StorageLevel.NONE)
  }

  test("default disable value is false") {
    val cacher = new Cacher()
    assert(!cacher.getDisable)
  }

  test("copy preserves params") {
    val cacher = new Cacher().setDisable(true)
    val copied = cacher.copy(new org.apache.spark.ml.param.ParamMap())
    assert(copied.asInstanceOf[Cacher].getDisable)
  }

  test("transformSchema returns same schema") {
    val df = Seq(1, 2, 3).toDF("x")
    val cacher = new Cacher()
    assert(cacher.transformSchema(df.schema) === df.schema)
  }
}
