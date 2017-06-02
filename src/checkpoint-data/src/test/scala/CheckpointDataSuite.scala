// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.Transformer
import org.apache.spark.sql.types._
import org.apache.spark.ml.param._

class CheckpointDataSuite extends TestBase {

  test("Smoke test for Spark session version") {
    assert(session.sparkContext.version ==
             sys.env.getOrElse("SPARK_VERSION",
                               sys.error("Missing $SPARK_VER environment variable")))
  }

  import session.implicits._

  test("Cache DF") {
    val input = makeBasicDF()
    input.createOrReplaceTempView("cachingDFView")

    val checkpointer = new CheckpointData().setDiskIncluded(false).setRemoveCheckpoint(false)
    checkpointer.transform(input)

    assert(input.sqlContext.isCached("cachingDFView"))
  }

  test("Remove Cache on DF") {
    assert(session.sqlContext.isCached("cachingDFView"))
    val input = session.table("cachingDFView")

    val checkpointer = new CheckpointData().setDiskIncluded(false).setRemoveCheckpoint(true)
    checkpointer.transform(input)

    assert(!input.sqlContext.isCached("cachingDFView"))
  }

}
