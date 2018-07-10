// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.sql.execution.streaming.{BingImageSource, CountSourceProvider}

class IteratorSourceSuite extends TestBase {

  test("Count", TestBase.Extended) {
    val q1 = session.readStream.format(classOf[CountSourceProvider].getName)
      .option("batchSize", 10)
      .load()
      .writeStream
      .format("console")
      .queryName("foo")
      .start()

    Thread.sleep(5000)
  }

}
