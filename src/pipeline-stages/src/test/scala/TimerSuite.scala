// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}

class TimerSuite extends TestBase {

  import session.implicits._

  val df = session
    .createDataFrame(Seq((0, "Hi I"),
      (1, "I wish for snow today"),
      (2, "we Cant go to the park, because of the snow!"),
      (3, "")))
    .toDF("label", "sentence")

  test("Work with transformers and estimators") {

    val tok = new Tokenizer()
      .setInputCol("sentence")
      .setOutputCol("tokens")

    val df2 = new Timer().setStage(tok).fit(df).transform(df)

    val df3 = new HashingTF().setInputCol("tokens").setOutputCol("hash").transform(df2)

    val idf = new IDF().setInputCol("hash").setOutputCol("idf")

    val df4 = new Timer().setStage(idf).fit(df3).transform(df3)

  }

}
