// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.io.File

import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import java.nio.file.{Files, Path, Paths}

import org.apache.spark.sql.functions._
import org.apache.commons.io.FileUtils
import org.apache.spark.ml.feature.{Binarizer, StringIndexer}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder, TrainValidationSplit}

class VerifyVowpalWabbitFeaturizer extends TestBase {

  case class Sample1(val str: String, val seq: Seq[String])
  case class Sample2(val str: String, val seq: Seq[String])

  test("Verify VowpalWabbit Featurizer can be run with seq and string") {
    val featurizer1 = new VowpalWabbitFeaturizer()
      .setInputCols(Array("str", "seq"))
      .setOutputCol("features")
    val df1 = session.createDataFrame(Seq(Sample1("abc", Seq("foo", "bar"))))

    featurizer1.transform(df1).show

    val featurizer2 = new VowpalWabbitFeaturizer()
      .setInputCols(Array("seq", "str"))
      .setOutputCol("features")
    val df2 = session.createDataFrame(Seq(Sample1("abc", Seq("foo", "bar"))))

    featurizer2.transform(df2).show
  }
}