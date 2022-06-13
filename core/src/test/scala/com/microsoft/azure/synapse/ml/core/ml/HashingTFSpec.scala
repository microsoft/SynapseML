// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.ml

import com.microsoft.azure.synapse.ml.core.schema.DatasetExtensions._
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.linalg.SparseVector

class HashingTFSpec extends TestBase {

  test("operation on tokenized strings") {
    val wordDataFrame = spark.createDataFrame(Seq(
      (0, Array("Hi", "I", "can", "not", "foo", "foo")),
      (1, Array("I")),
      (2, Array("Logistic", "regression")),
      (3, Array("Log", "f", "reg"))
    )).toDF("label", "words")

    val hashDF = new HashingTF().setInputCol("words").setOutputCol("hashedTF").transform(wordDataFrame)
    val lines = hashDF.getSVCol("hashedTF")

    val trueLines = List(
      new SparseVector(262144, Array(44775,108437,156204,215198,221693), Array(1.0,1.0,1.0,2.0,1.0)),
      new SparseVector(262144, Array(156204), Array(1.0)),
      new SparseVector(262144, Array(46243,142455), Array(1.0, 1.0)),
      new SparseVector(262144, Array(134093,228158,257491), Array(1.0, 1.0, 1.0))
    )
    assert(lines === trueLines)
  }

  test("support several values for number of features") {
    val featureSizes = List(1, 5, 100, 100000)
    val words = Array("Hi", "I", "can", "not", "foo", "bar", "foo", "afk")
    val wordDataFrame = spark.createDataFrame(Seq((0, words))).toDF("label", "words")

    val fsResults = featureSizes.map { n =>
      new HashingTF()
        .setNumFeatures(n)
        .setInputCol("words")
        .setOutputCol("hashedTF")
        .transform(wordDataFrame)
        .getSVCol("hashedTF")(0)
    }
    val trueResults = Array(
      new SparseVector(1,      Array(0), Array(8.0)),
      new SparseVector(5,      Array(0,1,2,3), Array(2.0,2.0,1.0,3.0)),
      new SparseVector(100,    Array(5,16,18,32,33,70,91), Array(1.0,1.0,2.0,1.0,1.0,1.0,1.0)),
      new SparseVector(100000, Array(17405,37332,54133,54316,55591,75270,98718),
        Array(1.0,1.0,1.0,1.0,1.0,1.0,2.0))
    )
    assert(fsResults === trueResults)
  }

  test("treat empty strings as another word") {
    val wordDataFrame = spark.createDataFrame(Seq(
      (0, "hey you no way"),
      (1, "")))
      .toDF("label", "sentence")

    val tokenized = new Tokenizer().setInputCol("sentence").setOutputCol("tokens").transform(wordDataFrame)
    val hashDF = new HashingTF().setInputCol("tokens").setOutputCol("HashedTF").transform(tokenized)

    val lines = hashDF.getSVCol("hashedTF")
    assert(lines(1) === new SparseVector(262144, Array(249180), Array(1.0)))
  }

  test("raise an error when applied to a null array") {
    val tokenDataFrame = spark.createDataFrame(Seq(
      (0, Some(Array("Hi", "I", "can", "not", "foo"))),
      (1, None))
    ).toDF("label", "tokens")
    assertSparkException[org.apache.spark.SparkException](new HashingTF().setInputCol("tokens"), tokenDataFrame)
  }

  test("raise an error when given strange values of n") {
    List(0, -1, -10).foreach { n =>
      intercept[IllegalArgumentException] {
        new HashingTF().setNumFeatures(n)
      }
    }
  }

}
