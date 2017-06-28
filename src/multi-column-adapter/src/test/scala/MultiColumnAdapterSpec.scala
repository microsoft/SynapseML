// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.feature.Tokenizer
import com.microsoft.ml.spark.schema.DatasetExtensions._
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

class MultiColumnAdapterSpec extends TestBase with TransformerFuzzing[MultiColumnAdapter] {

  val wordDF = session.createDataFrame(Seq(
    (0, "This is a test", "this is one too"),
    (1, "could be a test", "maybe not"),
    (2, "foo", "bar")))
    .toDF("label", "words1", "words2")
  val inputCols = "words1,words2"
  val outputCols = "output1,output2"
  val transformer1 = new Tokenizer()
  val adapter1 =
    new MultiColumnAdapter().setBaseTransformer(transformer1).setInputCols(inputCols).setOutputCols(outputCols)

  test("parallelize transformers") {

    val tokenizedDF = adapter1.transform(wordDF)
    val lines = tokenizedDF.getColAs[Array[String]]("output2")

    val trueLines = Array(
      Array("this", "is", "one", "too"),
      Array("maybe", "not"),
      Array("bar")
    )
    assert(lines === trueLines)
  }

  def testObjects(): Seq[TestObject[MultiColumnAdapter]] = List(new TestObject(adapter1, wordDF))

  override def reader: MLReadable[_] = MultiColumnAdapter
}
