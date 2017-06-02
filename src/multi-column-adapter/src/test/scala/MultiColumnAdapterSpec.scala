// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.feature.Tokenizer
import com.microsoft.ml.spark.schema.DatasetExtensions._
import org.apache.spark.ml.Transformer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

class MultiColumnAdapterSpec extends TransformerFuzzingTest {

  val wordDF = session.createDataFrame(Seq(
    (0, "This is a test", "this is one too"),
    (1, "could be a test", "maybe not"),
    (2, "foo", "bar")))
    .toDF("label", "words1", "words2")
  val inputCols = "words1,words2"
  val outputCols = "output1,output2"

  test("parallelize transformers") {
    val transformer1 = new Tokenizer()
    val adapter1 =
      new MultiColumnAdapter().setBaseTransformer(transformer1).setInputCols(inputCols).setOutputCols(outputCols)
    val tokenizedDF = adapter1.transform(wordDF)
    val lines = tokenizedDF.getColAs[Array[String]]("output2")

    val trueLines = Array(
      Array("this", "is", "one", "too"),
      Array("maybe", "not"),
      Array("bar")
    )
    assert(lines === trueLines)
  }

  override def setParams(fitDataset: DataFrame, transformer: Transformer): Transformer =
    transformer.asInstanceOf[MultiColumnAdapter]
      .setBaseTransformer(new Tokenizer())
      .setInputCols(inputCols)
      .setOutputCols(outputCols)

  override def createDataset: DataFrame = wordDF

  override def schemaForDataset: StructType = ???

  override def getTransformer(): Transformer = new MultiColumnAdapter()

}
