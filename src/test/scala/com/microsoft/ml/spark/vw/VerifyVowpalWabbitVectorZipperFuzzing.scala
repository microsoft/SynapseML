package com.microsoft.ml.spark.vw

import com.microsoft.ml.spark.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame

class VerifyVowpalWabbitVectorZipperFuzzing extends TransformerFuzzing[VectorZipper] {

  import session.implicits._

  def makeDFWithSequences(): DataFrame = {
    val df = Seq(
      ("action1_f", "action2_f"),
      ("action1_f", "action2_f"),
      ("action1_f", "action2_f"),
      ("action1_f", "action2_f")
    ).toDF("action1", "action2")

    val actionOneFeaturizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("action1"))
      .setOutputCol("sequence_one")

    val actionTwoFeaturizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("action2"))
      .setOutputCol("sequence_two")

    actionTwoFeaturizer.transform(actionOneFeaturizer.transform(df))
  }

  override def testObjects(): Seq[TestObject[VectorZipper]] = Seq(new TestObject(
    new VectorZipper().setInputCols(Array("sequence_one", "sequence_two")).setOutputCol("out"),
    makeDFWithSequences()))

  override def reader: MLReadable[_] = VectorZipper
}
