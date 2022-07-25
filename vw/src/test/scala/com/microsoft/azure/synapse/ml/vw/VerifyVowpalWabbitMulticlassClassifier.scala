// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.core.test.benchmarks.{Benchmarks, DatasetUtils}
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{EstimatorFuzzing, TestObject}
import org.apache.spark.TaskContext
import org.apache.spark.ml.classification.{LogisticRegression, OneVsRest}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, UserDefinedType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import java.io.File

class VerifyVowpalWabbitMulticlassClassifier extends Benchmarks with EstimatorFuzzing[VowpalWabbitMulticlassClassifier] {
  lazy val moduleName = "vw"
  val numClasses = 11

  def getVowelTrainDataFrame(localNumPartitions: Int): Dataset[Row] = {
    val fileLocation = DatasetUtils.multiclassTrainFile(s"vowel.train.svmlight").toString
    spark.read.format("libsvm")
      .load(fileLocation)
      .repartition(localNumPartitions)
  }

  def getVowelTestDataFrame(localNumPartitions: Int): Dataset[Row] = {
    val fileLocation = DatasetUtils.multiclassTestFile(s"vowel.test.svmlight").toString
    spark.read.format("libsvm")
      .load(fileLocation)
      .repartition(localNumPartitions)
  }

  test ("Verify VowpalWabbit Multiclass Classifier") {
    val train = getVowelTrainDataFrame(1)
    val test = getVowelTestDataFrame(2)

    // find max-label
    // train.select(max(col("label"))).show()

    val vw = new VowpalWabbitMulticlassClassifier()
      .setNumClasses(numClasses)
      .setNumPasses(30)
      .setPassThroughArgs("--quiet --holdout_off --loss_function=logistic -q ::")

    val model = vw.fit(train)

    val evaluator = new MulticlassClassificationEvaluator()
      .setMetricName("accuracy")

    assert(evaluator.evaluate(model.transform(train)) > 0.9)
    assert(evaluator.evaluate(model.transform(test)) > 0.5)
  }

  test ("Verify VowpalWabbit Multiclass Classifier Probability output") {
    val train = getVowelTrainDataFrame(1)

    val vw = new VowpalWabbitMulticlassClassifier()
      .setNumClasses(numClasses)
      .setPassThroughArgs("--quiet --holdout_off --loss_function=logistic")

    val model = vw.fit(train)

    model.setTestArgs("--probabilities --loss_function=logistic")

    model.transform(train).show()

    val outputSchema = model.transform(train).schema

    assert(outputSchema("prediction").dataType == DoubleType)
    assert(outputSchema("probability").dataType == org.apache.spark.ml.linalg.SQLDataTypes.VectorType)
  }

  override def reader: MLReadable[_] = VowpalWabbitMulticlassClassifier
  override def modelReader: MLReadable[_] = VowpalWabbitMulticlassModel

  override def testObjects(): Seq[TestObject[VowpalWabbitMulticlassClassifier]] = {
    val dataset = getVowelTrainDataFrame(1)
    Seq(new TestObject(
      new VowpalWabbitMulticlassClassifier()
        .setNumClasses(numClasses),
      dataset))
  }
}
