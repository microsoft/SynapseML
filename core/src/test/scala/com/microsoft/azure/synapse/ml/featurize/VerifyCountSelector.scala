// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.featurize

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{EstimatorFuzzing, TestObject, TransformerFuzzing}
import org.apache.commons.io.FileUtils
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, lit, when}


trait VerifyCountSelectorShared extends TestBase {
  import spark.implicits._

  lazy val df: DataFrame = Seq(
    (Vectors.sparse(3, Seq((0, 1.0), (2, 2.0))), Vectors.dense(1.0, 0.1, 0)),
    (Vectors.sparse(3, Seq((0, 1.0), (2, 2.0))), Vectors.dense(1.0, 0.1, 0))
  ).toDF("col1", "col2")

  lazy val emptyVectorDf: DataFrame = Seq(
    Tuple1(Vectors.sparse(4, Array.empty[Int], Array.empty[Double])),
    Tuple1(Vectors.dense(0.0, 0.0, 0.0, 0.0))
  ).toDF("features")

  lazy val emptyVectorDfWithNull: DataFrame = emptyVectorDf.unionByName(
    emptyVectorDf.limit(1).select(when(lit(false), col("features")).as("features")))

}

class VerifyCountSelector extends EstimatorFuzzing[CountSelector] with VerifyCountSelectorShared {

  test("Basic Usage") {
    val result1 = new CountSelector().setInputCol("col1").setOutputCol("col3").fit(df).transform(df)
    assert(result1.head().getAs[SparseVector]("col3").size === 2)

    val result2 = new CountSelector().setInputCol("col2").setOutputCol("col4").fit(df).transform(df)
    assert(result2.head().getAs[DenseVector]("col4").size === 2)
  }

  test("issue 1667 - CountSelector produces an empty model without throwing") {
    val model = new CountSelector().setInputCol("features").setOutputCol("selected").fit(emptyVectorDf)
    assert(model.getIndices.isEmpty)

    val transformed = model.transform(emptyVectorDf)
    val selectedVectors = transformed.select("selected").collect().map(_.getAs[Vector](0))
    assert(selectedVectors.forall(_.size == 0))
    assert(AttributeGroup.fromStructField(transformed.schema("selected")).numAttributes.contains(0))
  }

  override def testObjects(): List[TestObject[CountSelector]] = List(new TestObject(
    new CountSelector().setInputCol("col1").setOutputCol("col3"), df))

  override def reader: MLReadable[_] = CountSelector

  override def modelReader: MLReadable[_] = CountSelectorModel

}

class VerifyCountSelectorModel extends TransformerFuzzing[CountSelectorModel] with VerifyCountSelectorShared {

  test("issue 1667 - CountSelectorModel supports empty indices, copy, and persistence") {
    val model = new CountSelectorModel()
      .setIndices(Array.empty[Int])
      .setInputCol("features")
      .setOutputCol("selected")

    val copiedModel = model.copy(ParamMap.empty)
    assert(copiedModel.getIndices.isEmpty)
    assert(copiedModel.uid == model.uid)
    assert(copiedModel.getInputCol == model.getInputCol)
    assert(copiedModel.getOutputCol == model.getOutputCol)

    val modelDir = tmpDir.resolve("count-selector-empty-model").toFile
    if (modelDir.exists()) {
      FileUtils.deleteDirectory(modelDir)
    }

    try {
      model.write.overwrite().save(modelDir.getAbsolutePath)
      val loadedModel = CountSelectorModel.load(modelDir.getAbsolutePath)
      assert(loadedModel.getIndices.isEmpty)

      val expected = model.transform(emptyVectorDfWithNull).select("selected")
      val actual = loadedModel.transform(emptyVectorDfWithNull).select("selected")
      assert(AttributeGroup.fromStructField(actual.schema("selected")).numAttributes.contains(0))
      assert(verifyResult(expected, actual))
    } finally {
      if (modelDir.exists()) {
        FileUtils.deleteDirectory(modelDir)
      }
    }
  }

  test("issue 1667 - empty CountSelectorModel preserves nulls and matches transformSchema") {
    val model = new CountSelectorModel()
      .setIndices(Array.empty[Int])
      .setInputCol("features")
      .setOutputCol("selected")

    val expectedSchema = model.transformSchema(emptyVectorDfWithNull.schema)
    val transformed = model.transform(emptyVectorDfWithNull)
    val selected = transformed.select("selected").collect().map(row => Option(row.getAs[Vector](0)))

    assert(transformed.schema == expectedSchema)
    assert(transformed.schema("selected").nullable)
    assert(selected.count(_.isEmpty) == 1)
    assert(selected.flatten.forall(_.size == 0))
    assert(AttributeGroup.fromStructField(transformed.schema("selected")).numAttributes.contains(0))
  }

  test("issue 1667 - empty CountSelectorModel validates input and output columns") {
    val model = new CountSelectorModel()
      .setIndices(Array.empty[Int])
      .setInputCol("features")
      .setOutputCol("selected")

    val wrongType = emptyVectorDf.select(lit(1.0).as("features"))
    val wrongTypeError = intercept[IllegalArgumentException] {
      model.transform(wrongType)
    }
    assert(wrongTypeError.getMessage.contains("features"))
    assert(wrongTypeError.getMessage.contains("vector"))

    val existingOutput = emptyVectorDf.withColumn("selected", lit(1.0))
    val existingOutputError = intercept[IllegalArgumentException] {
      model.transform(existingOutput)
    }
    assert(existingOutputError.getMessage.contains("selected"))
    assert(existingOutputError.getMessage.contains("already exists"))
  }

  override def testObjects(): List[TestObject[CountSelectorModel]] = List(new TestObject(
    new CountSelectorModel().setIndices(Array(0)).setInputCol("col1").setOutputCol("col3"), df))

  override def reader: MLReadable[_] = CountSelectorModel
}
