// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.stages

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.{DenseVector, SQLDataTypes}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DoubleType, Metadata, StructField}

class EnsembleByKeySuite extends TestBase with TransformerFuzzing[EnsembleByKey] {

  test("Should work on Dataframes doubles or vectors") {
    val scoreDF = spark.createDataFrame(Seq(
      (0, "foo", 1.0, .1),
      (1, "bar", 4.0, -2.0),
      (1, "bar", 0.0, -3.0)))
      .toDF("label1", "label2", "score1", "score2")

    val va = new VectorAssembler().setInputCols(Array("score1", "score2")).setOutputCol("v1")
    val scoreDF2 = va.transform(scoreDF)

    val t = new EnsembleByKey().setKey("label1").setCol("score1")
    val df1 = t.transform(scoreDF2)
    df1.printSchema()
    assert(df1.collect().map(r => (r.getInt(0), r.getDouble(1))).toSet === Set((1, 2.0), (0, 1.0)))

    val t2 = new EnsembleByKey().setKeys("label1", "label2").setCols("score1", "score2", "v1")
    val df2 = t2.transform(scoreDF2)
    val res2 = df2.select("mean(score1)", "mean(v1)").collect().map(r => (r.getDouble(0), r.getAs[DenseVector](1)))
    val true2 = Set(
      (2.0, new DenseVector(Array(2.0, -2.5))),
      (1.0, new DenseVector(Array(1.0, 0.1))))
    assert(res2.toSet === true2)
  }

  test("should support collapsing or not") {
    val scoreDF = spark.createDataFrame(
        Seq((0, "foo", 1.0, .1),
            (1, "bar", 4.0, -2.0),
            (1, "bar", 0.0, -3.0)))
      .toDF("label1", "label2", "score1", "score2")

    val va = new VectorAssembler().setInputCols(Array("score1", "score2")).setOutputCol("v1")
    val scoreDF2 = va.transform(scoreDF)

    val t = new EnsembleByKey().setKey("label1").setCol("score1").setCollapseGroup(false)
    val df1 = t.transform(scoreDF2)

    assert(df1.collect().map(r => (r.getInt(0), r.getDouble(5))).toSet === Set((1, 2.0), (0, 1.0)))
    assert(df1.count() == scoreDF.count())
    df1.show()
  }

  test("transformSchema should match mixed aggregate output for default and explicit names") {
    val input = mixedTypeDF
    val inputNames = Array("doubleScore", "floatScore", "features")
    val defaultNames = inputNames.map(name => s"mean($name)")
    val explicitNames = Array("averageDouble", "averageFloat", "averageFeatures")
    val keyNames = Array("group", "region")

    assert(input.schema("features").metadata !== Metadata.empty)

    Seq(defaultNames -> false, explicitNames -> true).foreach { case (outputNames, useExplicitNames) =>
      Seq(true, false).foreach { collapseGroup =>
        val transformer = new EnsembleByKey()
          .setKeys(keyNames)
          .setCols(inputNames)
          .setCollapseGroup(collapseGroup)
        if (useExplicitNames) {
          transformer.setColNames(outputNames)
        }

        val transformedSchema = transformer.transformSchema(input.schema)
        val actualSchema = transformer.transform(input).schema
        val expectedNames = if (collapseGroup) {
          keyNames ++ outputNames
        } else {
          keyNames ++ input.columns.filterNot((keyNames ++ outputNames).contains) ++ outputNames
        }

        withClue(s"explicitNames=$useExplicitNames, collapseGroup=$collapseGroup: ") {
          assert(transformedSchema === actualSchema)
          assert(actualSchema.fieldNames === expectedNames)
          assert(actualSchema(outputNames(0)) === StructField(outputNames(0), DoubleType))
          assert(actualSchema(outputNames(1)) === StructField(outputNames(1), DoubleType))
          assert(actualSchema(outputNames(2)) ===
            StructField(outputNames(2), SQLDataTypes.VectorType, nullable = false))
        }
      }
    }
  }

  test("non-collapsed output should overwrite numeric and vector columns") {
    val input = mixedTypeDF
    val overwrittenNames = Array("doubleScore", "floatScore", "features")
    val transformer = new EnsembleByKey()
      .setKeys("group", "region")
      .setCols(overwrittenNames)
      .setColNames(overwrittenNames)
      .setCollapseGroup(false)

    val transformedSchema = transformer.transformSchema(input.schema)
    val transformed = transformer.transform(input)

    assert(transformed.schema === transformedSchema)
    assert(transformed.columns ===
      Array("group", "region", "id", "component1", "component2") ++ overwrittenNames)
    assert(transformed.schema("features").metadata === Metadata.empty)
    assert(!transformed.schema("features").nullable)

    val actual = transformed.orderBy("id")
      .select("doubleScore", "floatScore", "features")
      .collect()
      .map(row => (row.getDouble(0), row.getDouble(1), row.getAs[DenseVector](2)))
    val expected = Array(
      (1.0, 1.0, new DenseVector(Array(1.0, 0.1))),
      (2.0, 2.0, new DenseVector(Array(2.0, -2.5))),
      (2.0, 2.0, new DenseVector(Array(2.0, -2.5))))

    assert(actual === expected)
  }

  test("default output names should follow updated input columns before transform") {
    val transformer = new EnsembleByKey()
      .setKeys("group", "region")
      .setCol("doubleScore")

    transformer.transformSchema(mixedTypeDF.schema)
    transformer.setCols("doubleScore", "floatScore")

    assert(transformer.transformSchema(mixedTypeDF.schema).fieldNames ===
      Array("group", "region", "mean(doubleScore)", "mean(floatScore)"))
  }

  lazy val testDF: DataFrame = {
    val initialTestDF = spark.createDataFrame(
      Seq((0, "foo", 1.0, .1),
        (1, "bar", 4.0, -2.0),
        (1, "bar", 0.0, -3.0)))
      .toDF("label1", "label2", "score1", "score2")

    new VectorAssembler().setInputCols(Array("score1", "score2"))
      .setOutputCol("v1").transform(initialTestDF)
  }

  lazy val mixedTypeDF: DataFrame = {
    val initialTestDF = spark.createDataFrame(
      Seq((0, "west", "foo", 1.0, 1.0f, 1.0, 0.1),
          (1, "east", "bar", 4.0, 4.0f, 4.0, -2.0),
          (2, "east", "bar", 0.0, 0.0f, 0.0, -3.0)))
      .toDF("id", "region", "group", "doubleScore", "floatScore", "component1", "component2")

    new VectorAssembler()
      .setInputCols(Array("component1", "component2"))
      .setOutputCol("features")
      .transform(initialTestDF)
  }

  lazy val testModel: EnsembleByKey = new EnsembleByKey().setKey("label1").setCol("score1")
      .setCollapseGroup(false).setVectorDims(Map("v1"->2))

  test("should support passing the vector dims to avoid maerialization") {
    val df1 = testModel.transform(testDF)
    assert(df1.collect().map(r => (r.getInt(0), r.getDouble(5))).toSet === Set((1, 2.0), (0, 1.0)))
    assert(df1.count() == testDF.count())
    df1.show()
  }

  test("should overwrite a column if instructed") {
    val scoreDF = spark.createDataFrame(
        Seq((0, "foo", 1.0, .1),
            (1, "bar", 4.0, -2.0),
            (1, "bar", 0.0, -3.0)))
      .toDF("label1", "label2", "score1", "score2")

    val va = new VectorAssembler().setInputCols(Array("score1", "score2")).setOutputCol("v1")
    val scoreDF2 = va.transform(scoreDF)

    val t = new EnsembleByKey().setKey("label1").setCol("score1").setColName("score1").setCollapseGroup(false)
    val df1 = t.transform(scoreDF2)

    assert(scoreDF2.columns.toSet === df1.columns.toSet)

  }

  test("should roundtrip serialize") {
    testSerialization()
  }

  def testObjects(): Seq[TestObject[EnsembleByKey]] = Seq(new TestObject(testModel, testDF))

  def reader: EnsembleByKey.type = EnsembleByKey
}
