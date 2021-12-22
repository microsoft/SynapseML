// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.flaky

import com.microsoft.azure.synapse.ml.core.test.base.{TestBase, TimeLimitedFlaky}
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import com.microsoft.azure.synapse.ml.stages.PartitionConsolidator
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{DoubleType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.scalatest.Assertion

class PartitionConsolidatorSuite extends TransformerFuzzing[PartitionConsolidator] with TimeLimitedFlaky {

  override def beforeAll(): Unit = {
    TestBase.resetSparkSession(numCores = Option(2))
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    TestBase.resetSparkSession()
    super.afterAll()
  }

  import spark.implicits._

  lazy val df: DataFrame = (1 to 1000).toDF("values")

  override val sortInDataframeEquality: Boolean = true

  override def testObjects(): Seq[TestObject[PartitionConsolidator]] = Seq(
    new TestObject(new PartitionConsolidator(), df))

  override def reader: MLReadable[_] = PartitionConsolidator

  def getPartitionDist(df: DataFrame): List[Int] = {
    df.rdd.mapPartitions(it => Iterator(it.length)).collect().toList
  }

  //TODO figure out what is causing the issue on the build server
  override def testSerialization(): Unit = {}

  override def testExperiments(): Unit = {}

  def basicTest(df: DataFrame): Assertion = {
    val pd1 = getPartitionDist(df)
    val newDF = new PartitionConsolidator().transform(df)
    val pd2 = getPartitionDist(newDF)
    assert(pd1.sum === pd2.sum)
    assert(pd2.max >= pd1.max)
    assert(pd1.length === pd2.length)
  }

  test("basic functionality") {
    basicTest(df)
  }

  test("works with more partitions than cores") {
    basicTest(df.repartition(12))
  }

  test("overheads") {
    val baseDF = (1 to 1000).toDF("values").cache()
    println(baseDF.count())

    def getDF: Dataset[Row] = baseDF.map { x => Thread.sleep(10); x }(
      RowEncoder(new StructType().add("values", DoubleType)))

    val t1 = getTime(3)(
      getDF.foreach(_ => ()))._2
    val t2 = getTime(3)(
      new PartitionConsolidator().transform(getDF).foreach(_ => ()))._2

    println(t2.toDouble / t1.toDouble)
    assert(t2.toDouble / t1.toDouble < 3.0)
  }

  test("works with more partitions than cores2") {
    basicTest(df.repartition(100))
  }

  test("work with 1 partition") {
    basicTest(df.repartition(1))
  }

}
