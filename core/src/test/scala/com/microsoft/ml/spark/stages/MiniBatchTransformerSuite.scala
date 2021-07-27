// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.stages

import com.microsoft.ml.spark.core.test.base.TestBase
import com.microsoft.ml.spark.core.test.fuzzing.{TestObject, TransformerFuzzing}
import com.microsoft.ml.spark.stages
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.param.DataFrameEquality
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.scalactic.Equality
import org.scalatest.Assertion
import org.apache.spark.sql.functions.{col, udf}

trait MiniBatchTestUtils extends TestBase with DataFrameEquality {
  import spark.implicits._
  lazy val n = 1000
  lazy val df: DataFrame = sc.parallelize((1 to n).zip(List.fill(n)("foo"))).toDF("in1", "in2")

  def delayedIterator(n: Int, wait: Int = 5): Iterator[Int] = {
    (1 to n).toIterator.map { x =>
      Thread.sleep(x * wait.toLong)
      x
    }
  }

  def basicTest(t: MiniBatchBase): Assertion = {
    val delay = 5
    val slowDf = df.map { x => Thread.sleep(3 * delay.toLong); x }(RowEncoder(df.schema))
    val df2 = t.transform(slowDf)

    val df3 = df2.map { x => Thread.sleep(10 * delay.toLong); x }(RowEncoder(df2.schema))

    assert(df3.schema == new StructType()
      .add("in1", ArrayType(IntegerType))
      .add("in2", ArrayType(StringType)))

    val lengths = df3.collect().map(row => row.getSeq(0).length)
    assert(lengths.sum == n)
    assert(lengths.max > 2)

    val flattened = new FlattenBatch().transform(df3)

    assert(flattened.collect() === df.collect())
  }

  override implicit lazy val dfEq: Equality[DataFrame] = new Equality[DataFrame]{
    def areEqual(a: DataFrame, bAny: Any): Boolean = bAny match {
      case ds: Dataset[_] =>
        val b = ds.toDF()
        baseDfEq.areEqual(new FlattenBatch().transform(a), new FlattenBatch().transform(b))
    }
  }
}

class DynamicMiniBatchTransformerSuite
  extends TransformerFuzzing[DynamicMiniBatchTransformer]
  with MiniBatchTestUtils {

  test("basic functionality") {
    basicTest(new DynamicMiniBatchTransformer())
  }

  override def testObjects(): Seq[TestObject[DynamicMiniBatchTransformer]] = Seq(
    new TestObject(new DynamicMiniBatchTransformer(), df))

  override def reader: MLReadable[_] = DynamicMiniBatchTransformer

}

class FixedMiniBatchTransformerSuite
  extends TransformerFuzzing[FixedMiniBatchTransformer]
  with MiniBatchTestUtils {

  test("basic functionality") {
    basicTest(new FixedMiniBatchTransformer().setBatchSize(3))
  }

  test("buffered functionality") {
    basicTest(new FixedMiniBatchTransformer().setBuffered(true).setBatchSize(3))
  }

  override def testObjects(): Seq[TestObject[FixedMiniBatchTransformer]] = Seq(
    new TestObject(
      new FixedMiniBatchTransformer().setBatchSize(3),
      df))

  override def reader: MLReadable[_] = FixedMiniBatchTransformer

}

class TimeIntervalMiniBatchTransformerSuite
  extends TransformerFuzzing[TimeIntervalMiniBatchTransformer]
  with MiniBatchTestUtils {

  test("basic functionality") {
    basicTest(new TimeIntervalMiniBatchTransformer().setMillisToWait(1000))
  }

  override def testObjects(): Seq[TestObject[TimeIntervalMiniBatchTransformer]] = Seq(
    new TestObject(
      new TimeIntervalMiniBatchTransformer()
        .setMillisToWait(1000)
        .setMaxBatchSize(30),
      df))

  override def reader: MLReadable[_] = TimeIntervalMiniBatchTransformer

}

object FlattenBatchUtils extends Serializable {
  def nullify(arr: Seq[Int]): Seq[Int] = {
    if (arr.head == 7){
      null
    }else{
      arr
    }
  }
}

class FlattenBatchSuite extends TransformerFuzzing[FlattenBatch] {
  import spark.implicits._
  lazy val n = 1000
  lazy val df: DataFrame = sc.parallelize((1 to n).zip(List.fill(n)("foo"))).toDF("in1", "in2")

  test("null support"){
    val batchedDf = new stages.FixedMiniBatchTransformer().setBatchSize(3).transform(df)
    val nullifiedDf = batchedDf.withColumn(
      "nullCol", UDFUtils.oldUdf(FlattenBatchUtils.nullify _, ArrayType(IntegerType))(col("in1")))
    assert(new FlattenBatch().transform(nullifiedDf).count() == 1000)
  }

  override def testObjects(): Seq[TestObject[FlattenBatch]] = Seq(
    new TestObject[FlattenBatch](
      new FlattenBatch(),
      new DynamicMiniBatchTransformer().transform(df))
  )

  override def reader: MLReadable[_] = FlattenBatch

}
