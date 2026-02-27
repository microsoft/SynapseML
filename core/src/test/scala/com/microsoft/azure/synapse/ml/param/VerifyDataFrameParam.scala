// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.param

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.ml.linalg.{DenseVector, Vectors}
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._

class VerifyDataFrameParam extends TestBase {

  import spark.implicits._

  private class TestParamsHolder extends Params {
    override val uid: String = "test-holder"
    val dfParam = new DataFrameParam(this, "dataFrame", "A dataframe param")
    override def copy(extra: ParamMap): Params = this
  }

  // DataFrameParam basic tests
  test("DataFrameParam can be created with basic constructor") {
    val holder = new TestParamsHolder
    assert(holder.dfParam.name === "dataFrame")
    assert(holder.dfParam.doc === "A dataframe param")
  }

  test("DataFrameParam accepts DataFrame") {
    val holder = new TestParamsHolder
    val df = Seq(("a", 1), ("b", 2)).toDF("str", "num")
    holder.set(holder.dfParam, df)
    assert(holder.isSet(holder.dfParam))
  }

  test("DataFrameParam pyValue returns DF reference") {
    val holder = new TestParamsHolder
    val df = Seq(1, 2, 3).toDF("num")
    val pyVal = holder.dfParam.pyValue(df)
    assert(pyVal === "dataFrameDF")
  }

  test("DataFrameParam pyLoadLine generates Python code") {
    val holder = new TestParamsHolder
    val pyCode = holder.dfParam.pyLoadLine(1)
    assert(pyCode.contains("spark.read.parquet"))
    assert(pyCode.contains("model-1.model"))
    assert(pyCode.contains("complexParams"))
    assert(pyCode.contains("dataFrame"))
  }

  test("DataFrameParam rValue returns DF reference") {
    val holder = new TestParamsHolder
    val df = Seq(1, 2, 3).toDF("num")
    val rVal = holder.dfParam.rValue(df)
    assert(rVal === "dataFrameDF")
  }

  test("DataFrameParam rLoadLine generates R code") {
    val holder = new TestParamsHolder
    val rCode = holder.dfParam.rLoadLine(2)
    assert(rCode.contains("spark_read_parquet"))
    assert(rCode.contains("model-2.model"))
    assert(rCode.contains("complexParams"))
    assert(rCode.contains("dataFrame"))
  }

  // DataFrameEquality tests
  test("DataFrameEquality compares equal DataFrames correctly") {
    val holder = new TestParamsHolder
    val df1 = Seq(("a", 1), ("b", 2)).toDF("str", "num")
    val df2 = Seq(("a", 1), ("b", 2)).toDF("str", "num")
    // Should not throw
    holder.dfParam.assertEquality(df1, df2)
  }

  test("DataFrameEquality detects different DataFrames") {
    val holder = new TestParamsHolder
    val df1 = Seq(("a", 1), ("b", 2)).toDF("str", "num")
    val df2 = Seq(("a", 1), ("c", 3)).toDF("str", "num")
    assertThrows[AssertionError] {
      holder.dfParam.assertEquality(df1, df2)
    }
  }

  test("DataFrameEquality throws for non-DataFrame types") {
    val holder = new TestParamsHolder
    assertThrows[AssertionError] {
      holder.dfParam.assertEquality("not a df", "also not a df")
    }
  }

  // DataFrameEquality implicit tests
  test("DataFrameEquality handles doubles with tolerance") {
    val holder = new TestParamsHolder
    val df1 = Seq(1.0, 2.0, 3.0).toDF("num")
    val df2 = Seq(1.00001, 2.00001, 3.00001).toDF("num")
    // Should not throw due to tolerance
    holder.dfParam.assertEquality(df1, df2)
  }

  test("DataFrameEquality handles NaN values") {
    val holder = new TestParamsHolder
    val df1 = Seq(Double.NaN, 2.0).toDF("num")
    val df2 = Seq(Double.NaN, 2.0).toDF("num")
    holder.dfParam.assertEquality(df1, df2)
  }

  test("DataFrameEquality handles DenseVector columns") {
    val holder = new TestParamsHolder
    val schema = StructType(Seq(StructField("vec", new org.apache.spark.ml.linalg.VectorUDT())))
    val data1 = Seq(Row(Vectors.dense(1.0, 2.0)), Row(Vectors.dense(3.0, 4.0)))
    val data2 = Seq(Row(Vectors.dense(1.0, 2.0)), Row(Vectors.dense(3.0, 4.0)))
    val df1 = spark.createDataFrame(spark.sparkContext.parallelize(data1), schema)
    val df2 = spark.createDataFrame(spark.sparkContext.parallelize(data2), schema)
    holder.dfParam.assertEquality(df1, df2)
  }

  test("DataFrameEquality handles binary columns") {
    val holder = new TestParamsHolder
    val df1 = Seq(Array[Byte](1, 2, 3)).toDF("bytes")
    val df2 = Seq(Array[Byte](1, 2, 3)).toDF("bytes")
    holder.dfParam.assertEquality(df1, df2)
  }

  test("DataFrameEquality detects different column names") {
    val holder = new TestParamsHolder
    val df1 = Seq(1, 2, 3).toDF("col1")
    val df2 = Seq(1, 2, 3).toDF("col2")
    assertThrows[AssertionError] {
      holder.dfParam.assertEquality(df1, df2)
    }
  }

  test("DataFrameEquality detects different row counts") {
    val holder = new TestParamsHolder
    val df1 = Seq(1, 2, 3).toDF("num")
    val df2 = Seq(1, 2).toDF("num")
    assertThrows[AssertionError] {
      holder.dfParam.assertEquality(df1, df2)
    }
  }

  test("DataFrameParam with custom validator") {
    val holder = new Params {
      override val uid: String = "test"
      val nonEmptyDf = new DataFrameParam(
        this, "nonEmpty", "Non-empty dataframe",
        (df: DataFrame) => df.count() > 0
      )
      override def copy(extra: ParamMap): Params = this
    }
    val df = Seq(1, 2, 3).toDF("num")
    holder.set(holder.nonEmptyDf, df)
  }

  test("DataFrameParam sortInDataframeEquality is true") {
    val holder = new TestParamsHolder
    assert(holder.dfParam.sortInDataframeEquality)
  }
}
