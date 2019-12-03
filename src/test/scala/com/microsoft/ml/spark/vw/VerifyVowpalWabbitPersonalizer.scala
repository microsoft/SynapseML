// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.vw

import java.io.File
import java.nio.file.Files

import com.microsoft.ml.spark.core.test.benchmarks.{Benchmarks, DatasetUtils}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import com.microsoft.ml.spark.core.test.fuzzing.{EstimatorFuzzing, TestObject}
import com.microsoft.ml.spark.vw.featurizer.{SeqFeaturizer, StringFeaturizer}
import org.apache.spark.TaskContext
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.vowpalwabbit.spark.VowpalWabbitMurmur

class VerifyVowpalWabbitPersonalizer extends Benchmarks {

  case class L1(arr: Array[L2])
  case class L2(ns1: L3)
  case class L3(a: Double, b: Float)

  test ("Verify VowpalWabbit can featurize Array(Struct(Double, Float))") {
    val df1 = session.createDataFrame(Seq(L1(Array(
      L2(L3(1.0, 2.0f))
    ))))

    val featurizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("arr"))

    val v1 = featurizer.transform(df1).select(col("features")).collect.apply(0).getAs[SparseVector](0)
    assert(v1.numNonzeros == 2)

    val bitMask = (1 << 30) - 1

    assert((bitMask & v1.indices(0)) == (bitMask & VowpalWabbitMurmur.hash("a", VowpalWabbitMurmur.hash("ns1", 0))))
  }

  case class L1Array(ns1: Array[Int])

  test ("Verify VowpalWabbit can featurize Array(Double)") {
    val df1 = session.createDataFrame(Seq(L1Array(Array(4, 5, 6))))

    val featurizer = new VowpalWabbitFeaturizer().setInputCols(Array("ns1"))

    val v1 = featurizer.transform(df1).select(col("features")).collect.apply(0).getAs[SparseVector](0)
    assert(v1.numNonzeros == 3)

    val bitMask = (1 << 30) - 1

    assert((bitMask & v1.indices(0)) == (bitMask & (0 + VowpalWabbitMurmur.hash("ns1", 0))))
    assert((bitMask & v1.indices(1)) == (bitMask & (1 + VowpalWabbitMurmur.hash("ns1", 0))))
    assert((bitMask & v1.indices(2)) == (bitMask & (2 + VowpalWabbitMurmur.hash("ns1", 0))))

    assert(v1.values.deep == Array(4.0, 5.0, 6.0).deep)
  }

  // TODO: more testing

  test ("Verify VowpalWabbit can ingest Personalizer logs") {

    val schema = DataTypes.createStructType(Array(StructField("EventId", DataTypes.StringType,true),
    StructField("Timestamp",DataTypes.StringType,true),
    StructField("VWState", DataTypes.createStructType(Array(StructField("m",DataTypes.StringType,true))),true),
    StructField("Version",DataTypes.StringType,true),
    StructField("_corrupt_record",DataTypes.StringType,true),
    StructField("_labelIndex",DataTypes.LongType,true),
    StructField("_label_Action",DataTypes.LongType,true),
    StructField("_label_cost",DataTypes.LongType,true),
    StructField("_label_probability",DataTypes.DoubleType,true),
    StructField("a",ArrayType(DataTypes.LongType,true),true),
    StructField("c", DataTypes.createStructType(Array(
      StructField("FromUrl",DataTypes.createArrayType(
        DataTypes.createStructType(Array(
        StructField("device",DataTypes.createStructType(Array(
        StructField("DeviceBrand",DataTypes.StringType,true),
        StructField("DeviceFamily",DataTypes.StringType,true),
        StructField("DeviceModel",DataTypes.StringType,true),
        StructField("OSFamily",DataTypes.StringType,true),
        StructField("UAFamily",DataTypes.StringType,true)))))))))), true),
    StructField("location",DataTypes.createStructType(Array(
      StructField("CountryISO",DataTypes.StringType,true))), true),
    StructField("_multi", DataTypes.createArrayType(DataTypes.createStructType(Array(
      StructField("_tag",DataTypes.StringType,true),
      StructField("i",new StructType(Array(
        StructField("constant",DataTypes.LongType,true),
        StructField("id",DataTypes.StringType,true))))))), true),
    StructField("j",ArrayType(DataTypes.createStructType(Array(
      StructField("Atitle",MapType(DataTypes.StringType, DataTypes.StringType),true),
      StructField("Bexcerpt",MapType(DataTypes.StringType, DataTypes.StringType),true),
      StructField("Categories",MapType(DataTypes.StringType, DataTypes.StringType),true),
      StructField("_URL",DataTypes.StringType,true)))),true),
    StructField("o",ArrayType(DataTypes.createStructType(Array(
      StructField("ActionTaken",DataTypes.BooleanType,true),
      StructField("EventId",DataTypes.StringType,true),
      StructField("v",DataTypes.DoubleType,true))),true),true),
    StructField("p",ArrayType(DataTypes.DoubleType,true),true)))

    // TODO: add to datasets?
    val df = session.read.json("file:////mnt/c/work/mmlspark/src/test/resources/personalizer1.json")
    df.show()

    val featurizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("context"))
      .setOutputCol("features")

    // println(df.selectExpr("c.FromUrl").schema)
    featurizer.transform(df.selectExpr("c.FromUrl AS context")).select("features").show()
    // assert(saveDir.exists)
  }
}