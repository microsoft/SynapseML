// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.sql._
import org.apache.spark.ml.Transformer
import org.apache.spark.sql.types._
import org.apache.spark.ml.param._
import com.microsoft.ml.spark.schema._

import scala.reflect.{ClassTag, classTag}

class TestCategoricalMap extends TestBase {

  /** basic asserts that should be true for all Categorical Maps
    *
    * @param levels       sorted categorical levels
    * @param wrong_level  a value that is not a level
    * @param dataType     corresponding Spark datatype
    * @param isOrdinal    whether levels are Ordinal or not
    * @param mmlStyle     save to MML (true, default) or MLlib (false) metadata
    */
  private def testMapBasic[T: ClassTag](levels: Array[T], wrong_level: T, dataType: DataType,
                                        isOrdinal: Boolean, mmlStyle: Boolean = true): Unit = {

    val map = new CategoricalMap(levels, isOrdinal)
    val s   = " " + classTag[T]; // to idenfity which type throws the error

    assert(map.numLevels == levels.length, "numLevels" + s)
    assert(map.isOrdinal == isOrdinal, "isOrdinal" + s)
    assert(map.dataType == dataType, "dataType" + s)
    assert(map.getIndex(levels.head) == 0 & map.getIndex(levels.last) == levels.length - 1, "getIndex" + s)
    assert(map.getIndexOption(wrong_level) == None & map.getIndexOption(levels(1)) == Some(1), "getIndexOption" + s)
    assert(map.hasLevel(levels(1)) == true & map.hasLevel(wrong_level) == false, "hasLevel" + s)
    assert(map.getLevel(1) == levels(1), "getLevel" + s)
    assert(map.getLevelOption(1) == Some(levels(1)) & map.getLevelOption(-1) == None, "getLevelOption" + s)

    val mml_meta = map.toMetadata(mmlStyle) //TODO: check metadata for correctness
  }

  /** test CategoricalMap for different undelying types */
  test("Test: Create basic CategoricalMap") {

    for (mmlStyle <- List(true, false)) {

      val isOrdinal = mmlStyle

      val strArray = Array("as", "", "efe")
      testMapBasic(strArray, "wrong_level", StringType, isOrdinal, mmlStyle)

      val intArray = Array[Int](34, 54747, -346, 756, 0)
      testMapBasic(intArray, -45, IntegerType, isOrdinal, mmlStyle)

      val longArray = Array[Long](34, 54747, -346, 756, 0)
      testMapBasic(longArray, (-45: Long), LongType, isOrdinal, mmlStyle)

      val doubleArray = Array[Double](34.45, 54.747, -3.46, 7.56, 0)
      testMapBasic(doubleArray, (-45: Double), DoubleType, isOrdinal, mmlStyle)
    }
  }

  import session.implicits._

  /** sample dafaframe */
  private val DF = Seq[(Int, Long, Double, Boolean, String)](
      (-3, 24L, 0.32534, true, "piano"),
      (1, 5L, 5.67, false, "piano"),
      (-3, 5L, 0.32534, false, "guitar"))
    .toDF("int", "long", "double", "bool", "string")

  /** sample dafaframe with Null values*/
  private val nullDF = Seq[(String, java.lang.Integer, java.lang.Double)](
      ("Alice", null, 44.3),
      (null, 60, null),
      ("Josh", 25, Double.NaN))
    .toDF("string", "int", "double")

  /** test CategoricalMap for different undelying types */
  test("Test: Convert the regular column into categorical") {
    for (col <- DF.columns; mmlStyle <- List(false, true)) {
      val newName = col + "_cat"
      val df      = SparkSchema.makeCategorical(DF, column = col, newColumn = newName, mmlStyle)

      assert(!SparkSchema.isCategorical(df, col), "Check for non-categorical columns")
      assert(SparkSchema.isCategorical(df, newName), "Check for categorical columns")

      val info = new CategoricalColumnInfo(df, newName)

      assert(info.isCategorical, "the column is supposed to be categorical")
      assert(info.isMML == mmlStyle, "wrong metadata style in categorical column")
      assert(!info.isOrdinal, "wrong ordinal style in categorical column")
      if (mmlStyle)
        assert(info.dataType == DF.schema(col).dataType, "categorical data type is not correct")
      else
        assert(info.dataType == StringType, "categorical data type is not String")
    }
  }

  test("Test: String categorical levels") {
    val col = "string"
    val true_levels = DF.select("string").collect().map(_(0).toString).distinct.sorted

    for (mmlStyle <- List(false, true)) {
      val newName = col + "_cat"
      val df = SparkSchema.makeCategorical(DF, column = col, newColumn = newName, mmlStyle)

      val map = CategoricalUtilities.getMap[String](df.schema(newName).metadata)

      val levels = map.levels.sorted

      (true_levels zip levels).foreach {
        case (a, b) => assert(a == b, "categorical levels are not the same")
      }
    }
  }

  test("Test: Going to Categorical and Back") {
    val col = "string"
    for (mmlStyle <- List(false, true)) {
      val newName = col + "_cat"
      val df = SparkSchema.makeCategorical(DF, column = col, newColumn = newName, mmlStyle)

      val testName = col + "_noncat"
      val df1 = SparkSchema.makeNonCategorical(df, column = newName, newColumn = testName)

      df1.select(col, testName).collect.foreach(row => assert(row(0) == row(1), "two columns should be the same"))
    }
  }

}
