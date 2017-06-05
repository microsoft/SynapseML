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
}
