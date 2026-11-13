// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.schema

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.core.schema.DatasetExtensions._
import org.apache.spark.sql.catalyst.analysis.{caseInsensitiveResolution, caseSensitiveResolution}
import org.apache.spark.sql.internal.SQLConf

class VerifyDatasetExtensions extends TestBase {

  test("findUnusedColumnName returns prefix when not in set") {
    assert(findUnusedColumnName("col")(Set("other")) === "col")
  }

  test("findUnusedColumnName returns prefix_1 when prefix is taken") {
    assert(findUnusedColumnName("col")(Set("col")) === "col_1")
  }

  test("findUnusedColumnName returns prefix_2 when prefix and prefix_1 are taken") {
    assert(findUnusedColumnName("col")(Set("col", "col_1")) === "col_2")
  }

  test("findUnusedColumnName with empty set returns prefix") {
    assert(findUnusedColumnName("col")(Set.empty[String]) === "col")
  }

  test("findUnusedColumnName respects Spark's column resolver") {
    val conf = SQLConf.get
    val wasCaseSensitive = conf.caseSensitiveAnalysis
    conf.setConf(SQLConf.CASE_SENSITIVE, false)
    try {
      assert(findUnusedColumnName("col")(Set("COL")) === "col_1")
    } finally {
      conf.setConf(SQLConf.CASE_SENSITIVE, wasCaseSensitive)
    }
  }

  test("findUnusedColumnName with a Dataset uses that Dataset's resolver") {
    val inputSession = spark.newSession()
    inputSession.conf.set("spark.sql.caseSensitive", "true")
    import inputSession.implicits._
    val df = inputSession.range(1).toDF("COL")
    val conf = SQLConf.get
    val wasCaseSensitive = conf.caseSensitiveAnalysis
    conf.setConf(SQLConf.CASE_SENSITIVE, false)
    try {
      assert(findUnusedColumnName("col", df) === "col")
      assert(df.withDerivativeCol("col") === "col")
    } finally {
      conf.setConf(SQLConf.CASE_SENSITIVE, wasCaseSensitive)
    }
  }

  test("built-in resolvers use indexed lookup for collision-heavy names") {
    for (sensitive <- Seq(true, false)) {
      val session = spark.newSession()
      session.conf.set("spark.sql.caseSensitive", sensitive.toString)
      val builtin = if (sensitive) caseSensitiveResolution else caseInsensitiveResolution
      val prefix = if (sensitive) "col" else "COL"
      val occupied = (0 until 2000).map(i => if (i == 0) prefix else s"${prefix}_$i").toSet
      for (resolver <- Seq(builtin, resolverFor(session.range(1)))) {
        var visited = 0
        val names = new scala.collection.mutable.HashSet[String] {
          override def iterator: Iterator[String] = super.iterator.map { name =>
            visited += 1
            name
          }
          override def foreach[U](f: String => U): Unit = super.foreach { name =>
            visited += 1
            f(name)
          }
        }
        names ++= occupied
        assert(findUnusedColumnName("col", names, resolver) == "col_2000")
        assert(visited <= occupied.size * 2, s"Repeatedly scanned occupied names: $visited visits")
      }
    }
  }

  test("case-insensitive indexed lookup preserves Unicode equalsIgnoreCase matching") {
    val cases = Seq(
      ("i", Set("I", "\u0130_1", "\u0131_2"), "i_3"),
      ("\u03c3", Set("\u03a3", "\u03c2_1", "\u03c3_2"), "\u03c3_3"),
      ("k", Set("\u212a", "K_1"), "k_2"),
      ("ss", Set("\u00df"), "ss"))
    cases.foreach { case (prefix, names, expected) =>
      assert(findUnusedColumnName(prefix, names, caseInsensitiveResolution) == expected)
    }
  }

  test("custom resolvers retain their own matching semantics") {
    val resolver = (left: String, right: String) => left.replace('-', '_').equalsIgnoreCase(right)
    assert(findUnusedColumnName("name", Set("NAME", "name-1", "name-2"), resolver) == "name_3")
  }

  test("getColAs retrieves column values from DataFrame") {
    import spark.implicits._
    val df = Seq(1, 2, 3).toDF("num")
    val result = df.getColAs[Int]("num")
    assert(result === Seq(1, 2, 3))
  }

  test("withDerivativeCol returns unused column name for DataFrame") {
    import spark.implicits._
    val df = Seq((1, "a")).toDF("col", "col_1")
    assert(df.withDerivativeCol("col") === "col_2")
    assert(df.withDerivativeCol("newcol") === "newcol")
  }

}
