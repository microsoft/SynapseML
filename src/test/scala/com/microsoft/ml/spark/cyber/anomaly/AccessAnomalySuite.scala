// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cyber.anomaly

import com.microsoft.ml.spark.core.test.fuzzing.{EstimatorFuzzing, TestObject, TransformerFuzzing}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}

import scala.util.Random

class AccessAnomalySuite extends EstimatorFuzzing[AccessAnomaly] {

  import session.implicits._

  lazy val r = new Random(0)

  def sample(n: Int, low: Int, high: Int): Seq[Int] = {
    Seq.fill(n) {
      r.nextInt(high - low) + low
    }
  }

  lazy val users = sample(20, 0, 30) ++
    sample(20, 30, 40) ++
    sample(20, 40, 50)
  lazy val resources = sample(20, 0, 10) ++
    sample(20, 10, 20) ++
    sample(20, 20, 30)
  lazy val acceses = sample(60, 1, 4)

  lazy val trainDF = users.zip(resources).zip(acceses).toDF("user", "res", "acc")

  lazy val usersWithAnamoly = users ++ sample(10, 40, 50)
  lazy val resourcesWithAnamoly = resources ++  sample(10, 0, 10)
  lazy val accesesWithAnomaly = acceses ++ sample(10, 1, 4)

  def aa: AccessAnomaly = new AccessAnomaly()
    .setTenantCol("tenant")
    .setUserCol("user")
    .setResCol("res")
    .setRatingCol("acc")

  def accessSet(df: DataFrame, tenant: String): Set[Seq[Int]] = {
    df
      .where(col("tenant") === lit(tenant))
      .collect().map(r => Seq(r.getInt(1), r.getInt(2)))
      .toSet
  }

  test("basic usage") {
    val results = ca.transform(df).cache()
    Seq("t1", "t2").map(tenant =>
      assert(accessSet(results, tenant).intersect(accessSet(df, tenant)).isEmpty)
    )
    assert(results.count == 10)
  }

  override def testObjects(): Seq[TestObject[ComplementSampler]] = Seq(
    new TestObject(ca, df)
  )

  override def reader: MLReadable[_] = ComplementSampler

}

