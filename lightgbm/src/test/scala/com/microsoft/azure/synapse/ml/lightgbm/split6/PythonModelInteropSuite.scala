// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split6

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.lightgbm.booster.LightGBMBooster
import scala.io.Source

import org.apache.spark.ml.linalg.{Vector, Vectors}

class PythonModelInteropSuite extends TestBase {
  private val featureCount = 4
  private val expectedCaseCount = 14
  private val expectedTreeCount = 40
  private val tolerance = 1e-9

  private def resource(name: String): String = {
    val stream = Option(getClass.getResourceAsStream(s"/python-interop/$name"))
      .getOrElse(throw new IllegalArgumentException(s"Missing interoperability fixture: $name"))
    val source = Source.fromInputStream(stream, "UTF-8")
    try {
      source.mkString
    } finally {
      source.close()
    }
  }

  private case class Expected(name: String, values: Array[Double], raw: Double,
                              normal: Double, leaves: Array[Double])

  private def examples: Seq[Expected] = resource("expected.tsv").trim.split("\n").map { line =>
    val fields = line.split("\t", -1)
    val scoreOffset = featureCount + 1
    require(fields.length == scoreOffset + 2 + expectedTreeCount)
    Expected(fields.head,
      fields.slice(1, scoreOffset).map(value => if (value == "nan") Double.NaN else value.toDouble),
      fields(scoreOffset).toDouble, fields(scoreOffset + 1).toDouble,
      fields.drop(scoreOffset + 2).map(_.toDouble))
  }.toSeq

  private def vectors(values: Array[Double]): Seq[Vector] = {
    val indices = values.indices.filter(index => values(index) != 0.0).toArray
    Seq(Vectors.dense(values), Vectors.sparse(values.length, indices, indices.map(values(_))))
  }

  test("Python-trained model retains scores and leaves for dense and sparse encoded inputs") {
    val cases = examples
    assert(cases.length == expectedCaseCount && cases.map(_.name).distinct.length == expectedCaseCount)
    val model = resource("python-model.txt")
    assert(model.contains("pandas_categorical:[[\"red\", \"blue\", \"green\"],"))
    val booster = new LightGBMBooster(model)
    try {
      cases.foreach { example =>
        vectors(example.values).foreach { vector =>
          val raw = booster.score(vector, raw = true, classification = false, disableShapeCheck = false)
          val normal = booster.score(vector, raw = false, classification = false, disableShapeCheck = false)
          assert(raw.length == 1 && normal.length == 1, example.name)
          assert(math.abs(raw.head - example.raw) <= tolerance, example.name + " raw score")
          assert(math.abs(normal.head - example.normal) <= tolerance, example.name + " normal score")
          assert(booster.predictLeaf(vector).sameElements(example.leaves), example.name + " leaves")
        }
      }
      val scores = cases.map { example =>
        example.name -> booster.score(Vectors.dense(example.values),
          raw = true, classification = false, disableShapeCheck = false).head
      }.toMap
      Seq("numeric_nan" -> "numeric_zero", "category_missing" -> "ordinary",
        "category_blue" -> "wrong_blue_mapping").foreach { case (left, right) =>
        assert(math.abs(scores(left) - scores(right)) > tolerance, s"$left must differ from $right")
      }
    } finally {
      booster.freeNativeMemory()
    }
  }
}
