// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.core.utils

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.param.Param
import org.apache.spark.ml.util.MLReadable
import org.scalactic.TripleEquals._

trait ParamEquality[T] extends Param[T] {
  def assertEquality(v1: Any, v2: Any): Unit
}


object ModelEquality {

  def jaccardSimilarity(s1: String, s2: String): Double = {
    val a = Set(s1)
    val b = Set(s2)
    a.intersect(b).size.toDouble / (a | b).size.toDouble
  }

  def assertEqual(m1: PipelineStage, m2: PipelineStage): Unit = {
    assert(m1.getClass === m2.getClass, s"${m1.getClass} != ${m2.getClass}, assertion failed.")
    val m1Params = m1.extractParamMap().toSeq.map(pp => pp.param.name).toSet
    val m2Params = m2.extractParamMap().toSeq.map(pp => pp.param.name).toSet
    assert(m1Params === m2Params)

    m1Params.foreach { paramName =>
      val p1 = m1.getParam(paramName)
      val p2 = m2.getParam(paramName)
      val v1 = m1.getOrDefault(p1)
      val v2 = m2.getOrDefault(p2)

      p1 match {
        case pe1: ParamEquality[_] =>
          pe1.assertEquality(v1, v2)
        case _ if Set("outputCol", "errorCol", "featuresCol")(paramName) => // These usually have UIDs in them
          assert(v1.asInstanceOf[String].length == v2.asInstanceOf[String].length, s"$v1 != $v2")
        case _ if Set("defaultListenPort")(paramName) => // Randomly assigned ports in LightGBM
          assert(v1.asInstanceOf[Int] > 0 && v1.asInstanceOf[Int] > 0)
        case _ if Set("validationMetrics")(paramName) =>
          assert(v1.asInstanceOf[Seq[Double]].length === v2.asInstanceOf[Seq[Double]].length) // This can be flaky
        case _ =>
          assert(v1 === v2, s"Param: ${p1.name} not equal with ${v1.toString} and ${v2.toString}")
      }

    }
  }

  private def companion[T](name: String)(implicit man: Manifest[T]): T =
    Class.forName(name + "$").getField("MODULE$").get(man.runtimeClass).asInstanceOf[T]


  def assertEqual(modelClassName: String, path1: String, path2: String): Unit = {
    val companionObject = companion[MLReadable[_ <: PipelineStage]](modelClassName)
    assertEqual(companionObject.load(path1), companionObject.load(path2))
  }

}
