// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.{Estimator, Model, PipelineStage, Transformer}
import org.apache.spark.ml.param.{ParamMap, PipelineStageParam}
import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType

object Timer extends DefaultParamsReadable[Timer]

class Timer(val uid: String) extends Estimator[TimerModel] with MMLParams {

  def this() = this(Identifiable.randomUID("Timer"))

  val stage = new PipelineStageParam(this, "stage", "the stage to time")

  def getStage: PipelineStage = $(stage)

  def setStage(s: PipelineStage): this.type = set(stage, s)

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = getStage.transformSchema(schema)

  def fit(dataset: Dataset[_]): TimerModel = {
    getStage match {
      case t: Transformer => new TimerModel(uid, t).setParent(this)
      case e: Estimator[_] =>
        val t0 = System.nanoTime()
        val fitE = e.fit(dataset)
        val t1 = System.nanoTime()
        println(s"$e took ${(t1 - t0) / 1000000000.0}s to fit")
        new TimerModel(uid, fitE).setParent(this)
    }
  }
}

class TimerModel(val uid: String, t: Transformer) extends Model[TimerModel] {
  override def copy(extra: ParamMap): TimerModel = {
    new TimerModel(uid, t.copy(extra)).setParent(parent)
  }

  def transform(dataset: Dataset[_]): DataFrame = {
    val t0 = System.nanoTime()
    val res = t.transform(dataset)
    val t1 = System.nanoTime()
    println(s"$t took ${(t1 - t0) / 1000000000.0}s to transform")
    res
  }

  def transformSchema(schema: StructType): StructType = t.transformSchema(schema)

}
