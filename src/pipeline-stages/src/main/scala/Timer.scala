// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.{Estimator, Model, PipelineStage, Transformer}
import org.apache.spark.ml.param.{BooleanParam, ParamMap, PipelineStageParam}
import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType

object Timer extends DefaultParamsReadable[Timer]

trait TimerParams extends MMLParams {
  val stage = new PipelineStageParam(this, "stage", "the stage to time")

  def getStage: PipelineStage = $(stage)

  val logToScala = BooleanParam(this, "logToScala", "Whether to output the time to the scala console", true)

  def getLogToScala: Boolean = $(logToScala)

  def setLogToScala(v: Boolean): this.type = set(logToScala, v)

  protected def logTime(str: String): Unit = {
    println(str)
  }

}

class Timer(val uid: String) extends Estimator[TimerModel] with TimerParams {

  def this() = this(Identifiable.randomUID("Timer"))

  def setStage(s: PipelineStage): this.type = set(stage, s)

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = getStage.transformSchema(schema)

  def fitWithTime(dataset: Dataset[_]): (TimerModel, String) = {
    getStage match {
      case t: Transformer => (new TimerModel(uid, t).setParent(this), "")
      case e: Estimator[_] =>
        val t0 = System.nanoTime()
        val fitE = e.fit(dataset)
        val t1 = System.nanoTime()
        (new TimerModel(uid, fitE).setParent(this), s"$e took ${(t1 - t0) / 1000000000.0}s to fit")
    }
  }

  def fit(dataset: Dataset[_]): TimerModel = {
    val (model, message) = fitWithTime(dataset)
    if (message != "") logTime(message)
    model
  }

}

class TimerModel(val uid: String, t: Transformer) extends Model[TimerModel] with TimerParams {
  override def copy(extra: ParamMap): TimerModel = {
    new TimerModel(uid, t.copy(extra)).setParent(parent)
  }

  def transformWithTime(dataset: Dataset[_]): (DataFrame, String) = {
    val t0 = System.nanoTime()
    val res = t.transform(dataset)
    val t1 = System.nanoTime()
    (res, s"$t took ${(t1 - t0) / 1000000000.0}s to transform")
  }

  def transformSchema(schema: StructType): StructType = t.transformSchema(schema)

  def transform(dataset: Dataset[_]): DataFrame = {
    val (model, message) = transformWithTime(dataset)
    logTime(message)
    model
  }
}
