// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.{Estimator, Model, PipelineStage, Transformer}
import org.apache.spark.ml.param.{BooleanParam, ParamMap, PipelineStageParam}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType

import scala.reflect.{ClassTag, classTag}

object Timer extends DefaultParamsReadable[Timer]

trait TimerParams extends Wrappable {
  val stage = new PipelineStageParam(this, "stage", "the stage to time")

  def getStage: PipelineStage = $(stage)

  val logToScala = BooleanParam(this, "logToScala", "Whether to output the time to the scala console", true)

  def getLogToScala: Boolean = $(logToScala)

  def setLogToScala(v: Boolean): this.type = set(logToScala, v)

  val cacheBefore = BooleanParam(this, "cacheBefore", "whether to cache the dataframe before entering the timer", true)

  def getCacheBefore: Boolean = $(cacheBefore)

  def setCacheBefore(v: Boolean): this.type = set(cacheBefore, v)

  val countBefore = BooleanParam(this, "countBefore", "whether to count the dataframe before entering the timer", true)

  def getCountBefore: Boolean = $(countBefore)

  def setCountBefore(v: Boolean): this.type = set(countBefore, v)

  val cacheAfter = BooleanParam(this, "cacheAfter", "whether to cache the dataframe before exiting the timer", true)

  def getCacheAfter: Boolean = $(cacheAfter)

  def setCacheAfter(v: Boolean): this.type = set(cacheAfter, v)

  val countAfter = BooleanParam(this, "countAfter", "whether to count the dataframe before exiting the timer", true)

  def getCountAfter: Boolean = $(countAfter)

  def setCountAfter(v: Boolean): this.type = set(countAfter, v)

  protected def formatTime(t: Long, isTransform: Boolean, count: Option[Long]): String = {
    val time = {
      t / 1000000000.0
    }
    val action = if (isTransform) "transform" else "predict"
    val amount = count match {
      case Some(i) => s"$i rows"
      case _ => ""
    }
    s"${this.getStage} took ${time}s to $action $amount"
  }

  protected def log(str: String): Unit = {
    if (str != "") {
      println(str)
    }
  }

}

class Timer(val uid: String) extends Estimator[TimerModel]
  with TimerParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("Timer"))

  def setStage(s: PipelineStage): this.type = set(stage, s)

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = getStage.transformSchema(schema)

  def fitWithTime(dataset: Dataset[_]): (TimerModel, String) = {

    val cachedDatasetBefore = if (getCacheBefore) dataset.cache() else dataset
    val countBeforeVal = if (getCountBefore) Some(dataset.count()) else None

    getStage match {
      case t: Transformer => (new TimerModel(uid, t).setParent(this), "")
      case e: Estimator[_] =>
        val t0 = System.nanoTime()
        val fitE = e.fit(dataset)
        val t1 = System.nanoTime()
        (new TimerModel(uid, fitE).setParent(this), formatTime(t1 - t0, false, countBeforeVal))
    }
  }

  def fit(dataset: Dataset[_]): TimerModel = {
    val (model, message) = fitWithTime(dataset)
    log(message)
    model
  }

}

object TimerModel extends DefaultReadable[TimerModel] {
  override val typesToRead = List(classOf[String], classOf[Transformer])
  override val ttag: ClassTag[TimerModel] = classTag[TimerModel]
}

class TimerModel(val uid: String, t: Transformer)
  extends Model[TimerModel] with TimerParams with DefaultWritable[TimerModel]{

  override val objectsToSave = List(uid, t)
  override val ttag: ClassTag[TimerModel] = classTag[TimerModel]

  override def copy(extra: ParamMap): TimerModel = {
    new TimerModel(uid, t.copy(extra)).setParent(parent)
  }

  def transformWithTime(dataset: Dataset[_]): (DataFrame, String) = {
    val cachedDatasetBefore = if (getCacheBefore) dataset.cache() else dataset
    val countBeforeVal = if (getCountBefore) Some(dataset.count()) else None

    val t0 = System.nanoTime()

    val res = t.transform(cachedDatasetBefore)

    val cachedDatasetAfter = if (getCacheBefore) res.cache() else res
    val countAfterVal = if (getCountBefore) Some(cachedDatasetAfter.count()) else None

    val t1 = System.nanoTime()

    (cachedDatasetAfter, formatTime(t1 - t0, true, countAfterVal))
  }

  def transformSchema(schema: StructType): StructType = t.transformSchema(schema)

  def transform(dataset: Dataset[_]): DataFrame = {
    val (model, message) = transformWithTime(dataset)
    log(message)
    model
  }
}
