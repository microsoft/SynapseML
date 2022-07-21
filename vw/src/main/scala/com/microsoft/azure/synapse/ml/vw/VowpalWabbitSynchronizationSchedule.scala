// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.core.utils.ClusterUtil
import com.microsoft.azure.synapse.ml.vw.VowpalWabbitSynchronizationScheduleTemporal.ordering
import org.apache.commons.lang.time.DateUtils
import org.apache.spark.TaskContext
import org.apache.spark.sql.{DataFrame, Row, functions => F}

import java.io.Serializable
import java.time.Instant
import java.time.temporal.{ChronoField, ChronoUnit, Temporal, TemporalAdjuster, TemporalAdjusters}
import java.util.{Calendar, Date}

trait VowpalWabbitSynchronizationSchedule extends Serializable {
  def getAllReduceTriggerCount(row: Row): Int

  def getFinishTriggerCount(): Int
}

class VowpalWabbitSynchronizationScheduleDisabled extends VowpalWabbitSynchronizationSchedule {
  override def getAllReduceTriggerCount(row: Row): Int = 0

  def getFinishTriggerCount(): Int = 0
}

object VowpalWabbitSynchronizationSchedule
{
  lazy val Disabled = new VowpalWabbitSynchronizationScheduleDisabled
}

abstract class VowpalWabbitSynchronizationScheduleBase[T: Ordering] extends VowpalWabbitSynchronizationSchedule {
  protected def getNextSyncValue(current: T): T

  protected def getInitialNextSyncValue(): T

  protected def getDifference(value: T, nextSyncValue: T): Int

  // need a level of indirection to get per-partition value
  class ValueHolder(var value: T)

  @transient
  lazy val nextSyncValue = new ValueHolder(getInitialNextSyncValue())

  @transient
  lazy val lastSyncValue = new ValueHolder(getInitialNextSyncValue())

  protected def getRowValue(row: Row): T

  protected def getAllReduceTriggerCount(value: T): Int = {
    if (Ordering[T].lt(value, nextSyncValue.value))
      0
    else {
      val lastSyncValueX = nextSyncValue.value
      nextSyncValue.value = getNextSyncValue(value)

      getDifference(lastSyncValueX, nextSyncValue.value)
    }
  }

  def getAllReduceTriggerCount(row: Row): Int = getAllReduceTriggerCount(getRowValue(row))
}

object VowpalWabbitSynchronizationScheduleTemporal {
  implicit def ordering[A <: Temporal]: Ordering[A] = Ordering.by(_.getLong(ChronoField.INSTANT_SECONDS))
}

class VowpalWabbitSynchronizationScheduleTemporal (df: DataFrame,
                                                   scheduleCol: String,
                                                   stepUnit: ChronoUnit,
                                                   // stepAdjuster: TemporalAdjuster,
                                                   stepSize: Int,
                                                   startAndEndDate: Option[(Instant, Instant)] = None)
  extends VowpalWabbitSynchronizationScheduleBase[Instant] {

  assert(stepSize > 0, "stepSize must be greater than zero")

  val (startDateVal, endDateVal) =
    startAndEndDate.getOrElse {
      // compute min/max timestamp
      val row = df.agg(F.min(F.col(scheduleCol)), F.max(F.col(scheduleCol))).head
      (row.getTimestamp(0).toInstant, row.getTimestamp(1).toInstant)
    }

  val scheduleColIdx = df.schema.fieldIndex(scheduleCol)

  override protected def getDifference(value: Instant, nextSyncValue: Instant): Int =
    stepUnit.between(value, nextSyncValue).toInt / stepSize

  override protected def getNextSyncValue(current: Instant): Instant =
    stepUnit.addTo(current, stepSize)

  override protected def getInitialNextSyncValue(): Instant = getNextSyncValue(startDateVal)

  override protected def getRowValue(row: Row): Instant = row.getTimestamp(scheduleColIdx).toInstant

  override def getFinishTriggerCount(): Int = getAllReduceTriggerCount(endDateVal)
}

class VowpalWabbitSynchronizationScheduleSplits (df: DataFrame,
                                                 numSplits: Integer)
  extends VowpalWabbitSynchronizationScheduleBase[Long] {

  assert(numSplits > 0, "Number of splits must be greater than zero")

  val rowsPerPartitions = ClusterUtil.getNumRowsPerPartition(df, F.lit(0))

  val stepSizePerPartition = rowsPerPartitions.map { c => Math.ceil(c / numSplits.toDouble).toLong }

  override def getFinishTriggerCount(): Int = 0

  // row count must be continuous. no need to do fancy difference for skipping
  override protected def getDifference(value: Long, nextSyncValue: Long): Int = 1
    // ((nextSyncValue - value - 1) / stepSizePerPartition(TaskContext.getPartitionId())).toInt

  override protected def getNextSyncValue(current: Long) =
    current + stepSizePerPartition(TaskContext.getPartitionId())

  override protected def getInitialNextSyncValue(): Long = stepSizePerPartition(TaskContext.getPartitionId())

  @transient
  var rowCount = 0

  override protected def getRowValue(row: Row): Long = {
    val ret = rowCount
    rowCount += 1
    ret
  }
}
