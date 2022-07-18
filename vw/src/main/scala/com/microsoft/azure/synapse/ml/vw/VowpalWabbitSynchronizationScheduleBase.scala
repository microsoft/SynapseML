package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.core.utils.ClusterUtil
import org.apache.commons.lang.time.DateUtils
import org.apache.spark.TaskContext
import org.apache.spark.sql.{DataFrame, Row, functions => F}

import java.io.Serializable
import java.util.{Calendar, Date}

trait VowpalWabbitSynchronizationSchedule extends Serializable {
  def shouldTriggerAllReduce(row: Row): Boolean
}

class VowpalWabbitSynchronizationScheduleDisabled extends VowpalWabbitSynchronizationSchedule {
  override def shouldTriggerAllReduce(row: Row): Boolean = false
}

object VowpalWabbitSynchronizationSchedule
{
  lazy val Disabled = new VowpalWabbitSynchronizationScheduleDisabled
}

abstract class VowpalWabbitSynchronizationScheduleBase[T: Ordering] extends VowpalWabbitSynchronizationSchedule {
  protected def getNextSyncValue(current: T): T

  protected def getInitialSyncValue(): T

  // need a level of indirection to get per-partition value
  class ValueHolder
  {
    var value: T = getInitialSyncValue()
  }

  @transient
  lazy val nextSyncValue = new ValueHolder

  protected def getRowValue(row: Row): T

  def shouldTriggerAllReduce(row: Row): Boolean = {
    val value = getRowValue(row)

//    println(s"shouldTrigger $value < ${nextSyncValue.value} in part ${TaskContext.getPartitionId()}")

    if (Ordering[T].lt(value, nextSyncValue.value))
      false
    else {
      nextSyncValue.value = getNextSyncValue(value)
      true
    }
  }
}

// This needs some more thought as it only works if the data contains the repsective timesteps
// we'd have to account for skips
//class VowpalWabbitSynchronizationScheduleTemporal (df: DataFrame,
//                                                   scheduleCol: String,
//                                                   calendarField: Integer,
//                                                   stepSize: Integer)
//  extends VowpalWabbitSynchronizationSchedule[java.util.Date] {
//
//  assert(stepSize > 0, "stepSize must be greater than zero")
//
//  val (minVal, maxVal) = {
//    val row = df.agg(F.min(F.col(scheduleCol)), F.max(F.col(scheduleCol))).head
//    row.getTimestamp(0)
//    (row.getTimestamp(0), row.getTimestamp(1))
//  }
//
//  @transient
//  val calendar = Calendar.getInstance
//
//  override protected def getNextSyncValue(current: java.util.Date) = {
//    val truncated = DateUtils.truncate(current, Calendar.DATE)
//
//    calendar.setTime(truncated)
//    calendar.add(calendarField, stepSize)
//
//    calendar.getTime
//    //    val nextTime = calendar.getTime
//    // Ordering[java.util.Date].max(nextTime, maxVal)
//  }
//
//  override protected def getInitialSyncValue(): Date = getNextSyncValue(minVal)
//
//  val scheduleColIdx = df.schema.fieldIndex(scheduleCol)
//  override protected def getRowValue(row: Row): Date = row.getTimestamp(scheduleColIdx)
//}

class VowpalWabbitSynchronizationScheduleSplits (df: DataFrame,
                                                 numSplits: Integer)
  extends VowpalWabbitSynchronizationScheduleBase[Long] {

  assert(numSplits > 0, "Number of splits must be greater than zero")

  val rowsPerPartitions = ClusterUtil.getNumRowsPerPartition(df, F.lit(0))

  val stepSizePerPartition = rowsPerPartitions.map { c => Math.ceil(c / numSplits.toDouble).toLong }

//  println(s"rowsPerPartitions: ${rowsPerPartitions.mkString(",")}")
//  println(s"stepSizePerPartition: ${stepSizePerPartition.mkString(",")}")

  override protected def getNextSyncValue(current: Long) =
    current + stepSizePerPartition(TaskContext.getPartitionId())

  // TODO: log?
  override protected def getInitialSyncValue(): Long = stepSizePerPartition(TaskContext.getPartitionId())

  @transient
  var rowCount = 0

  override protected def getRowValue(row: Row): Long = {
//    println(s"getRowValue part: ${TaskContext.get.partitionId} rowCount: $rowCount")

    val ret = rowCount
    rowCount += 1
    ret
  }
}
