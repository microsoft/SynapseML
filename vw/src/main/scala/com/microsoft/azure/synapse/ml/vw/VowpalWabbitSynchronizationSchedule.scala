// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.core.utils.ClusterUtil
import org.apache.spark.TaskContext
import org.apache.spark.sql.{DataFrame, Row, functions => F}

import java.io.Serializable

/**
  * Defines when VW needs to synchonize across partitions.
  */
trait VowpalWabbitSynchronizationSchedule extends Serializable {
  /**
    * Implementations must guarantee to trigger the same number of times across all partitions.
    *
    * @param row passed in to enable content passed schedules (e.g. temporal)
    * @return True if this partition needs to synchronize, false otherwise.
    */
  def shouldTriggerAllReduce(row: Row): Boolean
}

class VowpalWabbitSynchronizationScheduleDisabled extends VowpalWabbitSynchronizationSchedule {
  override def shouldTriggerAllReduce(row: Row): Boolean = false
}

object VowpalWabbitSynchronizationSchedule
{
  lazy val Disabled = new VowpalWabbitSynchronizationScheduleDisabled
}

class VowpalWabbitSynchronizationScheduleSplits (df: DataFrame,
                                                 numSplits: Integer)
  extends VowpalWabbitSynchronizationSchedule {

  assert(numSplits > 0, "Number of splits must be greater than zero")

  val rowsPerPartitions = ClusterUtil.getNumRowsPerPartition(df, F.lit(0))

  val stepSizePerPartition = rowsPerPartitions.map { c => c / numSplits.toDouble }

  lazy val rowCount = rowsPerPartitions(TaskContext.getPartitionId())

  @transient
  lazy val stepSize = {
    val s = stepSizePerPartition(TaskContext.getPartitionId())

    assert (s > 1, s"Number of splits $numSplits > ${rowCount}")

    Math.ceil(s).toLong
  }

  lazy val needToSyncOnLastRow = stepSize * numSplits != rowCount

  @transient
  var i = 0

  override def shouldTriggerAllReduce(row: Row): Boolean = {
    i += 1

    if (i % stepSize == 0)
      true
    else {
      // let's make sure even and odd partitions have the same number synchronizations
      needToSyncOnLastRow && i == rowCount
    }
  }
}
