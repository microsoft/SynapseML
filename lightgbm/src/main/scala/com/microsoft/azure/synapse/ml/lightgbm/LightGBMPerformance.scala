// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm

import com.microsoft.azure.synapse.ml.lightgbm.InstrumentationMeasures.getInterval

/**
  * Class for encapsulating performance instrumentation measures of each partition Task.
  */
class TaskInstrumentationMeasures(val partitionId: Int) extends Serializable {
  private val startTime = System.currentTimeMillis()
  private var initializationStart: Long = 0
  private var initializationStop: Long = 0
  private var libraryInitializationStart: Long = 0
  private var libraryInitializationStop: Long = 0
  private var networkInitializationStart: Long = 0
  private var networkInitializationStop: Long = 0
  private var samplingStart: Long = 0
  private var samplingStop: Long = 0
  private var dataPreparationStart: Long = 0
  private var dataPreparationStop: Long = 0
  private var waitStart: Long = 0
  private var waitStop: Long = 0
  private var datasetCreationStart: Long = 0
  private var datasetCreationStop: Long = 0
  private var validationDatasetCreationStart: Long = 0
  private var validationDatasetCreationStop: Long = 0
  private var trainingIterationsStart: Long = 0
  private var trainingIterationsStop: Long = 0
  private var cleanupStart: Long = 0
  private var cleanupStop: Long = 0
  private var endTime: Long = 0

  var isActiveTrainingTask: Boolean = false // TODO make private?

  def markInitializationStart(): Unit = { initializationStart = System.currentTimeMillis() }
  def markInitializationStop(): Unit = { initializationStop = System.currentTimeMillis() }
  def markLibraryInitializationStart(): Unit = { libraryInitializationStart = System.currentTimeMillis() }
  def markLibraryInitializationStop(): Unit = { libraryInitializationStop = System.currentTimeMillis() }
  def markNetworkInitializationStart(): Unit = { networkInitializationStart = System.currentTimeMillis() }
  def markNetworkInitializationStop(): Unit = { networkInitializationStop = System.currentTimeMillis() }
  def markSamplingStart(): Unit = { samplingStart = System.currentTimeMillis() }
  def markSamplingStop(): Unit = { samplingStop = System.currentTimeMillis() }
  def markDataPreparationStart(): Unit = { dataPreparationStart = System.currentTimeMillis() }
  def markDataPreparationStop(): Unit = { dataPreparationStop = System.currentTimeMillis() }
  def markWaitStart(): Unit = { waitStart = System.currentTimeMillis() }
  def markWaitStop(): Unit = { waitStop = System.currentTimeMillis() }
  def markDatasetCreationStart(): Unit = { datasetCreationStart = System.currentTimeMillis() }
  def markDatasetCreationStop(): Unit = { datasetCreationStop = System.currentTimeMillis() }
  def markValidationDatasetStart(): Unit = { validationDatasetCreationStart = System.currentTimeMillis() }
  def markValidationDatasetStop(): Unit = { validationDatasetCreationStop = System.currentTimeMillis() }
  def markTrainingIterationsStart(): Unit = { trainingIterationsStart = System.currentTimeMillis() }
  def markTrainingIterationsStop(): Unit = { trainingIterationsStop = System.currentTimeMillis() }
  def markCleanupStart(): Unit = { cleanupStart = System.currentTimeMillis() }
  def markCleanupStop(): Unit = { cleanupStop = System.currentTimeMillis() }
  def markTaskEnd(): Unit = { endTime = System.currentTimeMillis() }

  def initializationTime: Long = getInterval(initializationStart, initializationStop)
  def libraryInitializationTime: Long = getInterval(libraryInitializationStart, libraryInitializationStop)
  def networkInitializationTime: Long = getInterval(networkInitializationStart, networkInitializationStop)
  def dataPreparationTime: Long = getInterval(dataPreparationStart, dataPreparationStop)
  def waitTime: Long = getInterval(waitStart, waitStop)
  def datasetCreationTime: Long = getInterval(datasetCreationStart, datasetCreationStop)
  def validationDatasetCreationTime: Long = getInterval(validationDatasetCreationStart, validationDatasetCreationStop)
  def trainingIterationsTime: Long = getInterval(trainingIterationsStart, trainingIterationsStop)
  def cleanupTime: Long = getInterval(cleanupStart, cleanupStop)
  def totalTime: Long = getInterval(startTime, endTime)
  def overheadTime: Long = {
    (totalTime
      - initializationTime
      - dataPreparationTime
      - waitTime
      - datasetCreationTime
      - validationDatasetCreationTime
      - trainingIterationsTime)
  }
}

object InstrumentationMeasures {
  def getInterval(start: Long, stop: Long): Long = {
    // always return 0 if no stop time given, and always return at least 1 if there is any internal at all
    if (stop == 0) 0 else Math.max(1, stop - start)
  }
}

/**
  * Class for encapsulating performance instrumentation measures of overall training.
  */
class InstrumentationMeasures() extends Serializable {
  private val startTime = System.currentTimeMillis()
  private var validationDataCollectionStart: Long = 0
  private var validationDataCollectionStop: Long = 0
  private var columnStatisticsStart: Long = 0
  private var columnStatisticsStop: Long = 0
  private var rowStatisticsStart: Long = 0
  private var rowStatisticsStop: Long = 0
  private var rowCountsStart: Long = 0
  private var rowCountsStop: Long = 0
  private var samplingStart: Long = 0
  private var samplingStop: Long = 0
  private var trainingStart: Long = 0
  private var trainingStop: Long = 0
  private var endTime: Long = 0

  private var taskMeasures: Option[Seq[TaskInstrumentationMeasures]] = None

  def setTaskMeasures(taskStats: Seq[TaskInstrumentationMeasures]): Unit = { taskMeasures = Option(taskStats) }

  def markValidDataCollectionStart(): Unit = { validationDataCollectionStart = System.currentTimeMillis() }
  def markValidDataCollectionStop(): Unit = { validationDataCollectionStop = System.currentTimeMillis() }
  def markColumnStatisticsStart(): Unit = { columnStatisticsStart = System.currentTimeMillis() }
  def markColumnStatisticsStop(): Unit = { columnStatisticsStop = System.currentTimeMillis() }
  def markRowStatisticsStart(): Unit = { rowStatisticsStart = System.currentTimeMillis() }
  def markRowStatisticsStop(): Unit = { rowStatisticsStop = System.currentTimeMillis() }
  def markRowCountsStart(): Unit = { rowCountsStart = System.currentTimeMillis() }
  def markRowCountsStop(): Unit = { rowCountsStop = System.currentTimeMillis() }
  def markSamplingStart(): Unit = { samplingStart = System.currentTimeMillis() }
  def markSamplingStop(): Unit = { samplingStop = System.currentTimeMillis() }
  def markTrainingStart(): Unit = { trainingStart = System.currentTimeMillis() }
  def markTrainingStop(): Unit = { trainingStop = System.currentTimeMillis() }
  def markExecutionEnd(): Unit = { endTime = System.currentTimeMillis() }

  def getTaskMeasures: Seq[TaskInstrumentationMeasures] = { taskMeasures.get }

  def validationDataCollectionTime(): Long = getInterval(validationDataCollectionStart, validationDataCollectionStop)
  def columnStatisticsTime(): Long = getInterval(columnStatisticsStart, columnStatisticsStop)
  def rowStatisticsTime(): Long = getInterval(rowStatisticsStart, rowStatisticsStop)
  def rowCountTime(): Long = getInterval(rowCountsStart, rowCountsStop)
  def samplingTime(): Long = getInterval(samplingStart, samplingStop)
  def trainingTime(): Long = getInterval(trainingStart, trainingStop)
  def totalTime: Long = getInterval(startTime, endTime)

  def overheadTime: Long = { (totalTime
    - columnStatisticsTime()
    - rowStatisticsTime()
    - trainingTime()) }

  def taskInitializationTimes(): Seq[Long] = getTaskMeasureSeq(measures => measures.initializationTime)
  def taskLibraryInitializationTimes(): Seq[Long] = getTaskMeasureSeq(measures => measures.libraryInitializationTime)
  def taskNetworkInitializationTimes(): Seq[Long] = getTaskMeasureSeq(measures => measures.networkInitializationTime)
  def taskDataPreparationTimes(): Seq[Long] = getTaskMeasureSeq(measures => measures.dataPreparationTime)
  def taskWaitTimes(): Seq[Long] = getTaskMeasureSeq(measures => measures.waitTime)
  def taskDatasetCreationTimes(): Seq[Long] = getTaskMeasureSeq(measures => measures.datasetCreationTime)
  def taskValidDatasetCreationTimes(): Seq[Long] = getTaskMeasureSeq(measures => measures.validationDatasetCreationTime)
  def taskTrainingIterationTimes(): Seq[Long] = getTaskMeasureSeq(measures => measures.trainingIterationsTime)
  def taskCleanupTimes(): Seq[Long] = getTaskMeasureSeq(measures => measures.cleanupTime)
  def taskTotalTimes(): Seq[Long] = getTaskMeasureSeq(measures => measures.totalTime)
  def taskOverheadTimes(): Seq[Long] = getTaskMeasureSeq(measures => measures.overheadTime)

  private def getTaskMeasureSeq(f: TaskInstrumentationMeasures => Long) = {
    if (taskMeasures.isDefined) taskMeasures.get.map(f) else Seq()
  }
}

trait LightGBMPerformance extends Serializable {
  private var performanceMeasures: Option[Array[Option[InstrumentationMeasures]]] = None

  protected def initPerformanceMeasures(batchCount: Int): Unit = {
    performanceMeasures = Option(Array.fill(batchCount)(None))
  }

  protected def setBatchPerformanceMeasure(index: Int, measures: InstrumentationMeasures): Unit = {
    performanceMeasures.get(index) = Option(measures)
  }

  protected def setBatchPerformanceMeasures(measures: Array[Option[InstrumentationMeasures]]): this.type = {
    // TODO throw if already set?
    performanceMeasures = Option(measures)
    this
  }

  def getAllPerformanceMeasures: Option[Array[InstrumentationMeasures]] = {
    performanceMeasures.map(array => array.flatten)
  }

  /**
    * In the common case of 1 batch, there is only 1 measure, so this is a convenience method.
    */
  def getPerformanceMeasures: Option[InstrumentationMeasures] = {
    performanceMeasures.flatMap(array => array(0))
  }
}
