// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm

/**
  * Class for encapsulating performance instrumentation measures.
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

  def initializationTime: Long = { if (initializationStop == 0) 0 else initializationStop - initializationStart }
  def libraryInitializationTime: Long = {
    if (libraryInitializationStop == 0) 0 else libraryInitializationStop - libraryInitializationStart
  }
  def networkInitializationTime: Long = {
    if (networkInitializationStop == 0) 0 else networkInitializationStop - networkInitializationStart
  }
  def dataPreparationTime: Long = { if (dataPreparationStop == 0) 0 else dataPreparationStop - dataPreparationStart }
  def waitTime: Long = { if (waitStop == 0) 0 else waitStop - waitStart }
  def datasetCreationTime: Long = { if (datasetCreationStop == 0) 0 else datasetCreationStop - datasetCreationStart }
  def validationDatasetCreationTime: Long = {
    if (validationDatasetCreationStop == 0) 0 else validationDatasetCreationStop - validationDatasetCreationStart
  }
  def trainingIterationsTime: Long = {
    if (trainingIterationsStop == 0) 0
    else Math.max(1, trainingIterationsStop - trainingIterationsStart)
  }
  def cleanupTime: Long = { if (cleanupStop == 0) 0 else cleanupStop - cleanupStart }
  def totalTime: Long = { if (endTime == 0) 0 else endTime - startTime }
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

  def validationDataCollectionTime(): Long = {
    if (validationDataCollectionStop == 0) 0
    else Math.max(1, validationDataCollectionStop - validationDataCollectionStart) // show at least 1 msec
  }
  def columnStatisticsTime(): Long = {
    if (columnStatisticsStop == 0) 0
    else Math.max(1, columnStatisticsStop - columnStatisticsStart)
  }
  def rowStatisticsTime(): Long = {
    if (rowStatisticsStop == 0) 0
    else Math.max(1, rowStatisticsStop - rowStatisticsStart) }
  def rowCountTime(): Long = {
    if (rowCountsStop == 0) 0
    else Math.max(1, rowCountsStop - rowCountsStart) }
  def samplingTime(): Long = {
    if (samplingStop == 0) 0
    else Math.max(1, samplingStop - samplingStart) }
  def trainingTime(): Long = {
    if (trainingStop == 0) 0
    else Math.max(1, trainingStop - trainingStart) }
  def totalTime: Long = { if (endTime == 0) 0 else endTime - startTime }

  def overheadTime: Long = { (totalTime
    - columnStatisticsTime()
    - rowStatisticsTime()
    - trainingTime()) }

  def taskInitializationTimes(): Seq[Long] = {
    if (taskMeasures.isDefined) taskMeasures.get.map(measures => measures.initializationTime) else Seq()
  }

  def taskLibraryInitializationTimes(): Seq[Long] = {
    if (taskMeasures.isDefined) taskMeasures.get.map(measures => measures.libraryInitializationTime) else Seq()
  }

  def taskNetworkInitializationTimes(): Seq[Long] = {
    if (taskMeasures.isDefined) taskMeasures.get.map(measures => measures.networkInitializationTime) else Seq()
  }

  def taskDataPreparationTimes(): Seq[Long] = {
    if (taskMeasures.isDefined) taskMeasures.get.map(measures => measures.dataPreparationTime) else Seq()
  }

  def taskWaitTimes(): Seq[Long] = {
    if (taskMeasures.isDefined) taskMeasures.get.map(measures => measures.waitTime) else Seq()
  }

  def taskDatasetCreationTimes(): Seq[Long] = {
    if (taskMeasures.isDefined) taskMeasures.get.map(measures => measures.datasetCreationTime) else Seq()
  }

  def taskValidDatasetCreationTimes(): Seq[Long] = {
    if (taskMeasures.isDefined) taskMeasures.get.map(measures => measures.validationDatasetCreationTime) else Seq()
  }

  def taskTrainingIterationTimes(): Seq[Long] = {
    if (taskMeasures.isDefined) taskMeasures.get.map(measures => measures.trainingIterationsTime) else Seq()
  }

  def taskCleanupTimes(): Seq[Long] = {
    if (taskMeasures.isDefined) taskMeasures.get.map(measures => measures.cleanupTime) else Seq()
  }

  def taskTotalTimes(): Seq[Long] = {
    if (taskMeasures.isDefined) taskMeasures.get.map(measures => measures.totalTime) else Seq()
  }

  def taskOverheadTimes(): Seq[Long] = {
    if (taskMeasures.isDefined) taskMeasures.get.map(measures => measures.overheadTime) else Seq()
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

  /** In the common case of 1 batch, there is only 1 measure, so this is a convenience method.
    *
    */
  def getPerformanceMeasures: Option[InstrumentationMeasures] = {
    performanceMeasures.flatMap(array => array(0))
  }
}
