// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.lightgbm

import com.microsoft.ml.lightgbm.{SWIGTYPE_p_int, SWIGTYPE_p_void, lightgbmlib, lightgbmlibConstants}
import com.microsoft.ml.spark.core.utils.ClusterUtil
import com.microsoft.ml.spark.io.http.SharedSingleton
import com.microsoft.ml.spark.lightgbm.ConnectionState.Finished
import com.microsoft.ml.spark.lightgbm.LightGBMUtils.{closeConnections, getDatasetParams, handleConnection, sendDataToExecutors}
import com.microsoft.ml.spark.lightgbm.TaskTrainingMethods.{isWorkerEnabled, prepareDatasets}
import com.microsoft.ml.spark.lightgbm.TrainUtils._
import com.microsoft.ml.spark.lightgbm.booster.LightGBMBooster
import com.microsoft.ml.spark.lightgbm.dataset.DatasetUtils.addGroupColumn
import com.microsoft.ml.spark.lightgbm.dataset._
import com.microsoft.ml.spark.lightgbm.params._
import com.microsoft.ml.spark.logging.BasicLogging
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.attribute._
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.param.shared.{HasFeaturesCol => HasFeaturesColSpark, HasLabelCol => HasLabelColSpark}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.types._
import org.apache.spark.sql._

import java.net.{ServerSocket, Socket}
import java.util.concurrent.Executors
import scala.collection.immutable.HashSet
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.{Duration, SECONDS}
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.language.existentials
import scala.math.min
import scala.util.matching.Regex

trait LightGBMBase[TrainedModel <: Model[TrainedModel]] extends Estimator[TrainedModel]
  with LightGBMParams with HasFeaturesColSpark with HasLabelColSpark with BasicLogging {

  /** Trains the LightGBM model.  If batches are specified, breaks training dataset into batches for training.
    *
    * @param dataset The input dataset to train.
    * @return The trained model.
    */
  protected def train(dataset: Dataset[_]): TrainedModel = {
    logTrain({
      if (getNumBatches > 0) {
        val ratio = 1.0 / getNumBatches
        val datasets = dataset.randomSplit((0 until getNumBatches).map(_ => ratio).toArray)
        datasets.zipWithIndex.foldLeft(None: Option[TrainedModel]) { (model, datasetWithIndex) =>
          if (model.isDefined) {
            setModelString(stringFromTrainedModel(model.get))
          }

          val dataset = datasetWithIndex._1
          val batchIndex = datasetWithIndex._2

          beforeTrainBatch(batchIndex, dataset, model)

          val newModel = innerTrain(dataset, batchIndex)
          afterTrainBatch(batchIndex, dataset, newModel)

          Some(newModel)
        }.get
      } else {
        innerTrain(dataset, batchIndex = 0)
      }
    })
  }

  def beforeTrainBatch(batchIndex: Int, dataset: Dataset[_], model: Option[TrainedModel]): Unit = {
    if (getDelegate.isDefined) {
      val previousBooster: Option[LightGBMBooster] = model match {
        case Some(_) => Option(new LightGBMBooster(stringFromTrainedModel(model.get)))
        case None => None
      }

      getDelegate.get.beforeTrainBatch(batchIndex, log, dataset, previousBooster)
    }
  }

  def afterTrainBatch(batchIndex: Int, dataset: Dataset[_], model: TrainedModel): Unit = {
    if (getDelegate.isDefined) {
      val booster = new LightGBMBooster(stringFromTrainedModel(model))
      getDelegate.get.afterTrainBatch(batchIndex, log, dataset, booster)
    }
  }

  protected def castColumns(dataset: Dataset[_], trainingCols: Array[(String, Seq[DataType])]): DataFrame = {
    val schema = dataset.schema
    // Cast columns to correct types
    dataset.select(
      trainingCols.map {
        case (name, datatypes: Seq[DataType]) => {
          val index = schema.fieldIndex(name)
          // Note: We only want to cast if original column was of numeric type

          schema.fields(index).dataType match {
            case numericDataType: NumericType =>
              // If more than one datatype is allowed, see if any match
              if (datatypes.contains(numericDataType)) {
                dataset(name)
              } else {
                // If none match, cast to the first option
                dataset(name).cast(datatypes.head)
              }

            case _ => dataset(name)
          }
        }
      }: _*
    ).toDF()
  }

  protected def prepareDataframe(dataset: Dataset[_], numTasks: Int): DataFrame = {
    val df = castColumns(dataset, getTrainingCols)
    // Reduce number of partitions to number of executor tasks
    /* Note: with barrier execution mode we must use repartition instead of coalesce when
     * running on spark standalone.
     * Using coalesce, we get the error:
     *
     * org.apache.spark.scheduler.BarrierJobUnsupportedRDDChainException:
     * [SPARK-24820][SPARK-24821]: Barrier execution mode does not allow the following
     * pattern of RDD chain within a barrier stage:
     * 1. Ancestor RDDs that have different number of partitions from the resulting
     * RDD (eg. union()/coalesce()/first()/take()/PartitionPruningRDD). A workaround
     * for first()/take() can be barrierRdd.collect().head (scala) or barrierRdd.collect()[0] (python).
     * 2. An RDD that depends on multiple barrier RDDs (eg. barrierRdd1.zip(barrierRdd2)).
     *
     * Without repartition, we may hit the error:
     * org.apache.spark.scheduler.BarrierJobSlotsNumberCheckFailed: [SPARK-24819]:
     * Barrier execution mode does not allow run a barrier stage that requires more
     * slots than the total number of slots in the cluster currently. Please init a
     * new cluster with more CPU cores or repartition the input RDD(s) to reduce the
     * number of slots required to run this barrier stage.
     *
     * Hence we still need to estimate the number of tasks and repartition even when using
     * barrier execution, which is unfortunate as repartition is more expensive than coalesce.
     */
    if (getUseBarrierExecutionMode) {
      val numPartitions = df.rdd.getNumPartitions
      if (numPartitions > numTasks) {
        df.repartition(numTasks)
      } else {
        df
      }
    } else {
      df.coalesce(numTasks)
    }
  }

  protected def getTrainingCols: Array[(String, Seq[DataType])] = {
    val colsToCheck: Array[(Option[String], Seq[DataType])] = Array(
      (Some(getLabelCol), Seq(DoubleType)),
      (Some(getFeaturesCol), Seq(VectorType)),
      (get(weightCol), Seq(DoubleType)),
      (getOptGroupCol, Seq(IntegerType, LongType, StringType)),
      (get(validationIndicatorCol), Seq(BooleanType)),
      (get(initScoreCol), Seq(DoubleType, VectorType)))

    colsToCheck.flatMap {
      case (col: Option[String], colType: Seq[DataType]) =>
        if (col.isDefined) Some(col.get, colType) else None
    }
  }

  /**
    * Retrieves the categorical indexes in the features column.
    *
    * @param featuresSchema The schema of the features column
    * @return the categorical indexes in the features column.
    */
  def getCategoricalIndexes(featuresSchema: StructField): Array[Int] = {
    val categoricalColumnSlotNames = get(categoricalSlotNames).getOrElse(Array.empty[String])
    val categoricalIndexes = if (getSlotNames.nonEmpty) {
      categoricalColumnSlotNames.map(getSlotNames.indexOf(_))
    } else {
      val categoricalSlotNamesSet = HashSet(categoricalColumnSlotNames: _*)
      val metadata = AttributeGroup.fromStructField(featuresSchema)
      if (metadata.attributes.isEmpty) Array[Int]()
      else {
        metadata.attributes.get.zipWithIndex.flatMap {
          case (null, _) => Iterator()
          case (attr, idx) =>
            if (attr.name.isDefined && categoricalSlotNamesSet.contains(attr.name.get)) {
              Iterator(idx)
            } else {
              attr match {
                case _: NumericAttribute | UnresolvedAttribute => Iterator()
                // Note: it seems that BinaryAttribute is not considered categorical,
                // since all OHE cols are marked with this, but StringIndexed are always Nominal
                case _: BinaryAttribute => Iterator()
                case _: NominalAttribute => Iterator(idx)
              }
            }
        }
      }
    }

    get(categoricalSlotIndexes)
      .getOrElse(Array.empty[Int])
      .union(categoricalIndexes).distinct
  }


  private def validateSlotNames(schema: StructType, trainParams: TrainParams): Unit = {
    val featuresSchema = schema.fields(schema.fieldIndex(getFeaturesCol))
    val metadata = AttributeGroup.fromStructField(featuresSchema)
    if (metadata.attributes.isDefined) {
      val slotNamesOpt = DatasetUtils.getSlotNames(
        schema(getColumnParams.featuresColumn), metadata.attributes.get.length, trainParams)
      val pattern = new Regex("[\",:\\[\\]{}]")
      slotNamesOpt.foreach(slotNames => {
        val badSlotNames = slotNames.flatMap(slotName =>
          if (pattern.findFirstIn(slotName).isEmpty) None else Option(slotName))
        if (!badSlotNames.isEmpty) {
          val errorMsg = s"Invalid slot names detected in features column: ${badSlotNames.mkString(",")}"
          throw new IllegalArgumentException(errorMsg)
        }
      })
    }
  }

  /**
    * Constructs the DartModeParams
    *
    * @return DartModeParams object containing parameters related to dart mode.
    */
  protected def getDartParams: DartModeParams = {
    DartModeParams(getDropRate, getMaxDrop, getSkipDrop, getXGBoostDartMode, getUniformDrop)
  }

  /**
    * Constructs the ExecutionParams.
    *
    * @return ExecutionParams object containing parameters related to LightGBM execution.
    */
  protected def getExecutionParams: ExecutionParams = {
    ExecutionParams(getChunkSize, getMatrixType, getNumThreads, getUseSingleDatasetMode)
  }

  protected def getColumnParams: ColumnParams = {
    ColumnParams(getLabelCol, getFeaturesCol, get(weightCol), get(initScoreCol), getOptGroupCol)
  }

  /**
    * Constructs the ObjectiveParams.
    *
    * @return ObjectiveParams object containing parameters related to the objective function.
    */
  protected def getObjectiveParams: ObjectiveParams = {
    ObjectiveParams(getObjective, if (isDefined(fobj)) Some(getFObj) else None)
  }

  private def indptrArrayIncrement(indptrArray: SWIGTYPE_p_int, indptrCount: Long): Unit = {
    // Update indptr array indexes in sparse matrix
    (1L until indptrCount).foreach { index =>
      val indptrPrevValue = lightgbmlib.intArray_getitem(indptrArray, index - 1)
      val indptrCurrValue = lightgbmlib.intArray_getitem(indptrArray, index)
      lightgbmlib.intArray_setitem(indptrArray, index, indptrPrevValue + indptrCurrValue)
    }
  }

  def generateDenseDataset(denseAggregatedColumns: BaseDenseAggregatedColumns,
                           referenceDataset: Option[LightGBMDataset],
                           featureNamesOpt: Option[Array[String]],
                           trainParams: TrainParams): LightGBMDataset = {
    val numRows = denseAggregatedColumns.getRowCount
    val numCols = denseAggregatedColumns.getNumCols
    val featuresArray = denseAggregatedColumns.getFeaturesArray.array
    val isRowMajor = 1
    val datasetOutPtr = lightgbmlib.voidpp_handle()
    val datasetParams = getDatasetParams(trainParams)
    val data64bitType = lightgbmlibConstants.C_API_DTYPE_FLOAT64
    try {
      // Generate the dataset for features
      LightGBMUtils.validate(lightgbmlib.LGBM_DatasetCreateFromMat(lightgbmlib.double_to_voidp_ptr(featuresArray),
        data64bitType, numRows, numCols,
        isRowMajor, datasetParams, referenceDataset.map(_.datasetPtr).orNull, datasetOutPtr),
        "Dataset create")
    } finally {
      lightgbmlib.delete_doubleArray(featuresArray)
    }
    val dataset = new LightGBMDataset(lightgbmlib.voidpp_value(datasetOutPtr))
    dataset.setFeatureNames(featureNamesOpt, numCols)
    dataset
  }

  /** Generates a sparse dataset in CSR format.
    *
    * @return The constructed and wrapped native LightGBMDataset.
    */
  def generateSparseDataset(sac: BaseSparseAggregatedColumns,
                            referenceDataset: Option[LightGBMDataset],
                            featureNamesOpt: Option[Array[String]],
                            trainParams: TrainParams): LightGBMDataset = {
    val numCols: Long = sac.getNumCols
    val indptrLength: Long = sac.getIndptrCount
    val valuesLength: Long = sac.getIndexesCount
    val values: SWIGTYPE_p_void = lightgbmlib.double_to_voidp_ptr(sac.getValuesArray.array)
    val indexes: SWIGTYPE_p_int = sac.getIndexesArray.array
    val indptr: SWIGTYPE_p_void = lightgbmlib.int_to_voidp_ptr(sac.getIndptrArray.array)

    val datasetOutPtr = lightgbmlib.voidpp_handle()
    val datasetParams = getDatasetParams(trainParams)
    val dataInt32bitType = lightgbmlibConstants.C_API_DTYPE_INT32
    val data64bitType = lightgbmlibConstants.C_API_DTYPE_FLOAT64
    // Generate the dataset for features
    LightGBMUtils.validate(lightgbmlib.LGBM_DatasetCreateFromCSR(indptr,
      dataInt32bitType, indexes, values, data64bitType,
      indptrLength, valuesLength,
      numCols, datasetParams, referenceDataset.map(_.datasetPtr).orNull,
      datasetOutPtr),
      "Dataset create")
    val dataset = new LightGBMDataset(lightgbmlib.voidpp_value(datasetOutPtr))
    dataset.setFeatureNames(featureNamesOpt, numCols.toInt)
    dataset
  }

  def aggregateStreamedData(ac: BaseAggregatedColumns,
                            referenceDataset: Option[LightGBMDataset], schema: StructType,
                            trainParams: TrainParams): Option[LightGBMDataset] = {
    val numCols = ac.getNumCols
    val numRows = ac.getRowCount
    val slotNames = DatasetUtils.getSlotNames(schema(getFeaturesCol), numCols, trainParams)
    try {
      val dataset: Option[LightGBMDataset] = ac match {
        case sac: BaseSparseAggregatedColumns =>
          indptrArrayIncrement(sac.getIndptrArray.array, sac.getIndptrCount)
          Some(generateSparseDataset(sac, referenceDataset, slotNames, trainParams))
        case dac: BaseDenseAggregatedColumns =>
         Some(generateDenseDataset(dac, referenceDataset, slotNames, trainParams))
      }
      dataset.get.addFloatField(ac.getLabels.array, "label", numRows)
      ac.getWeights.foreach(weightArray =>
        dataset.get.addFloatField(weightArray.array, "weight", numRows))
      ac.getInitScores.foreach(initScoreArray =>
        dataset.get.addDoubleField(initScoreArray.array, "init_score", numRows))
      addGroupColumn(ac.getGroups, getOptGroupCol, dataset, numRows, schema, Some(0))
      dataset
    } finally {
      ac.cleanup()
    }
  }

  def generateDataset(aggregatedColumns: BaseAggregatedColumns,
                      referenceDataset: Option[LightGBMDataset],
                      schema: StructType,
                      trainParams: TrainParams): Option[LightGBMDataset] = {
    val dataset = aggregateStreamedData(aggregatedColumns, referenceDataset, schema, trainParams)
    // Validate generated dataset has the correct number of rows and cols
    dataset.get.validateDataset()
    dataset
  }


  def translate(batchIndex: Int,
                validationData: Option[BaseAggregatedColumns],
                trainParams: TrainParams,
                returnBooster: Boolean,
                schema: StructType,
                aggregatedColumns: BaseAggregatedColumns): Iterator[LightGBMBooster] = {
    var trainDatasetOpt: Option[LightGBMDataset] = None
    var validDatasetOpt: Option[LightGBMDataset] = None
    val columnParams = getColumnParams
    try {
      beforeGenerateTrainDataset(batchIndex, columnParams, schema, log, trainParams)
      trainDatasetOpt = generateDataset(aggregatedColumns, None, schema, trainParams)
      afterGenerateTrainDataset(batchIndex, columnParams, schema, log, trainParams)

      if (validationData.isDefined) {
        beforeGenerateValidDataset(batchIndex, columnParams, schema, log, trainParams)
        validDatasetOpt = generateDataset(validationData.get, trainDatasetOpt, schema, trainParams)
        afterGenerateValidDataset(batchIndex, columnParams, schema, log, trainParams)
      }

      var boosterOpt: Option[LightGBMBooster] = None
      try {
        val booster = createBooster(trainParams, trainDatasetOpt.get, validDatasetOpt)
        boosterOpt = Some(booster)
        val bestIterResult = trainCore(batchIndex, trainParams, booster, log, validDatasetOpt.isDefined)
        if (returnBooster) {
          val model = booster.saveToString()
          val modelBooster = new LightGBMBooster(model)
          // Set best iteration on booster if hit early stopping criteria in trainCore
          bestIterResult.foreach(modelBooster.setBestIteration)
          Iterator.single(modelBooster)
        } else {
          Iterator.empty
        }
      } finally {
        // Free booster
        boosterOpt.foreach(_.freeNativeMemory())
      }
    } finally {
      // Free datasets
      trainDatasetOpt.foreach(_.close())
      validDatasetOpt.foreach(_.close())
    }
  }

  def trainLightGBM(batchIndex: Int,
                    networkParams: NetworkParams,
                    validationData: Option[Broadcast[Array[Row]]],
                    trainParams: TrainParams,
                    numTasksPerExec: Int,
                    schema: StructType,
                    sharedState: SharedState)
                   (inputRows: Iterator[Row]): Iterator[LightGBMBooster] = {
    val useSingleDatasetMode = trainParams.executionParams.useSingleDatasetMode
    val emptyPartition = !inputRows.hasNext
    val isEnabledWorker = if (!emptyPartition) isWorkerEnabled(trainParams, log, sharedState) else false
    // Initialize the native library
    LightGBMUtils.initializeNativeLibrary()
    // Initialize the network communication
    val (nodes, localListenPort) = getNetworkInfo(networkParams, numTasksPerExec, log, isEnabledWorker)
    if (emptyPartition) {
      log.warn("LightGBM task encountered empty partition, for best performance ensure no partitions empty")
      List[LightGBMBooster]().toIterator
    } else {
      if (isEnabledWorker) {
        log.info(s"LightGBM task listening on: $localListenPort")
        if (useSingleDatasetMode) sharedState.helperStartSignal.countDown()
      } else {
        sharedState.helperStartSignal.await()
      }
      val (aggregatedColumns, aggregatedValidationColumns) = prepareDatasets(
        inputRows, validationData, sharedState)
      // Return booster only from main worker to reduce network communication overhead
      val returnBooster = getReturnBooster(isEnabledWorker, nodes, log, numTasksPerExec, localListenPort)
      try {
        if (isEnabledWorker) {
          // If worker enabled, initialize the network ring of communication
          networkInit(nodes, localListenPort, log, LightGBMConstants.NetworkRetries, LightGBMConstants.InitialDelay)
          if (useSingleDatasetMode) sharedState.doneSignal.await()
          translate(batchIndex, aggregatedValidationColumns, trainParams, returnBooster, schema, aggregatedColumns)
        } else {
          log.info("Helper task finished processing rows")
          sharedState.doneSignal.countDown()
          List[LightGBMBooster]().toIterator
        }
      } finally {
        // Finalize network when done
        if (isEnabledWorker) LightGBMUtils.validate(lightgbmlib.LGBM_NetworkFree(), "Finalize network")
      }
    }
  }

  /**
    * Opens a socket communications channel on the driver, starts a thread that
    * waits for the host:port from the executors, and then sends back the
    * information to the executors.
    *
    * @param numTasks The total number of training tasks to wait for.
    * @return The address and port of the driver socket.
    */
  def createDriverNodesThread(numTasks: Int, spark: SparkSession): (String, Int, Future[Unit]) = {
    // Start a thread and open port to listen on
    implicit val context: ExecutionContextExecutor =
      ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())
    val driverServerSocket = new ServerSocket(getDriverListenPort)
    // Set timeout on socket
    val duration = Duration(getTimeout, SECONDS)
    if (duration.isFinite()) {
      driverServerSocket.setSoTimeout(duration.toMillis.toInt)
    }
    val f = Future {
      var emptyTaskCounter = 0
      val hostAndPorts = ListBuffer[(Socket, String)]()
      if (getUseBarrierExecutionMode) {
        log.info(s"driver using barrier execution mode")

        def connectToWorkers: Boolean = handleConnection(driverServerSocket, log,
          hostAndPorts) == Finished || connectToWorkers

        connectToWorkers
      } else {
        log.info(s"driver expecting $numTasks connections...")
        while (hostAndPorts.size + emptyTaskCounter < numTasks) {
          val connectionResult = handleConnection(driverServerSocket, log, hostAndPorts)
          if (connectionResult == ConnectionState.EmptyTask) emptyTaskCounter += 1
        }
      }
      // Concatenate with commas, eg: host1:port1,host2:port2, ... etc
      val allConnections = hostAndPorts.map(_._2).mkString(",")
      log.info(s"driver writing back to all connections: $allConnections")
      // Send data back to all tasks and helper tasks on executors
      sendDataToExecutors(hostAndPorts, allConnections)
      closeConnections(log, hostAndPorts, driverServerSocket)
    }
    val host = ClusterUtil.getDriverHost(spark)
    val port = driverServerSocket.getLocalPort
    log.info(s"driver waiting for connections on host: $host and port: $port")
    (host, port, f)
  }

  /**
    * Inner train method for LightGBM learners.  Calculates the number of workers,
    * creates a driver thread, and runs mapPartitions on the dataset.
    *
    * @param dataset    The dataset to train on.
    * @param batchIndex In running in batch training mode, gets the batch number.
    * @return The LightGBM Model from the trained LightGBM Booster.
    */
  protected def innerTrain(dataset: Dataset[_], batchIndex: Int): TrainedModel = {
    val sc = dataset.sparkSession.sparkContext
    val numTasksPerExec = ClusterUtil.getNumTasksPerExecutor(dataset, log)
    // By default, we try to intelligently calculate the number of executors, but user can override this with numTasks
    val numTasks =
      if (getNumTasks > 0) getNumTasks
      else {
        val numExecutorTasks = ClusterUtil.getNumExecutorTasks(dataset.sparkSession, numTasksPerExec, log)
        min(numExecutorTasks, dataset.rdd.getNumPartitions)
      }
    val df = prepareDataframe(dataset, numTasks)

    val (inetAddress, port, future) = createDriverNodesThread(numTasks, df.sparkSession)

    /* Run a parallel job via map partitions to initialize the native library and network,
     * translate the data to the LightGBM in-memory representation and train the models
     */
    val encoder = Encoders.kryo[LightGBMBooster]

    val trainParams = getTrainParams(numTasks, dataset, numTasksPerExec)

    log.info(s"LightGBM parameters: ${trainParams.toString()}")
    val networkParams = NetworkParams(getDefaultListenPort, inetAddress, port, getUseBarrierExecutionMode)

    val (trainingData, validationData) =
      if (get(validationIndicatorCol).isDefined && dataset.columns.contains(getValidationIndicatorCol))
        (df.filter(x => !x.getBoolean(x.fieldIndex(getValidationIndicatorCol))),
          Some(sc.broadcast(preprocessData(df.filter(x =>
            x.getBoolean(x.fieldIndex(getValidationIndicatorCol)))).collect())))
      else (df, None)

    val preprocessedDF = preprocessData(trainingData)
    val schema = preprocessedDF.schema

    validateSlotNames(schema, trainParams)

    val sharedState = SharedSingleton(new SharedState(getColumnParams, schema, trainParams))
    val mapPartitionsFunc = trainLightGBM(batchIndex, networkParams,
      validationData, trainParams, numTasksPerExec, schema, sharedState.get)(_)

    val lightGBMBooster = if (getUseBarrierExecutionMode) {
      preprocessedDF.rdd.barrier().mapPartitions(mapPartitionsFunc).reduce((booster1, _) => booster1)
    } else {
      preprocessedDF.mapPartitions(mapPartitionsFunc)(encoder).reduce((booster1, _) => booster1)
    }

    // Wait for future to complete (should be done by now)
    Await.result(future, Duration(getTimeout, SECONDS))
    getModel(trainParams, lightGBMBooster)
  }

  /** Optional group column for Ranking, set to None by default.
    *
    * @return None
    */
  protected def getOptGroupCol: Option[String] = None

  /** Gets the trained model given the train parameters and booster.
    *
    * @return trained model.
    */
  protected def getModel(trainParams: TrainParams, lightGBMBooster: LightGBMBooster): TrainedModel

  /** Gets the training parameters.
    *
    * @param numTasks        The total number of tasks.
    * @param dataset         The training dataset.
    * @param numTasksPerExec The number of tasks per executor.
    * @return train parameters.
    */
  protected def getTrainParams(numTasks: Int, dataset: Dataset[_], numTasksPerExec: Int): TrainParams

  protected def stringFromTrainedModel(model: TrainedModel): String

  /** Allow algorithm specific preprocessing of dataset.
    *
    * @param dataset The dataset to preprocess prior to training.
    * @return The preprocessed data.
    */
  protected def preprocessData(dataset: DataFrame): DataFrame = dataset
}
