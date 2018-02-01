// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import com.microsoft.ml.lightgbm.{SWIGTYPE_p_void, lightgbmlib, lightgbmlibConstants}
import org.apache.commons.codec.StringEncoder
import org.apache.spark.TaskContext
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row}

import scala.collection.immutable.BitSet

/**
  * Helper utilities for LightGBM learners
  */
object LightGBMUtils {
  def validate(result: Int, component: String): Unit = {
    if (result == -1) {
      throw new Exception(component + " call failed in LightGBM with error: " + lightgbmlib.LGBM_GetLastError())
    }
  }

  /** Loads the native shared object binaries lib_lightgbm.so and lib_lightgbm_swig.so
    */
  def initializeNativeLibrary(): Unit = {
    new NativeLoader("/com/microsoft/ml/lightgbm").loadLibraryByName("_lightgbm")
    new NativeLoader("/com/microsoft/ml/lightgbm").loadLibraryByName("_lightgbm_swig")
  }

  def featurizeData(dataset: Dataset[_], labelColumn: String, featuresColumn: String): PipelineModel = {
    // Create pipeline model to featurize the dataset
    val oneHotEncodeCategoricals = true
    val featuresToHashTo = FeaturizeUtilities.numFeaturesTreeOrNNBased
    val featureColumns = dataset.columns.filter(col => col != labelColumn).toSeq
    val featurizer = new Featurize()
      .setFeatureColumns(Map(featuresColumn -> featureColumns))
      .setOneHotEncodeCategoricals(oneHotEncodeCategoricals)
      .setNumberOfFeatures(featuresToHashTo)
    featurizer.fit(dataset)
  }

  def getNodes(processedData: DataFrame, defaultListenPort: Int): (String, Int) = {
    val spark = processedData.sparkSession
    val nodesKeys2 = spark.sparkContext.getExecutorMemoryStatus.keys.map(key => key.split(":")(0))
    import spark.implicits._
    val nodes = processedData.mapPartitions((iter: Iterator[Row]) => {
      val ctx = TaskContext.get
      // The logic below is to get it to run in local[*] spark context
      val localHostAddr = java.net.InetAddress.getLocalHost().getHostAddress()
      val badAddr = Array("127.0.", "192.168.")
      val ipAddressPattern = """(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})"""
      val hostaddr =
        if (!badAddr.filter(localHostAddr.toString.startsWith(_)).isEmpty) {
          import java.net.NetworkInterface
          val interfaces = NetworkInterface.getNetworkInterfaces
          var done = false
          var goodAddr: String = null
          while (interfaces.hasMoreElements && !done) {
            val interface = interfaces.nextElement()
            val inetAddresses = interface.getInetAddresses
            while (inetAddresses.hasMoreElements && !done) {
              val inetaddress = inetAddresses.nextElement()
              goodAddr = inetaddress.getHostAddress
              if (badAddr.filter(goodAddr.startsWith(_)).isEmpty &&
                goodAddr.matches(ipAddressPattern)) {
                done = true
              }
            }
          }
          goodAddr
        } else localHostAddr
      val partId = ctx.partitionId()
      Array(hostaddr + ":" + (defaultListenPort + partId)).toIterator
    }).reduce((val1, val2) => val1 + "," + val2)
    (nodes, nodes.split(",").length)
  }

  def generateData(numRows: Int, rowsAsDoubleArray: Array[Array[Double]]): SWIGTYPE_p_void = {
    val numCols = rowsAsDoubleArray.head.length
    val data = lightgbmlib.new_doubleArray(numCols * numRows)
    rowsAsDoubleArray.zipWithIndex.foreach(ri =>
      ri._1.zipWithIndex.foreach(value =>
        lightgbmlib.doubleArray_setitem(data, value._2 + (ri._2 * numCols), value._1)))
    lightgbmlib.double_to_voidp_ptr(data)
  }

  def generateDataset(numRows: Int, rowsAsDoubleArray: Array[Array[Double]]): SWIGTYPE_p_void = {
    val numRowsIntPtr = lightgbmlib.new_intp()
    lightgbmlib.intp_assign(numRowsIntPtr, numRows)
    val numRows_int32_tPtr = lightgbmlib.int_to_int32_t_ptr(numRowsIntPtr)
    val numCols = rowsAsDoubleArray.head.length
    val isRowMajor = 1
    val numColsIntPtr = lightgbmlib.new_intp()
    lightgbmlib.intp_assign(numColsIntPtr, numCols)
    val numCols_int32_tPtr = lightgbmlib.int_to_int32_t_ptr(numColsIntPtr)
    val datasetOutPtr = lightgbmlib.voidpp_handle()
    val datasetParams = "max_bin=255 is_pre_partition=True"
    val data64bitType = lightgbmlibConstants.C_API_DTYPE_FLOAT64
    val dataAsVoidPtr = generateData(numRows, rowsAsDoubleArray)

    // Generate the dataset for features
    LightGBMUtils.validate(lightgbmlib.LGBM_DatasetCreateFromMat(dataAsVoidPtr, data64bitType,
      numRows_int32_tPtr, numCols_int32_tPtr, isRowMajor, datasetParams, null, datasetOutPtr), "Dataset create")
    lightgbmlib.voidpp_value(datasetOutPtr)
  }
}
