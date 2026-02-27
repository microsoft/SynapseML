// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.dataset

import com.microsoft.azure.synapse.ml.lightgbm.swig.SwigUtils
import com.microsoft.azure.synapse.ml.lightgbm._
import com.microsoft.ml.lightgbm._
import org.apache.spark.sql._
import org.slf4j.Logger


// scalastyle:off method.length
object ReferenceDatasetUtils {
  def createReferenceDatasetFromSample(datasetParams: String,
                                       featuresCol: String,
                                       numRows: Long,
                                       numCols: Int,
                                       sampledRowData: Array[Row],
                                       featureNames: Option[Array[String]],
                                       measures: InstrumentationMeasures,
                                       log: Logger): Array[Byte] = {
    log.info(s"Creating reference training dataset with ${sampledRowData.length} samples and config: $datasetParams")

    val lenPtr = lightgbmlib.new_intp()
    val bufferHandlePtr = lightgbmlib.voidpp_handle()
    val sampledData = SampledData(sampledRowData.length, numCols)

    try {
      measures.markSamplingStart()
      sampledRowData.zipWithIndex.foreach({case (row, index) => sampledData.pushRow(row, index, featuresCol)})
      measures.markSamplingStop()

      val datasetHandle = createDatasetFromSamples(sampledData, numCols, numRows, datasetParams)
      setFeatureNamesIfProvided(datasetHandle, featureNames, numCols, log)
      serializeAndCleanup(datasetHandle, bufferHandlePtr, lenPtr, log)
    } finally {
      sampledData.delete()
      lightgbmlib.delete_voidpp(bufferHandlePtr)
      lightgbmlib.delete_intp(lenPtr)
    }
  }
  // scalastyle:on method.length

  private def createDatasetFromSamples(sampledData: SampledData,
                                       numCols: Int,
                                       numRows: Long,
                                       datasetParams: String): SWIGTYPE_p_void = {
    val datasetVoidPtr = lightgbmlib.voidpp_handle()
    LightGBMUtils.validate(lightgbmlib.LGBM_DatasetCreateFromSampledColumn(
      sampledData.getSampleData,
      sampledData.getSampleIndices,
      numCols,
      sampledData.getRowCounts,
      sampledData.numRows,
      1, // Used for allocation and must be > 0, but we don't use this reference set for data collection
      numRows,
      datasetParams,
      datasetVoidPtr), "Dataset create from samples")
    lightgbmlib.voidpp_value(datasetVoidPtr)
  }

  private def setFeatureNamesIfProvided(datasetHandle: SWIGTYPE_p_void,
                                        featureNames: Option[Array[String]],
                                        numCols: Int,
                                        log: Logger): Unit = {
    featureNames.foreach { names =>
      if (names.nonEmpty) {
        log.info(s"Setting ${names.length} feature names on reference dataset")
        LightGBMUtils.validate(lightgbmlib.LGBM_DatasetSetFeatureNames(datasetHandle, names, numCols),
          "Dataset set feature names")
      }
    }
  }

  private def serializeAndCleanup(datasetHandle: SWIGTYPE_p_void,
                                  bufferHandlePtr: SWIGTYPE_p_p_void,
                                  lenPtr: SWIGTYPE_p_int,
                                  log: Logger): Array[Byte] = {
    LightGBMUtils.validate(lightgbmlib.LGBM_DatasetSerializeReferenceToBinary(
      datasetHandle, bufferHandlePtr, lenPtr), "Serialize ref")
    val bufferLen: Int = lightgbmlib.intp_value(lenPtr)
    log.info(s"Created serialized reference dataset of length $bufferLen")
    LightGBMUtils.validate(lightgbmlib.LGBM_DatasetFree(datasetHandle), "Free Dataset")
    toByteArray(bufferHandlePtr, bufferLen)
  }

  def getInitializedReferenceDataset(ctx: PartitionTaskContext): LightGBMDataset = {
    // The definition is broadcast from Spark, so retrieve it
    val serializedDataset: Array[Byte] = ctx.trainingCtx.serializedReferenceDataset.get

    // Convert byte array to actual dataset
    val count = ctx.executorRowCount
    val datasetParams = ctx.trainingCtx.datasetParams
    val lightGBMDataset = deserializeReferenceDataset(
      serializedDataset,
      count,
      datasetParams)

    // Initialize the dataset for streaming (allocates arrays mostly)
    val maxOmpThreads = ctx.trainingParams.executionParams.maxStreamingOMPThreads
    LightGBMUtils.validate(lightgbmlib.LGBM_DatasetInitStreaming(lightGBMDataset.datasetPtr,
      ctx.trainingCtx.hasWeightsAsInt,
      ctx.trainingCtx.hasInitialScoresAsInt,
      ctx.trainingCtx.hasGroupsAsInt,
      ctx.trainingParams.getNumClass,
      ctx.executorPartitionCount,
      maxOmpThreads),
      "LGBM_DatasetInitStreaming")

    lightGBMDataset.setFeatureNames(ctx.trainingCtx.featureNames, ctx.trainingCtx.numCols)
  }

  private def toByteArray(buffer: SWIGTYPE_p_p_void, bufferLen: Int): Array[Byte] = {
    val byteArray = new Array[Byte](bufferLen)
    val valPtr = lightgbmlib.new_bytep()
    val bufferHandle = lightgbmlib.voidpp_value(buffer)

    try
    {
      (0 until bufferLen).foreach { i =>
        LightGBMUtils.validate(lightgbmlib.LGBM_ByteBufferGetAt(bufferHandle, i, valPtr), "Buffer get-at")
        byteArray(i) = lightgbmlib.bytep_value(valPtr).toByte
      }
    }
    finally
    {
      // We assume once converted to byte array we should clean up the native memory and buffer
      lightgbmlib.delete_bytep(valPtr)
      LightGBMUtils.validate(lightgbmlib.LGBM_ByteBufferFree(bufferHandle), "Buffer free")
    }

    byteArray
  }

  private def deserializeReferenceDataset(serializedDataset: Array[Byte],
                                          rowCount: Int,
                                          datasetParams: String): LightGBMDataset = {
    // Convert byte array to native memory
    val datasetVoidPtr = lightgbmlib.voidpp_handle()
    val nativeByteArray = SwigUtils.byteArrayToNative(serializedDataset)
    LightGBMUtils.validate(lightgbmlib.LGBM_DatasetCreateFromSerializedReference( //scalastyle:ignore token
      lightgbmlib.byte_to_voidp_ptr(nativeByteArray),
      serializedDataset.length,
      rowCount,
      0, // Always zero since we will be using InitStreaming to do allocation
      datasetParams,
      datasetVoidPtr), "Dataset create from reference")

    val datasetPtr: SWIGTYPE_p_void = lightgbmlib.voidpp_value(datasetVoidPtr)
    lightgbmlib.delete_voidpp(datasetVoidPtr)
    new LightGBMDataset(datasetPtr)
  }
}
