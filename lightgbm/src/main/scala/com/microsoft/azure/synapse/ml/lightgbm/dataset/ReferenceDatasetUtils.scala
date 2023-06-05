// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.dataset

import com.microsoft.azure.synapse.ml.lightgbm.swig.SwigUtils
import com.microsoft.azure.synapse.ml.lightgbm._
import com.microsoft.ml.lightgbm._
import org.apache.spark.sql._
import org.slf4j.Logger


object ReferenceDatasetUtils {
  def createReferenceDatasetFromSample(datasetParams: String,
                                       featuresCol: String,
                                       numCols: Int,
                                       sampledRowData: Array[Row],
                                       measures: InstrumentationMeasures,
                                       log: Logger): Array[Byte] = {
    log.info(s"Loading sample data from broadcast with ${sampledRowData.length} samples")

    // Pre-create allocated native pointers so it's easy to clean them up
    val datasetVoidPtr = lightgbmlib.voidpp_handle()
    val lenPtr = lightgbmlib.new_intp()
    val bufferHandlePtr = lightgbmlib.voidpp_handle()

    val sampledData: SampledData = new SampledData(sampledRowData.length, numCols)
    try {
      // create properly formatted sampled data
      measures.markSamplingStart()
      sampledRowData.zipWithIndex.foreach(rowAndIndex =>
        sampledData.pushRow(rowAndIndex._1, rowAndIndex._2, featuresCol))
      measures.markSamplingStop()

      // Create dataset from samples
      log.info(s"Creating reference training dataset, config:$datasetParams")
      // Generate the dataset for features
      val datasetVoidPtr = lightgbmlib.voidpp_handle()
      LightGBMUtils.validate(lightgbmlib.LGBM_DatasetCreateFromSampledColumn(
        sampledData.getSampleData,
        sampledData.getSampleIndices,
        numCols,
        sampledData.getRowCounts,
        sampledData.numRows,
        1, // TODO check OK
        1, // TODO check OK
        datasetParams,
        datasetVoidPtr), "Dataset create from samples")


      // Serialize the raw dataset to a native buffer
      val datasetHandle: SWIGTYPE_p_void = lightgbmlib.voidpp_value(datasetVoidPtr)
      LightGBMUtils.validate(lightgbmlib.LGBM_DatasetSerializeReferenceToBinary(
        datasetHandle,
        bufferHandlePtr,
        lenPtr), "Serialize ref")
      val bufferLen: Int = lightgbmlib.intp_value(lenPtr)
      log.info(s"Created serialized reference dataset of length $bufferLen")

      // The dataset is now serialized to a buffer, so we don't need original
      LightGBMUtils.validate(lightgbmlib.LGBM_DatasetFree(datasetHandle), "Free Dataset")

      // This will also free the buffer
      convertToByteArray(bufferHandlePtr, bufferLen)
    }
    finally {
      sampledData.delete()
      lightgbmlib.delete_voidpp(datasetVoidPtr)
      lightgbmlib.delete_voidpp(bufferHandlePtr)
      lightgbmlib.delete_intp(lenPtr)
    }
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
      0,
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
    lightGBMDataset
  }

  private def convertToByteArray(buffer: SWIGTYPE_p_p_void, bufferLen: Int): Array[Byte] = {
    val byteArray = new Array[Byte](bufferLen)
    val valPtr = lightgbmlib.new_bytep()
    val bufferHandle = lightgbmlib.voidpp_value(buffer)

    try
    {
      (0 until bufferLen).foreach(i => {
        LightGBMUtils.validate(lightgbmlib.LGBM_ByteBufferGetAt(bufferHandle, i, valPtr), "Buffer getat")
        byteArray(i) = lightgbmlib.bytep_value(valPtr).toByte
      })
    }
    finally
    {
      // We assume once converte to byte array we should clean up the native memory and buffer
      lightgbmlib.delete_bytep(valPtr)
      LightGBMUtils.validate(lightgbmlib.LGBM_ByteBufferFree(bufferHandle), "Buffer free")
    }

    byteArray
  }

  private def deserializeReferenceDataset(serializedDataset: Array[Byte],
                                          rowCount: Int,
                                          numClasses: Int,
                                          datasetParams: String): LightGBMDataset = {
    // Convert byte array to native memory
    val datasetVoidPtr = lightgbmlib.voidpp_handle()
    val nativeByteArray = SwigUtils.byteArrayToNative(serializedDataset)
    LightGBMUtils.validate(lightgbmlib.LGBM_DatasetCreateFromSerializedReference( //scalastyle:ignore token
      lightgbmlib.byte_to_voidp_ptr(nativeByteArray),
      serializedDataset.length,
      rowCount,
      numClasses, // TODO should be 0?
      datasetParams,
      datasetVoidPtr), "Dataset create from reference")

    val datasetPtr: SWIGTYPE_p_void = lightgbmlib.voidpp_value(datasetVoidPtr)
    lightgbmlib.delete_voidpp(datasetVoidPtr)
    new LightGBMDataset(datasetPtr)
  }
}
