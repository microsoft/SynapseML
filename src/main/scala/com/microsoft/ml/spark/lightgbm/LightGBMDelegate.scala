// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.lightgbm

import com.microsoft.ml.lightgbm.SWIGTYPE_p_void
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.StructType
import org.slf4j.Logger

trait LightGBMDelegate extends Serializable {
  def beforeTrainBatch(batchIndex: Int, log: Logger, dataset: Dataset[_],
                       previousBooster: Option[LightGBMBooster]): Unit = {
    // override this function and write code
  }

  def afterTrainBatch(batchIndex: Int, log: Logger, dataset: Dataset[_],
                      booster: LightGBMBooster): Unit = {
    // override this function and write code
  }

  def beforeGenerateTrainDataset(batchIndex: Int, partitionId: Int, columnParams: ColumnParams, schema: StructType,
                                 log: Logger, trainParams: TrainParams): Unit = {
    // override this function and write code
  }

  def afterGenerateTrainDataset(batchIndex: Int, partitionId: Int, columnParams: ColumnParams, schema: StructType,
                                log: Logger, trainParams: TrainParams): Unit = {
    // override this function and write code
  }

  def beforeGenerateValidDataset(batchIndex: Int, partitionId: Int, columnParams: ColumnParams, schema: StructType,
                                 log: Logger, trainParams: TrainParams): Unit = {
    // override this function and write code
  }

  def afterGenerateValidDataset(batchIndex: Int, partitionId: Int, columnParams: ColumnParams, schema: StructType,
                                log: Logger, trainParams: TrainParams): Unit = {
    // override this function and write code
  }

  def beforeTrainIteration(batchIndex: Int, partitionId: Int, curIters: Int, log: Logger,
                           trainParams: TrainParams, boosterPtr: Option[SWIGTYPE_p_void], hasValid: Boolean): Unit = {
    // override this function and write code
  }

  def afterTrainIteration(batchIndex: Int, partitionId: Int, curIters: Int, log: Logger,
                          trainParams: TrainParams, boosterPtr: Option[SWIGTYPE_p_void], hasValid: Boolean,
                          isFinished: Boolean,
                          trainEvalResults: Option[Map[String, Double]],
                          validEvalResults: Option[Map[String, Double]]): Unit = {
    // override this function and write code
  }

  def getLearningRate(batchIndex: Int, partitionId: Int, curIters: Int, log: Logger, trainParams: TrainParams,
                      previousLearningRate: Double): Double = {
    // override this function and write code
    previousLearningRate
  }
}
