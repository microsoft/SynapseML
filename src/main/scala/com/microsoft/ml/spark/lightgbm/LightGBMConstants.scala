// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.lightgbm

object LightGBMConstants {
  /** The default port for LightGBM network initialization
    */
  val DefaultLocalListenPort = 12400
  /** The default timeout for LightGBM network initialization
    */
  val DefaultListenTimeout = 120
  /** Default buffer length for model
    */
  val DefaultBufferLength: Int = 10000
  /** Lambdarank ranking objective
    */
  val RankObjective: String = "lambdarank"
  /** Binary classification objective
    */
  val BinaryObjective: String = "binary"
  /** Multiclass classification objective
    */
  val MulticlassObjective: String = "multiclass"
  /** Ignore worker status, used to ignore workers that get empty partitions
    */
  val IgnoreStatus: String = "ignore"
  /** Barrier execution flag telling driver that all tasks have completed
    * sending port and host information
    */
  val FinishedStatus: String = "finished"
  /** Label column type or field name
    */
  val LabelCol: String = "label"
  /** Weight column type or field name
    */
  val WeightCol: String = "weight"
  /** Init score column type or field name
    */
  val InitScoreCol: String = "init_score"
}
