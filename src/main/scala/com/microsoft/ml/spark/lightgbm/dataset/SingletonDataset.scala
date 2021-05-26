// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.lightgbm.dataset

import java.util.concurrent.CountDownLatch

import com.microsoft.ml.spark.io.http.SharedSingleton
import com.microsoft.ml.spark.lightgbm.ColumnParams
import org.apache.spark.sql.types.StructType

object SingletonDataset {
  var IsSparse: SharedSingleton[Option[Boolean]] = SharedSingleton { None }
  def linkIsSparse(isSparse: Boolean): Unit = {
    this.synchronized {
      if (IsSparse.get == None) {
        IsSparse = SharedSingleton {
          Some(isSparse)
        }
      }
    }
  }

  var DoneSignal: CountDownLatch = new CountDownLatch(0)
  def incrementDoneSignal(): Unit = {
    this.synchronized {
      val count = DoneSignal.getCount().toInt
      DoneSignal = new CountDownLatch(count + 1)
    }
  }

  var DenseDatasetState: SharedSingleton[Option[DenseDatasetAggregator]] = SharedSingleton { None }
  def setupDenseDatasetState(columnParams: ColumnParams, chunkSize: Int, numCols: Int, schema: StructType): Unit = {
    this.synchronized {
      if (DenseDatasetState.get == None) {
        DenseDatasetState = SharedSingleton {
          Some(new DenseDatasetAggregator(columnParams, chunkSize, numCols, schema, true))
        }
      }
    }
  }

  var SparseDatasetState: SharedSingleton[Option[SparseDatasetAggregator]] = SharedSingleton { None }
  def setupSparseDatasetState(): Unit = {
    this.synchronized {
      if (SparseDatasetState.get == None) {
        SparseDatasetState = SharedSingleton {
          Some(new SparseDatasetAggregator(true))
        }
      }
    }
  }

  def resetSingletonDatasetState(): Unit = {
    IsSparse = SharedSingleton { None }
    DoneSignal = new CountDownLatch(0)
    DenseDatasetState = SharedSingleton { None }
    SparseDatasetState = SharedSingleton { None }
  }
}
