// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.causal

import breeze.linalg.{DenseVector => BDV}
import com.microsoft.azure.synapse.ml.causal.linalg.DVector
trait CacheOps[T] {
  def checkpoint(data: T): T = data
  def cache(data: T): T = data
}

object BDVCacheOps extends CacheOps[BDV[Double]] {
  override def checkpoint(data: BDV[Double]): BDV[Double] = data
  override def cache(data: BDV[Double]): BDV[Double] = data
}

object DVectorCacheOps extends CacheOps[DVector] {
  override def checkpoint(data: DVector): DVector = data.localCheckpoint(true)
  override def cache(data: DVector): DVector = data.cache
}
