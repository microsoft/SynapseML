// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml

import com.microsoft.ml.spark.LassoUtils
import org.apache.spark.ml.linalg.{DenseMatrix, DenseVector}

object LimeNamespaceInjections {

  def fitLasso(x: DenseMatrix, y: DenseVector, lambda: Double): DenseVector = {
    val result = LassoUtils.lasso(x.asBreeze.toDenseMatrix, y.asBreeze.toDenseVector, lambda)
    new DenseVector(result.coefficients.data)
  }

}
