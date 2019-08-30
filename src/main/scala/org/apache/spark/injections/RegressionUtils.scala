// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.injections

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.regression.Regressor

object RegressionUtils {

  def isRegressor(stage: PipelineStage): Boolean = {
    stage match {
      case _: Regressor[_, _, _] => true
      case _ => false
    }
  }

}
