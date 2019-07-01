package org.apache.spark.injections

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.regression.Regressor

object RegressionUtils {

  def isRegressor(stage: PipelineStage): Boolean = {
    stage match {
      case _: Regressor[_, _, _] => true
      case default => false
    }
  }

}
