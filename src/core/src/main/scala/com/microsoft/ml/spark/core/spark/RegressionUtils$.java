object RegressionUtils {

  def isRegressor(stage: PipelineStage): Boolean = {
    stage match {
      case _: Regressor[_, _, _] => true
      case default => false
    }
  }

}
