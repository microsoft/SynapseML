package com.microsoft.ml.spark.explainers
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.sql.{DataFrame, Dataset}

trait KernelSHAPParams extends HasNumSamples with HasMetricsCol {
  self: KernelSHAPBase =>

  setDefault(metricsCol -> "r2")
}

class KernelSHAPBase(override val uid: String) extends LocalExplainer with KernelSHAPParams {
  import spark.implicits._

  override def explain(instances: Dataset[_]): DataFrame = ???

  override def copy(extra: ParamMap): Params = defaultCopy(extra)
}
