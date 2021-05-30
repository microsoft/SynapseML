package com.microsoft.ml.spark.explainers
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

trait KernelSHAPParams extends HasNumSamples with HasMetricsCol {
  self: KernelSHAPBase =>

  setDefault(metricsCol -> "r2")
}

class KernelSHAPBase(override val uid: String) extends LocalExplainer with KernelSHAPParams {
  import spark.implicits._

  override def explain(instances: Dataset[_]): DataFrame = ???

  override def copy(extra: ParamMap): Params = defaultCopy(extra)

  protected override def validateSchema(schema: StructType): Unit = {
    super.validateSchema(schema)

    // TODO: extract the following check to the HasMetricsCol trait
    require(
      !schema.fieldNames.contains(getMetricsCol),
      s"Input schema (${schema.simpleString}) already contains metrics column $getMetricsCol"
    )
  }

  private def getNumSamples(numFeature: Int): Int = {
    val maxSamplesNeeded = math.pow(2, numFeature)
    math.min(this.getNumSamplesOpt.getOrElse(2 * numFeature + 2048), maxSamplesNeeded).toInt
  }
}
