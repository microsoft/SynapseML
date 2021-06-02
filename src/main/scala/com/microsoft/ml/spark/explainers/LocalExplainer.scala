package com.microsoft.ml.spark.explainers

import org.apache.spark.ml.param.Params
import org.apache.spark.ml.param.shared.HasOutputCol
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

trait LocalExplainer
  extends Params with Serializable with HasExplainTarget with HasOutputCol with HasModel {

  val spark: SparkSession = SparkSession.active

  final def setOutputCol(value: String): this.type = this.set(outputCol, value)

  def explain(instances: Dataset[_]): DataFrame

  protected def validateSchema(inputSchema: StructType): Unit = {
    if (inputSchema.fieldNames.contains(getOutputCol)) {
      throw new IllegalArgumentException(s"Input schema already has column $getOutputCol")
    }
  }
}

object LocalExplainer {
  object LIME {
    def tabular: TabularLIME = {
      new TabularLIME()
    }

    def vector: VectorLIME = {
      new VectorLIME()
    }

    def image: ImageLIME = {
      new ImageLIME()
    }

    def text: TextLIME = {
      new TextLIME()
    }
  }

  object KernelSHAP {
    def tabular: TabularSHAP = {
      new TabularSHAP()
    }

    def vector: VectorSHAP = {
      new VectorSHAP()
    }

    def image: ImageSHAP = {
      new ImageSHAP()
    }

    def text: TextSHAP = {
      new TextSHAP()
    }
  }
}