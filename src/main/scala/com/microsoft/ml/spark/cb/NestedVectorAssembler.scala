// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cb

import com.microsoft.ml.spark.core.contracts.Wrappable
import com.microsoft.ml.spark.core.env.{InternalWrapper, StreamUtilities}
import com.microsoft.ml.spark.core.schema.DatasetExtensions
import org.apache.spark.ml.{BaseRegressor, ComplexParamsReadable, ComplexParamsWritable, Estimator, Model, PredictionModel, Predictor, Transformer}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.ml.classification.{ProbabilisticClassificationModel, ProbabilisticClassifier}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, element_at, lit, udf, when}
import org.vowpalwabbit.spark.VowpalWabbitExample
import com.microsoft.ml.spark.core.schema.DatasetExtensions._
import org.apache.spark.ml.param.shared.HasProbabilityCol
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.StructType

import scala.collection.mutable
import scala.math.exp

object NestedVectorAssembler extends ComplexParamsReadable[NestedVectorAssembler]

@InternalWrapper
class NestedVectorAssembler(override val uid: String)
  extends Transformer with Wrappable with ComplexParamsWritable {

  def this() = this(Identifiable.randomUID("NestedVectorAssembler"))

  override def copy(extra: ParamMap): NestedVectorAssembler = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = schema

  private def appendDenseToSparse(source: DenseVector,
                          indices: mutable.ArrayBuilder[Int],
                          values: mutable.ArrayBuilder[Double],
                          featureIndex: Int = 0) = {
    source.foreachActive { case (i, v) =>
      if (v != 0.0) {
        indices += featureIndex + i
        values += v
      }
    }
  }

  private def appendSparseToSparse(source: SparseVector,
                                 indices: mutable.ArrayBuilder[Int],
                                 values: mutable.ArrayBuilder[Double],
                                 featureIndex: Int = 0) = {
    if (featureIndex == 0)
      indices ++= source.indices
    else {
      for (idx <- source.indices)
          indices += featureIndex + idx
    }

    values ++= source.values
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val schema = dataset.schema

    val encoder = RowEncoder(schema)
    val sharedIdx = schema.fieldIndex("shared")
    val featuresIdx = schema.fieldIndex("features")

    dataset.toDF.mapPartitions(it => {
      it.map(r => {
        val shared = r.getAs[Vector](sharedIdx)
        val features = r.getAs[Seq[Vector]](featuresIdx)

        // this mimics VectorAssembler
        val mergedFeatures = features.map(actionFeatures => {
          val indices = mutable.ArrayBuilder.make[Int]
          val values = mutable.ArrayBuilder.make[Double]

          indices.sizeHint(shared.size + actionFeatures.size)
          values.sizeHint(shared.size + actionFeatures.size)

          shared match {
            case dv: DenseVector => appendDenseToSparse(dv, indices, values)
            case sv: SparseVector => appendSparseToSparse(sv, indices, values)
          }

          actionFeatures match {
            case dv: DenseVector => appendDenseToSparse(dv, indices, values, shared.size)
            case sv: SparseVector => appendSparseToSparse(sv, indices, values, shared.size)
          }

          Vectors.sparse(shared.size + actionFeatures.size, indices.result(), values.result()).compressed
        })

        {
          val n = r.length
          val values = new Array[Any](n)
          var i = 0
          while (i < n) {
            values.update(i, r.get(i))
            i += 1
          }

          values.update(featuresIdx, mergedFeatures)

          // upcast to match type
          new GenericRow(values).asInstanceOf[Row]
        }
      })
    })(encoder)
  }
}

