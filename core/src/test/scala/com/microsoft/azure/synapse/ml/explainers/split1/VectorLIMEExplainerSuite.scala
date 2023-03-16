// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.explainers.split1

import breeze.linalg.{*, DenseMatrix => BDM, DenseVector => BDV}
import breeze.stats.distributions.Rand
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import com.microsoft.azure.synapse.ml.core.utils.BreezeUtils._
import com.microsoft.azure.synapse.ml.explainers.LocalExplainer.LIME
import com.microsoft.azure.synapse.ml.explainers.VectorLIME
import org.apache.spark.ml.linalg.{DenseVector, SQLDataTypes, Vector, Vectors}
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, DataType}
import org.scalactic.Equality

class VectorLIMEExplainerSuite extends TestBase
  with TransformerFuzzing[VectorLIME] {

  import spark.implicits._

  implicit val vectorEquality: Equality[BDV[Double]] = breezeVectorEq(1E-6)

  // The equality comparison for the weights matrices are quite complicated,
  // creating an override here to handle the logic.
  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    assert(DataType.equalsStructurally(df1.schema, df2.schema))

    val arrayOfVectorFields = df1.schema.fields.filter {
      f => DataType.equalsStructurally(f.dataType, ArrayType(SQLDataTypes.VectorType))
    }.map(_.name)

    // Non-matrix columns, defer to super.assertDFEq
    val otherFields = df1.schema.fields.map(_.name).filterNot(arrayOfVectorFields.contains)

    super.assertDFEq(
      df1.select(otherFields.map(col): _*),
      df2.select(otherFields.map(col): _*)
    )(eq)

    val (a, b) = (df1.select(arrayOfVectorFields.map(col): _*), df2.select(arrayOfVectorFields.map(col): _*)) match {
      case (x, y) =>
        val sorted = if (sortInDataframeEquality) {
          (x.sort(arrayOfVectorFields.map(col): _*), y.sort(arrayOfVectorFields.map(col): _*))
        } else {
          (x, y)
        }

        (sorted._1.collect, sorted._2.collect)
    }

    assert(a.length === b.length)
    a.indices foreach { // For each row
      i =>
        arrayOfVectorFields.indices foreach {
          j => // For each matrix type column
            val x: Seq[DenseVector] = a(i).getAs[Seq[DenseVector]](j)
            val y: Seq[DenseVector] = b(i).getAs[Seq[DenseVector]](j)
            assert(x.length === y.length)
            x.indices foreach {
              k =>
                x(k) === y(k)
            }
        }
    }
  }

  lazy val d1 = 3
  lazy val d2 = 1

  lazy val coefficients: BDM[Double] = new BDM(d1, d2, Array(1.0, -1.0, 2.0))

  lazy val df: DataFrame = {
    val nRows = 100
    val intercept: Double = math.random()

    val x: BDM[Double] = BDM.rand(nRows, d1, Rand.gaussian)
    val y = x * coefficients + intercept

    val xRows = x(*, ::).iterator.toSeq.map(dv => Vectors.dense(dv.toArray))
    val yRows = y(*, ::).iterator.toSeq.map(dv => dv(0))
    xRows.zip(yRows).toDF("features", "label")
  }

  lazy val model: LinearRegressionModel = new LinearRegression().fit(df)

  lazy val lime: VectorLIME = LIME.vector
    .setModel(model)
    .setBackgroundData(df)
    .setInputCol("features")
    .setTargetCol(model.getPredictionCol)
    .setOutputCol("weights")
    .setNumSamples(1000)

  test("VectorLIME can explain a model locally") {
    val predicted = model.transform(df)
    val weights = lime
      .transform(predicted)
      .select("weights", "r2")
      .as[(Seq[Vector], Vector)]
      .collect()
      .map {
        case (m, _) => m.head.toBreeze
      }

    val weightsMatrix = BDM(weights: _*)
    weightsMatrix(*, ::).foreach {
      row =>
        assert(row === coefficients(::, 0))
    }
  }

  override def testObjects(): Seq[TestObject[VectorLIME]] = Seq(new TestObject(lime, df))

  override def reader: MLReadable[_] = VectorLIME
}
