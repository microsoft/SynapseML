// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.causal

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.param.TransformerArrayParam
import com.microsoft.azure.synapse.ml.stages.DropColumns
import org.apache.commons.math3.stat.descriptive.rank.Percentile
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, Estimator, Model, Pipeline, Transformer}
import org.apache.spark.ml.regression.{DecisionTreeRegressionModel, RandomForestRegressor, Regressor}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.functions.vector_to_array
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

class ConstantColumns {
  val transformedOutcome = "_tmp_tsOutcome"
  val transformedWeights = "_tmp_twOutcome"
  val tempVecCol = "_tmp_combined_prediction"
  val tmpBLBBounds ="_tmp_blb_bounds"
  val tempForestPred = "_tmp_op_rf"
}

object ConstantColumns extends ConstantColumns

class OrthoForestDMLEstimator(override val uid: String)
  extends Estimator[OrthoForestDMLModel] with ComplexParamsWritable
    with OrthoForestDMLParams with Wrappable {

  def this() = this(Identifiable.randomUID("OrthoForestDMLEstimator"))

  /** Fits the OrthoForestDML model.
    *
    * @param dataset The input dataset to train.
    * @return The trained DoubleML model, from which you can get Ate and Ci values
    */
  override def fit(dataset: Dataset[_]): OrthoForestDMLModel = {

    require(getNumTrees > 0, "You got to have at least one tree in a forest")

    val treatmentColType = dataset.schema(getTreatmentCol).dataType
    require(treatmentColType == DoubleType,
      s"TreatmentCol must be of type DoubleType but got $treatmentColType")

    val forest = trainInternal(dataset)

    val dmlModel = this.copyValues(new OrthoForestDMLModel(uid))

    dmlModel.setForest(forest)
  }

  //scalastyle:off method.length
  private def trainInternal(dataset: Dataset[_]): Array[DecisionTreeRegressionModel] = {

    def getModel(model: Estimator[_ <: Model[_]],
                 labelColName: String,
                 featureColName: String)={
      model match {
        case regressor: Regressor[_, _, _] => (regressor
          .setFeaturesCol(featureColName)
          .setLabelCol(labelColName),
          regressor.getPredictionCol)
      }
    }


    val (treatmentEstimator, treatmentPredictionColName) = getModel(
      getTreatmentModel.copy(getTreatmentModel.extractParamMap()),
      getTreatmentCol,
      getConfounderVecCol
    )

    val (outcomeEstimator, outcomePredictionColName) = getModel(
      getOutcomeModel.copy(getOutcomeModel.extractParamMap()),
      getOutcomeCol,
      getConfounderVecCol
    )

    def calculateResiduals(train: Dataset[_], test: Dataset[_]): DataFrame = {
      val treatmentModel = treatmentEstimator.fit(train)
      val outcomeModel = outcomeEstimator.fit(train)

      val treatmentResidual =
        new ResidualTransformer()
          .setObservedCol(getTreatmentCol)
          .setPredictedCol(treatmentPredictionColName)
          .setOutputCol(getTreatmentResidualCol)
      val dropTreatmentPredictedColumns = new DropColumns()
        .setCols(Array(treatmentPredictionColName))

      val outcomeResidual =
        new ResidualTransformer()
          .setObservedCol(getOutcomeCol)
          .setPredictedCol(outcomePredictionColName)
          .setOutputCol(getOutcomeResidualCol)
      val dropOutcomePredictedColumns = new DropColumns()
        .setCols(Array(outcomePredictionColName))

      val pipeline = new Pipeline().setStages(Array(
        treatmentModel, treatmentResidual, dropTreatmentPredictedColumns,
        outcomeModel, outcomeResidual, dropOutcomePredictedColumns))

      pipeline.fit(test).transform(test)
    }

    // Note, we perform these steps to get ATE
    /*
      1. Split sample, e.g. 50/50
      2. Use the first split to fit the treatment model and the outcome model.
      3. Use the two models to fit a residual model on the second split.
      4. Cross-fit treatment and outcome models with the second split, residual model with the first split.
      5. Average slopes from the two residual models is eqiuivalent to fitting one tree
    */
    val splits = dataset.randomSplit(getSampleSplitRatio)
    val (train, test) = (splits(0).cache, splits(1).cache)
    val residualsDF1 = calculateResiduals(train, test)
    val residualsDF2 = calculateResiduals(test, train)

    val orthoPredTransformer = new OrthoForestVariableTransformer()
      .setTreatmentResidualCol(getTreatmentResidualCol)
      .setOutcomeResidualCol(getOutcomeResidualCol)
      .setOutputCol(ConstantColumns.transformedOutcome)
      .setWeightsCol(ConstantColumns.transformedWeights)

    def getTreesByFitting(residualDF: DataFrame): Array[DecisionTreeRegressionModel] = {
      val transformedDF = orthoPredTransformer.transform(residualDF)

      val rfRegressor = new RandomForestRegressor()
        .setFeaturesCol(getHeterogeneityVecCol)
        .setLabelCol(ConstantColumns.transformedOutcome)
        .setWeightCol(ConstantColumns.transformedWeights)
        .setPredictionCol(ConstantColumns.tempForestPred)
        .setMaxDepth(getMaxDepth)
        .setMinInstancesPerNode(getMinSamplesLeaf)

      val theForest = rfRegressor.fit(transformedDF)

      theForest.trees
    }

    val finalArray = Array.concat(getTreesByFitting(residualsDF1),
      getTreesByFitting(residualsDF2))

    Seq(train, test).foreach(_.unpersist)

    finalArray
  }

  override def copy(extra: ParamMap): Estimator[OrthoForestDMLModel] = {
    defaultCopy(extra)
  }

  override def transformSchema(schema: StructType): StructType = {
    OrthoForestDMLEstimator.validateTransformSchema(schema)
  }
}

object OrthoForestDMLEstimator extends ComplexParamsReadable[OrthoForestDMLEstimator] {

  def validateTransformSchema(schema: StructType): StructType = {
    StructType(schema.fields)
  }
}

/** Model produced by [[OrthoForestDMLEstimator]]. */
class OrthoForestDMLModel(val uid: String)
  extends Model[OrthoForestDMLModel] with OrthoForestDMLParams
    with ComplexParamsWritable with Wrappable{

  override protected lazy val pyInternalWrapper = false

  val forest = new TransformerArrayParam(this,
    "forest",
    "Forest Trees produced in Ortho Forest DML Estimator")

  private def getForest: Array[DecisionTreeRegressionModel] = {
    $(forest).map(x=>x.asInstanceOf[DecisionTreeRegressionModel])
  }

  def setForest(v: Array[DecisionTreeRegressionModel]): this.type = set(forest, v.map(x=>x.asInstanceOf[Transformer]))

  def this() = this(Identifiable.randomUID("OrthoForestDMLModel"))

  override def copy(extra: ParamMap): OrthoForestDMLModel = defaultCopy(extra)

  private def getBLBBounds(values: Array[Double]): Array[Double] = {

    val n = values.length
    val b = math.ceil(math.sqrt(n)).toInt
    val subSamples = values.toList.grouped(b).toList
    val r = 100

    def draw(x: List[Double]): Double = x(scala.util.Random.nextInt(x.length))

    val upsampledSorted = subSamples.map(x => List.fill(r)(draw(x)).sorted)

    def avg(x: List[Double]): Double = x.sum / x.length

    val ciLower = upsampledSorted.map(x => {

      val percentile = new Percentile()
      percentile.setData(x.toArray)

      percentile.evaluate(100 * (1 - getConfidenceLevel))
    })

    val ciHigher = upsampledSorted.map(x => {

      val percentile = new Percentile()
      percentile.setData(x.toArray)

      percentile.evaluate(100 * getConfidenceLevel)
    })

    val median = upsampledSorted.map(x => {

      val PERCENTILE_FOR_MEDIAN = 50

      val percentile = new Percentile()
      percentile.setData(x.toArray)

      //50 is the median value
      percentile.evaluate(PERCENTILE_FOR_MEDIAN)
    })

    Array(avg(ciLower), avg(median), avg(ciHigher))
  }

  override def transform(dataset: Dataset[_]): DataFrame = {

    val df = dataset.toDF()

    val cnt = getForest.length

    def predColIx(x: Any): String = {s"_tmp_op_$x"}

    var opCnt = 1
    for(tree<-getForest){
      tree.setPredictionCol(predColIx(opCnt))
      opCnt = opCnt + 1
    }

    val colsNamed = (1 to cnt).toArray.map{ x => predColIx(x) }

    val assembler = new VectorAssembler()
      .setInputCols(colsNamed)
      .setOutputCol(ConstantColumns.tempVecCol)

    val dropCols = new DropColumns()
      .setCols(colsNamed)

    val pipeline =  new Pipeline()
      .setStages(getForest ++ Array(assembler,dropCols))

    val getBLBBoundsUDF = udf { features: Array[Double] =>
      getBLBBounds(features)
    }

    val getLowerBound = udf { combined: Array[Double] => combined(0) }
    val getAverage = udf { combined: Array[Double] => combined(1) }
    val getUpperBound = udf { combined: Array[Double] => combined(2) }

    pipeline
      .fit(df)
      .transform(df)
      .withColumn(ConstantColumns.tmpBLBBounds, getBLBBoundsUDF(vector_to_array(col(ConstantColumns.tempVecCol))))
      .withColumn(getOutputLowCol, getLowerBound(col(ConstantColumns.tmpBLBBounds)))
      .withColumn(getOutputCol, getAverage(col(ConstantColumns.tmpBLBBounds)))
      .withColumn(getOutputHighCol, getUpperBound(col(ConstantColumns.tmpBLBBounds)))
      .drop(ConstantColumns.tempVecCol)
      .drop(ConstantColumns.tmpBLBBounds)
      .drop(ConstantColumns.transformedOutcome)
      .drop(ConstantColumns.transformedWeights)
  }

  override def transformSchema(schema: StructType): StructType =
    schema
      .add(StructField(getOutputCol, DoubleType, false))
      .add(StructField(getOutputLowCol, DoubleType, false))
      .add(StructField(getOutputHighCol, DoubleType, false))
}

object OrthoForestDMLModel extends ComplexParamsReadable[OrthoForestDMLModel]
