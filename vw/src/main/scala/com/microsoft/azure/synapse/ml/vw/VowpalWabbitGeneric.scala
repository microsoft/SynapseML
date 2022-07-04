package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.env.StreamUtilities
import com.microsoft.azure.synapse.ml.logging.BasicLogging
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, Estimator, Model}
import org.apache.spark.ml.param.{Param, ParamMap, Params, StringArrayParam}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{col, struct, udf}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row}
import org.apache.spark.sql.types.{ArrayType, FloatType, IntegerType, ObjectType, StringType, StructField, StructType}
import org.vowpalwabbit.spark.VowpalWabbitNative
import org.vowpalwabbit.spark.prediction.ScalarPrediction
import vowpalWabbit.responses.{ActionProbs, ActionScores, DecisionScores, Multilabels}

trait HasInputCol extends Params {
  val inputCol = new Param[String](this, "inputCol", "The name of the input column")
  def setInputCol(value: String): this.type = set(inputCol, value)
  def getInputCol: String = $(inputCol)
}

class VowpalWabbitGeneric(override val uid: String) extends Estimator[VowpalWabbitGenericModel]
  with VowpalWabbitBase
  with HasInputCol
  with BasicLogging {
  logClass()

  override protected lazy val pyInternalWrapper = true

  def this() = this(Identifiable.randomUID("VowpalWabbitGeneric"))

  setDefault(inputCol -> "input")

  // Members declared in org.apache.spark.ml.Estimator
  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  // Members declared in com.microsoft.azure.synapse.ml.vw.VowpalWabbitBase
  protected def getInputColumns(): Seq[String] = Seq(getInputCol)

  protected def trainRow(schema: StructType,
                         inputRows: Iterator[Row],
                         ctx: TrainContext
                        ): Unit = {

    val featureIdx = schema.fieldIndex(getInputCol)

    // learn from all rows
    for (row <- inputRows) {
      // ingestion and learning is collapsed
      // TODO: should we also measure here?
      //  ctx.nativeIngestTime.measure {
      ctx.learnTime.measure {
        ctx.vw.learnFromString(row.getString(featureIdx))
      }
    }
  }

  def fit(dataset: org.apache.spark.sql.Dataset[_]): VowpalWabbitGenericModel = {
    // TODO: copied from predictor. should we merge train in here?
    copyValues(train(dataset).setParent(this))
  }

  def transformSchema(schema: StructType): StructType = {
    // TODO: validation
    schema
  }

  protected def train(dataset: Dataset[_]): VowpalWabbitGenericModel = {
    logTrain({
      val model = new VowpalWabbitGenericModel(uid)
        .setInputCol(getInputCol)

      trainInternal(dataset.toDF, model)
    })
  }
}

object VowpalWabbitGeneric extends ComplexParamsReadable[VowpalWabbitGeneric]

class VowpalWabbitGenericModel(override val uid: String)
  extends Model[VowpalWabbitGenericModel]
    with VowpalWabbitBaseModel
    with HasInputCol
    with ComplexParamsWritable with Wrappable with BasicLogging {
  logClass()

  override protected lazy val pyInternalWrapper = true

  def this() = this(Identifiable.randomUID("VowpalWabbitGenericModel"))

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  private def schemaForPredictionType(): StructType = {
    vw.getOutputPredictionType match {
      case "prediction_type_t::scalar" => StructType(Seq(
        StructField(getInputCol, StringType, false),
        StructField("prediction", FloatType, false),
          StructField("confidence", FloatType, false)))
      case "prediction_type_t::scalars" => StructType(Seq(
        StructField(getInputCol, StringType, false),
        StructField("predictions", ArrayType(FloatType), false)
      ))
      case "prediction_type_t::action_scores" => StructType(Seq(
        StructField(getInputCol, StringType, false),
        StructField("predictions", ArrayType(StructType(Seq(
          StructField("action", IntegerType, false),
          StructField("score", FloatType, false)
        ))))
      ))
      case "prediction_type_t::action_probs" => StructType(Seq(
        StructField(getInputCol, StringType, false),
        StructField("predictions", ArrayType(StructType(Seq(
          StructField("action", IntegerType, false),
          StructField("probability", FloatType, false)
        ))))
      ))
      case "prediction_type_t::multiclass" => StructType(Seq(
        StructField(getInputCol, StringType, false),
        StructField("prediction", IntegerType, false)
      ))
      case "prediction_type_t::multilabels" => StructType(Seq(
        StructField(getInputCol, StringType, false),
        StructField("prediction", ArrayType(IntegerType), false)
      ))
      case "prediction_type_t::prob"=> StructType(Seq(
        StructField(getInputCol, StringType, false),
        StructField("prediction", FloatType, false)
      ))
      case "prediction_type_t::decision_probs" => StructType(Seq(
        StructField(getInputCol, StringType, false),
        StructField("predictions",
          ArrayType(
            ArrayType(
              StructType(Seq(
                StructField("action", IntegerType, false),
                StructField("score", FloatType, false)
              )))))))
      // TODO: the java wrapper would have to support them too
      // CASE(prediction_type_t::pdf)
      // CASE(prediction_type_t::multiclassprobs)
      // CASE(prediction_type_t::action_pdf_value)
      // CASE(prediction_type_t::active_multiclass)
      // CASE(prediction_type_t::nopred)
    }
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val df = dataset.toDF()

    val inputColIdx = df.schema.fieldIndex(getInputCol)

    import df.sparkSession.implicits._

    // TODO: logTransform?
    // limit(1).
    // better to use UDF? notice that I need to access the lazy vw object
    df.mapPartitions(inputRows => {
        inputRows.map { row => {
          val input = row.getString(inputColIdx)
          vw.predictFromString(input) match {
            case pred: ScalarPrediction => (input, pred.getValue, pred.getConfidence)
            // TODO: adding any of the lines below leads to
            // no Java class corresponding to Product with Serializable found
            // case pred: Array[java.lang.Float] => (input, pred)
            //            case pred: ActionScores => Tuple1(pred.getActionScores.map({ a_s => (a_s.getAction, a_s.getScore) }))
            //            case pred: ActionProbs => Tuple1(pred.getActionProbs.map({ a_s => (a_s.getAction, a_s.getProbability) }))
            //            case pred: DecisionScores => Tuple1(pred.getDecisionScores.map({ ds => {
            //              ds.getActionScores.map({ a_s => (a_s.getAction, a_s.getScore) })
            //            }
            //            }))
//                        case pred: java.lang.Integer => (input, pred.toInt)
                        //case pred: Multilabels => (input, pred.getLabels)
            //            case pred: java.lang.Float => Tuple1(pred.toFloat)
          }
        }}
      })
      .toDF(schemaForPredictionType().fields.map(_.name):_ *)
  }

  override def transformSchema(schema: StructType): StructType = {
    // TODO: validation
    StructType(schemaForPredictionType)
  }
}

object VowpalWabbitGenericModel extends ComplexParamsReadable[VowpalWabbitGenericModel]
