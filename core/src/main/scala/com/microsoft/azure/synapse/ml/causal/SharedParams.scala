package com.microsoft.azure.synapse.ml.causal

import org.apache.spark.ml.param.{Param, Params}

trait HasTreatmentCol extends Params {
  val treatmentCol = new Param[String](this, "treatmentCol", "treatment column")
  def getTreatmentCol: String = $(treatmentCol)

  /**
    * Set name of the column which will be used as treatment
    *
    * @group setParam
    */
  def setTreatmentCol(value: String): this.type = set(treatmentCol, value)
}

trait HasOutcomeCol extends Params {
  val outcomeCol: Param[String] = new Param[String](this, "outcomeCol", "outcome column")
  def getOutcomeCol: String = $(outcomeCol)

  /**
    * Set name of the column which will be used as outcome
    *
    * @group setParam
    */
  def setOutcomeCol(value: String): this.type = set(outcomeCol, value)
}

trait HasPostTreatmentCol extends Params {
  final val postTreatmentCol = new Param[String](this, "postTreatmentCol", "post treatment indicator column")
  def getPostTreatmentCol: String = $(postTreatmentCol)

  /**
    * Set name of the column which tells whether the outcome is measured post treatment.
    *
    * @group setParam
    */
  def setPostTreatmentCol(value: String): this.type = set(postTreatmentCol, value)
}

trait HasUnitCol extends Params {
  final val unitCol = new Param[String](this, "unitCol",
    "Column that identifies the units in panel data")
  def getUnitCol: String = $(unitCol)
  def setUnitCol(value: String): this.type = set(unitCol, value)
}

trait HasTimeCol extends Params {
  final val timeCol = new Param[String](this, "timeCol",
    "Column that identifies the time when outcome is measured in panel data")
  def getTimeCol: String = $(timeCol)
  def setTimeCol(value: String): this.type = set(timeCol, value)
}
