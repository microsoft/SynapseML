package com.microsoft.ml.spark.core.spark;

/** A fast vector assembler.  The columns given must be ordered such that categorical columns come first
  * (otherwise spark learners will give categorical attributes to the wrong index).
  * Does not keep spurious numeric data which can significantly slow down computations when there are
  * millions of columns.
  */
class FastVectorAssembler (override val uid: String)
  extends Transformer with HasInputCols with HasOutputCol with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("FastVectorAssembler"))

  /** @group setParam */
  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    // Schema transformation.
    val schema = dataset.schema
    lazy val first = dataset.toDF.first()
    var addedNumericField = false

    // Propagate only nominal (categorical) attributes (others only slow down the code)
    val attrs: Array[Attribute] = $(inputCols).flatMap { c =>
      val field = schema(c)
      val index = schema.fieldIndex(c)
      field.dataType match {
        case _: NumericType | BooleanType =>
          val attr = Attribute.fromStructField(field)
          if (attr.isNominal) {
            if (addedNumericField) {
              throw new SparkException("Categorical columns must precede all others, column out of order: " + c)
            }
            Some(attr.withName(c))
          } else {
            addedNumericField = true
            None
          }
        case _: VectorUDT =>
          val group = AttributeGroup.fromStructField(field)
          if (group.attributes.isDefined) {
            // If attributes are defined, copy them with updated names.
            group.attributes.get.zipWithIndex.map { case (attr, i) =>
              if (attr.isNominal && attr.name.isDefined) {
                if (addedNumericField) {
                  throw new SparkException("Categorical columns must precede all others, column out of order: " + c)
                }
                attr.withName(c + "_" + attr.name.get)
              } else if (attr.isNominal) {
                if (addedNumericField) {
                  throw new SparkException("Categorical columns must precede all others, column out of order: " + c)
                }
                attr.withName(c + "_" + i)
              } else {
                addedNumericField = true
                null
              }
            }.filter(attr => attr != null)
          } else {
            addedNumericField = true
            None
          }
        case otherType =>
          throw new SparkException(s"FastVectorAssembler does not support the $otherType type")
      }
    }
    val metadata = new AttributeGroup($(outputCol), attrs).toMetadata()

    // Data transformation.
    val assembleFunc = udf { r: Row =>
      FastVectorAssembler.assemble(r.toSeq: _*)
    }
    val args = $(inputCols).map { c =>
      schema(c).dataType match {
        case DoubleType => dataset(c)
        case _: VectorUDT => dataset(c)
        case _: NumericType | BooleanType => dataset(c).cast(DoubleType).as(s"${c}_double_$uid")
      }
    }

    dataset.select(col("*"), assembleFunc(struct(args: _*)).as($(outputCol), metadata))
  }

  override def transformSchema(schema: StructType): StructType = {
    val inputColNames = $(inputCols)
    val outputColName = $(outputCol)
    val inputDataTypes = inputColNames.map(name => schema(name).dataType)
    inputDataTypes.foreach {
      case _: NumericType | BooleanType =>
      case t if t.isInstanceOf[VectorUDT] =>
      case other =>
        throw new IllegalArgumentException(s"Data type $other is not supported.")
    }
    if (schema.fieldNames.contains(outputColName)) {
      throw new IllegalArgumentException(s"Output column $outputColName already exists.")
    }
    StructType(schema.fields :+ new StructField(outputColName, new VectorUDT, true))
  }

  override def copy(extra: ParamMap): FastVectorAssembler = defaultCopy(extra)

}
