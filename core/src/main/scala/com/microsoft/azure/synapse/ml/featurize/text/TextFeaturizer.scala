// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.featurize.text

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.contracts.{HasInputCol, HasOutputCol}
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import com.microsoft.azure.synapse.ml.stages.DropColumns
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.feature._
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.ml._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types._

trait TextFeaturizerParams extends Wrappable with DefaultParamsWritable {

  /** Tokenize the input when set to true
    *
    * @group param
    */
  val useTokenizer = new BooleanParam(this, "useTokenizer", "Whether to tokenize the input")

  /** @group getParam */
  final def getUseTokenizer: Boolean = $(useTokenizer)

  /** Indicates whether the regex splits on gaps (true) or matches tokens (false)
    *
    * @group param
    */
  val tokenizerGaps = new BooleanParam(
    this,
    "tokenizerGaps",
    "Indicates whether regex splits on gaps (true) or matches tokens (false)."
  )

  /** @group getParam */
  final def getTokenizerGaps: Boolean = $(tokenizerGaps)

  /** Minumum token length; must be 0 or greater.
    *
    * @group param
    */
  val minTokenLength = new IntParam(this, "minTokenLength", "Minimum token length, >= 0.")

  /** @group getParam */
  final def getMinTokenLength: Int = $(minTokenLength)

  /** Regex pattern used to match delimiters if gaps (true) or tokens (false)
    *
    * @group param
    */
  val tokenizerPattern = new Param[String](
    this,
    "tokenizerPattern",
    "Regex pattern used to match delimiters if gaps is true or tokens if gaps is false.")

  /** @group getParam */
  final def getTokenizerPattern: String = $(tokenizerPattern)

  /** Indicates whether to convert all characters to lowercase before tokenizing.
    *
    * @group param
    */
  val toLowercase = new BooleanParam(
    this,
    "toLowercase",
    "Indicates whether to convert all characters to lowercase before tokenizing.")

  /** @group getParam */
  final def getToLowercase: Boolean = $(toLowercase)

  /** Indicates whether to remove stop words from tokenized data.
    *
    * @group param
    */
  val useStopWordsRemover = new BooleanParam(this,
    "useStopWordsRemover",
    "Whether to remove stop words from tokenized data")

  /** @group getParam */
  final def getUseStopWordsRemover: Boolean = $(useStopWordsRemover)

  /** Indicates whether a case sensitive comparison is performed on stop words.
    *
    * @group param
    */
  val caseSensitiveStopWords = new BooleanParam(
    this,
    "caseSensitiveStopWords",
    " Whether to do a case sensitive comparison over the stop words")

  /** @group getParam */
  final def getCaseSensitiveStopWords: Boolean = $(caseSensitiveStopWords)

  /** Specify the language to use for stop word removal. The Use the custom setting when using the
    * stopWords input
    *
    * @group param
    */
  val defaultStopWordLanguage = new Param[String](this,
    "defaultStopWordLanguage",
    "Which language to use for the stop word remover," +
      " set this to custom to use the stopWords input")

  /** @group getParam */
  final def getDefaultStopWordLanguage: String = $(defaultStopWordLanguage)

  /** The words to be filtered out. This is a comma separated list of words, encoded as a single string.
    * For example, "a, the, and"
    */
  val stopWords = new Param[String](this, "stopWords", "The words to be filtered out.")

  /** @group getParam */
  final def getStopWords: String = $(stopWords)

  /** Enumerate N grams when set
    *
    * @group param
    */
  val useNGram = new BooleanParam(this, "useNGram", "Whether to enumerate N grams")

  /** @group getParam */
  final def getUseNGram: Boolean = $(useNGram)

  /** The size of the Ngrams
    *
    * @group param
    */
  val nGramLength = new IntParam(this, "nGramLength", "The size of the Ngrams")

  /** @group getParam */
  final def getNGramLength: Int = $(nGramLength)

  /** All nonnegative word counts are set to 1 when set to true
    *
    * @group param
    */
  val binary = new BooleanParam(
    this,
    "binary",
    "If true, all nonegative word counts are set to 1")

  /** @group getParam */
  final def getBinary: Boolean = $(binary)

  /** Set the number of features to hash each document to
    *
    * @group param
    */
  val numFeatures = new IntParam(
    this,
    "numFeatures",
    "Set the number of features to hash each document to")

  /** @group getParam */
  final def getNumFeatures: Int = $(numFeatures)

  /** Scale the Term Frequencies by IDF when set to true
    *
    * @group param
    */
  val useIDF = new BooleanParam(
    this,
    "useIDF",
    "Whether to scale the Term Frequencies by IDF")

  /** @group getParam */
  final def getUseIDF: Boolean = $(useIDF)

  /** Minimum number of documents in which a term should appear.
    *
    * @group param
    */
  val minDocFreq = new IntParam(
    this,
    "minDocFreq",
    "The minimum number of documents in which a term should appear.")

  /** @group getParam */
  final def getMinDocFreq: Int = $(minDocFreq)

}

object TextFeaturizer extends DefaultParamsReadable[TextFeaturizer]

/** Featurize text.
  *
  * @param uid The id of the module
  */
class TextFeaturizer(override val uid: String)
  extends Estimator[PipelineModel]
    with TextFeaturizerParams with HasInputCol with HasOutputCol with SynapseMLLogging {
  logClass(FeatureNames.Featurize)

  def this() = this(Identifiable.randomUID("TextFeaturizer"))

  setDefault(outputCol, uid + "_output")

  def setUseTokenizer(value: Boolean): this.type = set(useTokenizer, value)

  setDefault(useTokenizer -> true)

  /** @group setParam */
  def setTokenizerGaps(value: Boolean): this.type = set(tokenizerGaps, value)

  setDefault(tokenizerGaps -> true)

  /** @group setParam */
  def setMinTokenLength(value: Int): this.type = set(minTokenLength, value)

  setDefault(minTokenLength -> 0)

  /** @group setParam */
  def setTokenizerPattern(value: String): this.type =
    set(tokenizerPattern, value)

  setDefault(tokenizerPattern -> "\\s+")

  /** @group setParam */
  def setToLowercase(value: Boolean): this.type = set(toLowercase, value)

  setDefault(toLowercase -> true)

  /** @group setParam */
  def setUseStopWordsRemover(value: Boolean): this.type =
    set(useStopWordsRemover, value)

  setDefault(useStopWordsRemover -> false)

  /** @group setParam */
  def setCaseSensitiveStopWords(value: Boolean): this.type =
    set(caseSensitiveStopWords, value)

  setDefault(caseSensitiveStopWords -> false)

  /** @group setParam */
  def setDefaultStopWordLanguage(value: String): this.type =
    set(defaultStopWordLanguage, value)

  setDefault(defaultStopWordLanguage -> "english")

  /** @group setParam */
  def setStopWords(value: String): this.type = set(stopWords, value)

  /** @group setParam */
  def setUseNGram(value: Boolean): this.type = set(useNGram, value)

  /** @group setParam */
  def setNGramLength(value: Int): this.type = set(nGramLength, value)

  setDefault(useNGram -> false, nGramLength -> 2)

  /** @group setParam */
  def setBinary(value: Boolean): this.type = set(binary, value)

  /** @group setParam */
  def setNumFeatures(value: Int): this.type = set(numFeatures, value)

  setDefault(numFeatures -> (1 << 18), binary -> false)

  /** @group setParam */
  def setUseIDF(value: Boolean): this.type = set(useIDF, value)

  /** @group setParam */
  def setMinDocFreq(value: Int): this.type = set(minDocFreq, value)

  setDefault(useIDF -> true, minDocFreq -> 1)

  private def setParamInternal[M <: PipelineStage, T](model: M,
                                                      name: String,
                                                      value: T) = {
    model.set(model.getParam(name), value)
  }

  private def getParamInternal[M <: PipelineStage, T](model: M, name: String) = {
    model.getOrDefault(model.getParam(name))
  }

  //scalastyle:off method.length
  override def fit(dataset: Dataset[_]): PipelineModel = {
    logFit({
      try {
        getUseTokenizer
      } catch {
        case _: NoSuchElementException => setUseTokenizer(needsTokenizer(dataset.schema))
      }

      transformSchema(dataset.schema)
      var models: List[PipelineStage] = Nil
      if (getUseTokenizer)
        models ::= new RegexTokenizer()
          .setGaps(getTokenizerGaps)
          .setPattern(getTokenizerPattern)
          .setMinTokenLength(getMinTokenLength)
          .setToLowercase(getToLowercase)
          .setOutputCol(uid + "__Tokens")
      if (getUseStopWordsRemover) {
        val swr =
          new StopWordsRemover()
            .setCaseSensitive(getCaseSensitiveStopWords)
            .setOutputCol(uid + "__Stopwords")
        if (getDefaultStopWordLanguage == "custom") {
          models ::= swr.setStopWords(getStopWords.split(","))
        } else {
          models ::= swr.setStopWords(
            StopWordsRemover.loadDefaultStopWords(getDefaultStopWordLanguage))
        }
      }
      if (getUseNGram)
        models ::= new NGram().setN(getNGramLength).setOutputCol(uid + "__ngrams")
      models ::= new HashingTF()
        .setBinary(getBinary)
        .setNumFeatures(getNumFeatures).setOutputCol(uid + "__hash")
      if (getUseIDF)
        models ::= new IDF().setMinDocFreq(getMinDocFreq).setOutputCol(uid + "__idf")
      models = models.reverse

      val chainedModels = models
        .zip(0 to models.length)
        .map(
          { pair: (PipelineStage, Int) =>
            val model = pair._1
            val i = pair._2
            if (i == 0) {
              setParamInternal(model, "inputCol", getInputCol)
            } else if (i < models.length - 1) {
              setParamInternal(models(i - 1), "outputCol", getParamInternal(models(i - 1), "outputCol"))
              setParamInternal(model, "inputCol", getParamInternal(models(i - 1), "outputCol"))
            } else {
              setParamInternal(models(i - 1), "outputCol", getParamInternal(models(i - 1), "outputCol"))
              val m1 = setParamInternal(model, "inputCol",
                getParamInternal(models(i - 1), "outputCol"))
              setParamInternal(m1, "outputCol", getOutputCol)
            }
          }
        )
      val colsToDrop = chainedModels.reverse.tail
        .map(getParamInternal(_, "outputCol").asInstanceOf[String])

      val stages = chainedModels ++ Seq(new DropColumns().setCols(colsToDrop.toArray))
      new Pipeline().setStages(stages.toArray).fit(dataset).setParent(this)
    }, dataset.columns.length)
  }
  //scalastyle:on method.length

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    validateInputType(inputType)
    if (schema.fieldNames.contains($(outputCol))) {
      throw new IllegalArgumentException(
        s"Output column ${$(outputCol)} already exists.")
    }
    val attrGroup = new AttributeGroup($(outputCol), $(numFeatures))
    appendColumn(schema, attrGroup.toStructField())
  }

  private def needsTokenizer(schema: StructType) = {
    val inputType = schema($(inputCol)).dataType
    inputType == StringType
  }

  private def validateInputType(inputType: DataType): Unit = {
    if (getUseTokenizer) {
      if (inputType == ArrayType(StringType)) {
        require(
          inputType == StringType,
          s"Input type must be string type but got $inputType. " +
            s"It looks like your data is already tokenized, Try with useTokenizer=False")
      }
      require(inputType == StringType,
        s"Input type must be string type but got $inputType.")
    } else if (getUseNGram) {
      if (inputType == StringType) {
        require(
          inputType == ArrayType(StringType),
          s"Input type must be Array[string] type but got $inputType. " +
            s"It looks like your data not tokenized, Try with useTokenizer=True")
      }
      require(
        inputType == ArrayType(StringType),
        s"Input type must be Array(String) type type but got $inputType.")
    } else {
      if (inputType == StringType) {
        require(
          inputType.isInstanceOf[ArrayType],
          s"Input type must be Array[_] type but got $inputType. " +
            s"It looks like your data not tokenized, Try with useTokenizer=True")
      }
      require(inputType.isInstanceOf[ArrayType],
        s"Input type must be Array(_) type type but got $inputType.")
    }
  }

  private def appendColumn(schema: StructType, col: StructField): StructType = {
    require(!schema.fieldNames.contains(col.name),
      s"Column ${col.name} already exists.")
    StructType(schema.fields :+ col)
  }
}
