package com.microsoft.ml.spark.cognitive

import com.microsoft.ml.spark.core.schema.SparkBindings
import org.apache.spark.ml.ComplexParamsReadable


object DetectLanguageResponseV4 extends SparkBindings[TAResponseV4[DetectedLanguageV4]]

object KeyPhraseResponseV4 extends SparkBindings[TAResponseV4[KeyphraseV4]]

case class TAResponseV4[T](result: Option[T],
                           error: Option[TAErrorV4],
                           statistics: Option[DocumentStatistics],
                           modelVersion: Option[String])
case class DetectedLanguageV4(name: String, iso6391Name: String, confidenceScore: Double)

case class TAErrorV4(errorCode: String, errorMessage: String, target: String)

case class TAWarningV4 (warningCode: String, message: String)


case class KeyphraseV4(keyPhrases: List[String], warnings: List[TAWarningV4])
