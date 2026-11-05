// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import com.fasterxml.jackson.core.{JsonFactory, JsonParser, JsonProcessingException, JsonToken, StreamReadConstraints}
import com.fasterxml.jackson.core.exc.StreamConstraintsException
import com.microsoft.azure.synapse.ml.param.{RPrinter, ServiceParam}
import org.apache.spark.ml.param.Params
import spray.json._

import java.util.IdentityHashMap
import scala.collection.JavaConverters._

private[services] object AIJson {
  private[ai] val MaxDepth = 100
  private val MaxNumberLength = 1000
  private val ResponsesEnvelopeDepth = 3
  private val Factory = new JsonFactory().enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
    .setStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(Int.MaxValue)
      .maxNumberLength(MaxNumberLength).maxNestingDepth(MaxDepth + ResponsesEnvelopeDepth + 1).build())

  private class OrderedMap[+V](entries: Vector[(String, V)], index: Map[String, Int])
    extends Map[String, V] with Serializable {
    override def get(key: String): Option[V] = index.get(key).map(entries(_)._2)
    override def iterator: Iterator[(String, V)] = entries.iterator
    override def empty: OrderedMap[V] = new OrderedMap(Vector.empty, Map.empty)
    // Scala's immutable Map contract requires these operator names.
    //scalastyle:off method.name
    override def +[V1 >: V](entry: (String, V1)): OrderedMap[V1] = index.get(entry._1) match {
      case Some(position) => new OrderedMap(entries.updated(position, entry), index)
      case None => new OrderedMap(entries :+ entry, index.updated(entry._1, entries.length))
    }
    override def -(key: String): OrderedMap[V] = if (!index.contains(key)) this else {
      val remaining = entries.filterNot(_._1 == key)
      new OrderedMap(remaining, remaining.iterator.zipWithIndex.map { case ((name, _), position) =>
        name -> position
      }.toMap)
    }
    //scalastyle:on method.name
  }

  private[ai] def orderedMap[V](entries: Iterable[(String, V)]): Map[String, V] =
    entries.foldLeft(new OrderedMap[V](Vector.empty, Map.empty))(_ + _)

  implicit val AnyFormat: RootJsonFormat[Any] = new RootJsonFormat[Any] {
    override def write(value: Any): JsValue = fromAny(value)

    override def read(value: JsValue): Any = value match {
      case JsObject(fields) => orderedMap(fields.toSeq.map { case (key, item) => key -> read(item) })
      case JsArray(items) => items.map(read)
      case JsNumber(number) => number
      case JsString(text) => text
      case JsBoolean(boolean) => boolean
      case JsNull => None.orNull
    }
  }

  private implicit val ObjectFormat: RootJsonFormat[Map[String, Any]] = new RootJsonFormat[Map[String, Any]] {
    override def write(value: Map[String, Any]): JsValue = fromAny(value)
    override def read(value: JsValue): Map[String, Any] =
      orderedMap(obj(value).fields.toSeq.map { case (key, item) => key -> AnyFormat.read(item) })
  }

  class ObjectParam(parent: Params, name: String, doc: String, wireName: String)
    extends ServiceParam[Map[String, Any]](parent, name, doc, binding => {
      // Spark's validation error renderer dereferences an invalid null binding.
      require(Option(binding).isDefined, "Object parameter binding cannot be null")
      binding.fold(value => Option(value).isDefined, column => Option(column).isDefined)
    }) {
    override val payloadName: String = wireName
    // Preserve the existing left/right encoding without the default parser's unordered objects.
    override def jsonDecode(json: String): Either[Map[String, Any], String] = format.read(parseServiceParam(json))
  }

  def rJsonValue[T](value: Either[T, String]): String =
    RPrinter(JsString(value.fold(v => fromAny(v).compactPrint, identity)))

  def parse(value: String): JsValue = parse(value, 0)

  // The persisted left/right envelope is not part of the provider payload's depth.
  def parseServiceParam(value: String): JsValue = parse(value, -1)

  // Responses echoes a schema inside text.format.schema; these wrappers are not caller schema levels.
  def parseResponsesResponse(value: String): JsValue = parse(value, -ResponsesEnvelopeDepth)

  private def parse(value: String, initialDepth: Int): JsValue = {
    require(Option(value).isDefined, "Expected JSON, not null")
    val parser = Factory.createParser(value)
    try {
      val result = read(parser, parser.nextToken(), initialDepth)
      require(Option(parser.nextToken()).isEmpty, "JSON must contain exactly one value")
      result
    } catch {
      case _: StreamConstraintsException =>
        throw new IllegalArgumentException("JSON exceeds supported parsing limits (nesting or numeric token size)")
      case _: JsonProcessingException =>
        throw new IllegalArgumentException("Invalid JSON (including duplicate keys or non-finite numbers)")
      case _: NumberFormatException =>
        throw new IllegalArgumentException("Invalid JSON number")
    } finally {
      parser.close()
    }
  }

  private def read(parser: JsonParser, token: JsonToken, depth: Int): JsValue = {
    require(depth <= MaxDepth, "JSON nesting exceeds the supported depth")
    token match {
      case JsonToken.START_OBJECT =>
        val fields = Iterator.continually(parser.nextToken()).takeWhile(_ != JsonToken.END_OBJECT).map { next =>
          require(next == JsonToken.FIELD_NAME, "Expected a JSON object field")
          val key = validatedString(parser.currentName())
          key -> read(parser, parser.nextToken(), depth + 1)
        }.toVector
        JsObject(orderedMap(fields))
      case JsonToken.START_ARRAY =>
        val values = Iterator.continually(parser.nextToken()).takeWhile(_ != JsonToken.END_ARRAY).map { next =>
          require(Option(next).isDefined, "Unterminated JSON array")
          read(parser, next, depth + 1)
        }.toVector
        JsArray(values)
      case JsonToken.VALUE_STRING => JsString(validatedString(parser.getText))
      case JsonToken.VALUE_NUMBER_INT | JsonToken.VALUE_NUMBER_FLOAT =>
        val number = BigDecimal(parser.getText)
        validateRenderedExponent(number)
        JsNumber(number)
      case JsonToken.VALUE_TRUE => JsBoolean(true)
      case JsonToken.VALUE_FALSE => JsBoolean(false)
      case JsonToken.VALUE_NULL => JsNull
      case _ => throw new IllegalArgumentException("Expected a JSON value")
    }
  }

  def obj(value: JsValue): JsObject = value match {
    case result: JsObject => result
    case _ => throw new IllegalArgumentException("Expected a JSON object")
  }

  def objectString(value: String): JsObject = obj(parse(value))

  def fromAny(value: Any): JsValue = new ValueConverter().convert(value, 0)

  private def hasPairedSurrogates(value: String): Boolean =
    value.indices.forall { index =>
      val character = value.charAt(index)
      if (Character.isHighSurrogate(character)) {
        index + 1 < value.length && Character.isLowSurrogate(value.charAt(index + 1))
      } else if (Character.isLowSurrogate(character)) {
        index > 0 && Character.isHighSurrogate(value.charAt(index - 1))
      } else true
    }

  private[ai] def validatedString(value: String): String =
    validatedText(value, "JSON strings and keys")

  private[ai] def validatedText(value: String, description: String): String = {
    require(hasPairedSurrogates(value), s"$description must not contain unpaired surrogates")
    value
  }

  // Consume one value from the current token without replacing the caller's JSON syntax or limits.
  def hasValidStringTokens(parser: JsonParser): Boolean = {
    var depth = 0
    val tokens = Iterator.single(parser.currentToken()) ++
      Iterator.continually(()).takeWhile(_ => depth > 0).map(_ => parser.nextToken())
    tokens.forall {
      case JsonToken.START_OBJECT | JsonToken.START_ARRAY =>
        depth += 1
        true
      case JsonToken.END_OBJECT | JsonToken.END_ARRAY =>
        depth -= 1
        true
      case JsonToken.VALUE_STRING | JsonToken.FIELD_NAME => hasPairedSurrogates(parser.getText)
      case token => Option(token).isDefined
    }
  }

  private def validateRenderedExponent(number: BigDecimal): Unit =
    require(number.bigDecimal.precision.toLong - number.bigDecimal.scale.toLong - 1 <= Int.MaxValue,
      "JSON number exponent exceeds supported parsing limits")

  private def validatedNumber(value: JsNumber): JsNumber = {
    validateRenderedExponent(value.value)
    val encoded = value.compactPrint
    // Let the parser handle sign, decimal and exponent accounting at the boundary.
    if (encoded.length > MaxNumberLength) parse(encoded)
    value
  }

  private class ValueConverter {
    private val ancestors = new IdentityHashMap[AnyRef, java.lang.Boolean]()
    private def guarded(ref: AnyRef, depth: Int)(f: => JsValue): JsValue = {
      require(depth <= MaxDepth, "JSON nesting exceeds the supported depth")
      require(!ancestors.containsKey(ref), "Cyclic JSON values are not supported")
      ancestors.put(ref, true)
      try f finally ancestors.remove(ref)
    }
    private def objectFields(values: Iterable[(Any, Any)], depth: Int): JsObject =
      JsObject(orderedMap(values.toSeq.map {
        case (key: String, item) => validatedString(key) -> convert(item, depth + 1)
        case _ => throw new IllegalArgumentException("JSON object keys must be strings")
      }))
    def convert(item: Any, depth: Int): JsValue = {
      require(depth <= MaxDepth, "JSON nesting exceeds the supported depth")
      Option(item).fold[JsValue](JsNull) {
        case json: JsValue => jsonValue(json, depth)
        case text: String => JsString(validatedString(text))
        case boolean: Boolean => JsBoolean(boolean)
        case number: Double =>
          require(!number.isNaN && !number.isInfinity, "JSON numbers must be finite")
          validatedNumber(JsNumber(number))
        case number: Float =>
          require(!number.isNaN && !number.isInfinity, "JSON numbers must be finite")
          validatedNumber(JsNumber(BigDecimal(java.lang.Float.toString(number))))
        case number: java.lang.Number => validatedNumber(JsNumber(BigDecimal(number.toString)))
        case other => collection(other, depth)
      }
    }
    private def jsonValue(value: JsValue, depth: Int): JsValue = value match {
      case JsObject(fields) => objectFields(fields.toSeq, depth)
      case JsArray(values) => JsArray(values.map(convert(_, depth + 1)))
      case number: JsNumber => validatedNumber(number)
      case JsString(text) => JsString(validatedString(text))
      case scalar => scalar
    }
    private def collection(item: Any, depth: Int): JsValue = item match {
      case map: scala.collection.Map[_, _] =>
        guarded(map, depth)(objectFields(map.toSeq, depth))
      case map: java.util.Map[_, _] =>
        guarded(map, depth)(objectFields(map.asScala.toSeq, depth))
      case items: scala.collection.Seq[_] =>
        guarded(items, depth)(JsArray(items.map(convert(_, depth + 1)).toVector))
      case items: java.util.List[_] =>
        guarded(items, depth)(JsArray(items.asScala.map(convert(_, depth + 1)).toVector))
      case items: Array[_] =>
        guarded(items, depth)(JsArray(items.map(convert(_, depth + 1)).toVector))
      case _ => throw new IllegalArgumentException("Unsupported JSON value type")
    }
  }
}

private[services] object AIExtraBody {
  private val Reserved = Set(
    "model", "messages", "input", "state", "questions", "url", "endpoint", "base_url", "api_version",
    "api_key", "subscription_key", "subscriptionKey", "AADToken", "CustomAuthHeader", "customHeaders",
    "headers", "extra_headers", "extra_query", "query", "timeout", "max_retries", "retry_policy",
    "outputPath", "outputBatchSize", "output_path", "output_batch_size", "customUrlRoot")
  private val TokenAliases = Set("max_tokens", "max_completion_tokens", "max_output_tokens")

  def merge(base: JsObject, extension: JsObject, owned: Set[List[String]], api: String,
            lossless: Boolean): JsObject = {
    val reserved = extension.fields.keySet.intersect(Reserved)
    require(reserved.isEmpty, s"extraBody contains reserved fields: ${reserved.toSeq.sorted.mkString(", ")}")
    val extraTokens = extension.fields.keySet.intersect(TokenAliases)
    require(extraTokens.size <= 1, "extraBody contains conflicting token aliases")
    if (extraTokens.nonEmpty) {
      require(!owned.exists(p => p.nonEmpty && TokenAliases(p.head)),
        "extraBody conflicts with explicit typed option: max_tokens/max_completion_tokens/max_output_tokens")
    }
    val initial = if (extraTokens.nonEmpty) JsObject(base.fields -- TokenAliases) else base
    def combine(left: JsObject, right: JsObject, path: List[String]): JsObject = {
      JsObject(right.fields.foldLeft(AIJson.orderedMap(left.fields.toSeq)) { case (result, (key, value)) =>
        val current = path :+ key
        require(!owned.exists(p => current.startsWith(p)),
          s"extraBody conflicts with explicit typed option: ${current.mkString(".")}")
        val merged = (result.get(key), value) match {
          case (Some(a: JsObject), b: JsObject) => combine(a, b, current)
          case (None, b: JsObject) => combine(JsObject(), b, current)
          case _ =>
            require(!owned.exists(_.startsWith(current)),
              s"extraBody conflicts with explicit typed option: ${current.mkString(".")}")
            value
        }
        result.updated(key, merged)
      })
    }
    val result = combine(initial, extension, Nil)
    Seq("stream", "background").foreach { key =>
      result.fields.get(key).foreach(v =>
        require(v == JsFalse, s"$key=true is not supported; only synchronous responses are supported"))
    }
    require(result.fields.keySet.intersect(TokenAliases).size <= 1, "Request contains conflicting token aliases")
    if (api == "embeddings") {
      result.fields.get("encoding_format").foreach(v =>
        require(v == JsString("float"), "embeddings only supports encoding_format='float'"))
    }
    require(lossless || !result.fields.keySet.exists(Set("tools", "tool_choice", "functions", "function_call")),
      "Tool requests require rawResponseCol; AIPrompt does not execute tools")
    result
  }
}
