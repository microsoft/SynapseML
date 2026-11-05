// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import com.microsoft.azure.synapse.ml.services.openai.OpenAIPromptPythonOverrides.replaceExactlyOnce
import com.microsoft.azure.synapse.ml.services.openai.OpenAIDecisions

private[ai] object AIPromptPython {
  private val ObjectParams = Set("responseFormat", "extraBody")
  private val JsonParams = ObjectParams + "questions"

  def paramInputType(name: String): Option[String] =
    if (ObjectParams(name)) Some("Union[str, Dict[str, Any]]") else None

  def nativeReader(defaultClass: String): String = {
    val nativeClasses = Seq(classOf[AIPrompt], classOf[AISystemOne], classOf[OpenAIDecisions])
    val readers = nativeClasses.map { cls =>
      val name = cls.getName
      s""""$name": "${name.replace("com.microsoft.azure.synapse.ml", "synapse.ml")}""""
    }.mkString(",\n            ")
    replaceExactlyOnce(defaultClass, "        return from_java(java_stage, module_name)",
      s"""        native_readers = {
         |            $readers
         |        }
         |        module_name = native_readers.get(java_stage.getClass().getName(), module_name)
         |        return from_java(java_stage, module_name)""".stripMargin,
      "AIPrompt Python reader template did not match")
  }

  def initFunc(base: String): String = replaceExactlyOnce(base,
    "    if java_obj is None:\n        kwargs = dict(kwargs)",
    """    if java_obj is not None:
      |        self._resetUid(java_obj.uid())
      |    self._transfer_params_from_java()
      |    if java_obj is None:
      |        kwargs = dict(kwargs)""".stripMargin,
    "AIPrompt Python initializer template did not match")

  def paramSetter(base: String, name: String): String = {
    if (!JsonParams(name)) base else {
      val conversion = if (name == "responseFormat") {
        """value if isinstance(value, str) else self._java_obj.responseFormat().jsonDecode('{"left":' + """ +
          """self._ai_json(value, object_only=True) + '}').left().get()"""
      } else s"self._ai_json(value, object_only=${if (ObjectParams(name)) "True" else "False"})"
      replaceExactlyOnce(base, "value = self._service_param_value_to_java(value)", s"value = $conversion",
        s"AIPrompt Python $name setter template did not match")
    }
  }

  def additionalMethods(base: String, className: String): String = {
    val jsonParams = JsonParams.toSeq.sorted.map(name => s""""$name"""").mkString("(", ", ", ")")
    base + s"""
      |def _transfer_params_from_java(self, include_defaults=True):
      |    super($className, self)._transfer_params_from_java(include_defaults=include_defaults)
      |
      |def _service_param_scalar_to_python(self, name, value):
      |    if name not in $jsonParams:
      |        return super($className, self)._service_param_scalar_to_python(name, value)
      |    import json
      |    import math
      |    from decimal import Decimal
      |    def exact_number(token):
      |        number = float(token)
      |        exact = Decimal(token)
      |        return number if math.isfinite(number) and Decimal(str(number)) == exact else exact
      |    sc = SparkContext._active_spark_context
      |    parameter = self._java_obj.getParam(name)
      |    encoded = parameter.jsonEncode(sc._jvm.scala.util.Left.apply(value))
      |    return json.loads(encoded, parse_float=exact_number)["left"]
      |""".stripMargin + Methods
  }

  val Methods: String =
    """
      |def _ai_json(self, value, object_only=False):
      |    import json
      |    from decimal import Decimal
      |    if isinstance(value, str):
      |        # JVM parsing validates duplicate keys without losing the original JSON.
      |        return value
      |    if object_only and not isinstance(value, dict):
      |        raise TypeError("Expected a JSON object")
      |    active = set()
      |    def encode(item):
      |        if isinstance(item, Decimal):
      |            if not item.is_finite():
      |                raise ValueError("Non-finite Decimal values are not JSON compliant")
      |            return str(item)
      |        if isinstance(item, (dict, list, tuple)):
      |            identity = id(item)
      |            if identity in active:
      |                raise ValueError("Circular reference detected")
      |            active.add(identity)
      |            try:
      |                if isinstance(item, dict):
      |                    if any(not isinstance(key, str) for key in item):
      |                        raise TypeError("JSON object keys must be strings")
      |                    return "{" + ",".join(json.dumps(key) + ":" + encode(child)
      |                                          for key, child in item.items()) + "}"
      |                return "[" + ",".join(encode(child) for child in item) + "]"
      |            finally:
      |                active.remove(identity)
      |        return json.dumps(item, allow_nan=False)
      |    return encode(value)
      |
      |def setStateMode(self, value):
      |    return self.setInputMode(value)
      |""".stripMargin
}
