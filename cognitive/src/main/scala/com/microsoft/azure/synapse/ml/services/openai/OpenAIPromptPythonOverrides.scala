// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

private[openai] object OpenAIPromptPythonOverrides {
  private def replaceExactlyOnce(value: String, target: String, replacement: String, errorMessage: String): String = {
    require(value.sliding(target.length).count(_ == target) == 1, errorMessage)
    value.replace(target, replacement)
  }

  private val DefaultInitParamLoop =
    """    if java_obj is None:
      |        for k,v in kwargs.items():
      |            if v is not None:
      |                getattr(self, "set" + k[0].upper() + k[1:])(v)
      |""".stripMargin

  private val OptionsLastInitParamLoop =
    """    self._post_processing_explicitly_set = False
      |    if java_obj is None:
      |        kwargs = dict(kwargs)
      |        post_processing_options = kwargs.pop("postProcessingOptions", None)
      |        for k,v in kwargs.items():
      |            if v is not None:
      |                getattr(self, "set" + k[0].upper() + k[1:])(v)
      |        if post_processing_options is not None:
      |            self.setPostProcessingOptions(post_processing_options)
      |""".stripMargin

  def initFunc(defaultInitFunc: String): String = {
    replaceExactlyOnce(
      defaultInitFunc,
      DefaultInitParamLoop,
      OptionsLastInitParamLoop,
      "OpenAIPrompt Python initializer template did not match"
    )
  }

  def postProcessingOptionsSetter(defaultSetter: String): String = {
    val defaultBody =
      """    self._set(postProcessingOptions=value)
        |    return self
        |""".stripMargin
    val validatedBody =
      """    value = self._normalize_post_processing_options(value)
        |    java_value = self._to_java_post_processing_options(value)
        |    post_processing = (
        |        self.getPostProcessing() if self.isSet(self.postProcessing) else None
        |    )
        |    if (
        |        post_processing == ""
        |        and not self._post_processing_explicitly_set
        |    ):
        |        post_processing = None
        |    inferred_post_processing = self._validate_post_processing_options(
        |        java_value, post_processing
        |    )
        |    return self._apply_post_processing_options(
        |        value, inferred_post_processing
        |    )
        |""".stripMargin
    replaceExactlyOnce(
      defaultSetter,
      defaultBody,
      validatedBody,
      "OpenAIPrompt Python setter template did not match"
    )
  }

  def postProcessingSetter(defaultSetter: String): String = {
    val defaultBody =
      """    self._set(postProcessing=value)
        |    return self
        |""".stripMargin
    val validatedBody =
      """    value = self._coerce_post_processing(value)
        |    self._validate_post_processing(value)
        |    self._set(postProcessing=value)
        |    self._post_processing_explicitly_set = True
        |    return self
        |""".stripMargin
    replaceExactlyOnce(
      defaultSetter,
      defaultBody,
      validatedBody,
      "OpenAIPrompt Python postProcessing setter template did not match"
    )
  }

  def setParamsFunc(defaultSetParamsFunc: String): String = {
    val defaultBody =
      """    if hasattr(self, "_input_kwargs"):
        |        kwargs = self._input_kwargs
        |    else:
        |        kwargs = self.__init__._input_kwargs
        |    return self._set(**kwargs)
        |""".stripMargin
    val validatedBody =
      """    if hasattr(self, "_input_kwargs"):
        |        kwargs = dict(self._input_kwargs)
        |    else:
        |        kwargs = dict(self.__init__._input_kwargs)
        |    post_processing_explicit = "postProcessing" in kwargs
        |    if post_processing_explicit:
        |        kwargs["postProcessing"] = self._coerce_post_processing(
        |            kwargs["postProcessing"]
        |        )
        |    if "postProcessingOptions" not in kwargs:
        |        if "postProcessing" in kwargs:
        |            self._validate_post_processing(kwargs["postProcessing"])
        |        result = self._set_params_atomically(kwargs)
        |        if post_processing_explicit:
        |            self._post_processing_explicitly_set = True
        |        return result
        |    value = self._normalize_post_processing_options(kwargs["postProcessingOptions"])
        |    java_value = self._to_java_post_processing_options(value)
        |    post_processing = kwargs.get("postProcessing")
        |    if post_processing is None and self.isSet(self.postProcessing):
        |        post_processing = self.getPostProcessing()
        |    if (
        |        post_processing == ""
        |        and not post_processing_explicit
        |        and not self._post_processing_explicitly_set
        |    ):
        |        post_processing = None
        |    inferred_post_processing = self._validate_post_processing_options(
        |        java_value, post_processing
        |    )
        |    kwargs["postProcessingOptions"] = value
        |    if inferred_post_processing:
        |        kwargs["postProcessing"] = inferred_post_processing
        |    result = self._set_params_atomically(kwargs)
        |    if post_processing_explicit:
        |        self._post_processing_explicitly_set = True
        |    return result
        |""".stripMargin
    replaceExactlyOnce(
      defaultSetParamsFunc,
      defaultBody,
      validatedBody,
      "OpenAIPrompt Python setParams template did not match"
    )
  }

  val AdditionalMethods: String =
    """
      |def _to_java_post_processing_options(self, value):
      |    value = self._normalize_post_processing_options(value)
      |    java_value = SparkContext._active_spark_context._jvm.java.util.HashMap()
      |    for key, option in value.items():
      |        java_value.put(key, option)
      |    return java_value
      |
      |def _normalize_post_processing_options(self, value):
      |    if isinstance(value, JavaObject):
      |        result = {}
      |        if value.getClass().getName().startswith("scala.collection"):
      |            iterator = value.iterator()
      |            while iterator.hasNext():
      |                entry = iterator.next()
      |                result[entry._1()] = entry._2()
      |        else:
      |            iterator = value.entrySet().iterator()
      |            while iterator.hasNext():
      |                entry = iterator.next()
      |                result[entry.getKey()] = entry.getValue()
      |        value = result
      |    if not hasattr(value, "items"):
      |        raise TypeError("postProcessingOptions must be a mapping")
      |    result = {}
      |    for key, option in value.items():
      |        if not isinstance(key, basestring) or not isinstance(option, basestring):
      |            raise TypeError("postProcessingOptions keys and values must be strings")
      |        result[key] = option
      |    return result
      |
      |def _set_params_atomically(self, kwargs):
      |    converted = {}
      |    for param, value in kwargs.items():
      |        p = getattr(self, param)
      |        if value is not None:
      |            try:
      |                value = p.typeConverter(value)
      |            except TypeError as error:
      |                raise TypeError(
      |                    'Invalid param value given for param "%s". %s'
      |                    % (p.name, error)
      |                )
      |        converted[p] = value
      |    self._paramMap.update(converted)
      |    return self
      |
      |def _coerce_post_processing(self, value):
      |    if value is None:
      |        return value
      |    try:
      |        return self.postProcessing.typeConverter(value)
      |    except TypeError as error:
      |        raise TypeError(
      |            'Invalid param value given for param "%s". %s'
      |            % (self.postProcessing.name, error)
      |        )
      |
      |def _validate_post_processing_options(self, java_value, post_processing):
      |    return (
      |        SparkContext._active_spark_context._jvm.com.microsoft.azure.synapse.ml
      |        .services.openai.OpenAIPromptPostProcessing.validateAndInferMode(
      |            java_value, post_processing
      |        )
      |    )
      |
      |def _validate_post_processing(self, value):
      |    options = self.getPostProcessingOptions()
      |    if isinstance(options, JavaObject):
      |        (
      |            SparkContext._active_spark_context._jvm.com.microsoft.azure.synapse.ml
      |            .services.openai.OpenAIPromptPostProcessing.validateMode(
      |                self._java_obj, value
      |            )
      |        )
      |    else:
      |        java_value = self._to_java_post_processing_options(options)
      |        (
      |            SparkContext._active_spark_context._jvm.com.microsoft.azure.synapse.ml
      |            .services.openai.OpenAIPromptPostProcessing.validateModeWithOptions(
      |                java_value, value
      |            )
      |        )
      |
      |def _apply_post_processing_options(self, value, inferred_post_processing):
      |    self._set(postProcessingOptions=value)
      |    if inferred_post_processing:
      |        self._set(postProcessing=inferred_post_processing)
      |    return self
      |
      |def clear(self, param):
      |    if param == self.postProcessing:
      |        self._post_processing_explicitly_set = False
      |    return super().clear(param)
      |
      |def copy(self, extra=None):
      |    if extra is None:
      |        extra = {}
      |    result = super().copy(extra)
      |    result._post_processing_explicitly_set = (
      |        self._post_processing_explicitly_set
      |        or self.postProcessing in extra
      |    )
      |    if self.postProcessingOptions in extra:
      |        result.setPostProcessingOptions(result.getPostProcessingOptions())
      |    elif self.postProcessing in extra:
      |        result._validate_post_processing(result.getPostProcessing())
      |    return result
      |""".stripMargin
}
