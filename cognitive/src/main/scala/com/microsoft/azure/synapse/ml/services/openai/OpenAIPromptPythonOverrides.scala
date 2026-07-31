// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.codegen.GenerationUtils

private[openai] object OpenAIPromptPythonOverrides {
  private val DefaultInitParamLoop =
    """    if java_obj is None:
      |        for k,v in kwargs.items():
      |            if v is not None:
      |                getattr(self, "set" + k[0].upper() + k[1:])(v)
      |""".stripMargin

  private val OptionsLastInitParamLoop =
    """    if java_obj is None:
      |        post_processing_options = kwargs.pop("postProcessingOptions", None)
      |        for k,v in kwargs.items():
      |            if v is not None:
      |                getattr(self, "set" + k[0].upper() + k[1:])(v)
      |        if post_processing_options is not None:
      |            self.setPostProcessingOptions(post_processing_options)
      |""".stripMargin

  def initFunc(defaultInitFunc: String): String = {
    val result = defaultInitFunc.replace(DefaultInitParamLoop, OptionsLastInitParamLoop)
    require(result != defaultInitFunc, "OpenAIPrompt Python initializer template did not match")
    result
  }

  def methods(baseMethods: String, paramsArgs: String): String = baseMethods + {
    s"""
      |def setPostProcessingOptions(self, value):
      |    if not value:
      |        self._set(postProcessingOptions=value)
      |        return self
      |    if self.isSet(self.postProcessing):
      |        self._java_obj = self._java_obj.setPostProcessing(self.getPostProcessing())
      |    java_value = SparkContext._active_spark_context._jvm.java.util.HashMap()
      |    for key, option in value.items():
      |        java_value.put(key, option)
      |    self._java_obj = self._java_obj.setPostProcessingOptions(java_value)
      |    self._set(postProcessingOptions=value)
      |    self._set(postProcessing=self._java_obj.getPostProcessing())
      |    return self
      |
      |@keyword_only
      |def setParams(
      |    self,
      |${GenerationUtils.indent(paramsArgs, 1)}
      |    ):
      |    if hasattr(self, "_input_kwargs"):
      |        kwargs = dict(self._input_kwargs)
      |    else:
      |        kwargs = dict(self.__init__._input_kwargs)
      |    if "postProcessingOptions" not in kwargs:
      |        return self._set(**kwargs)
      |    value = kwargs.pop("postProcessingOptions")
      |    if value is None:
      |        kwargs["postProcessingOptions"] = value
      |        return self._set(**kwargs)
      |    self._set(**kwargs)
      |    return self.setPostProcessingOptions(value)
      |""".stripMargin
  }
}
