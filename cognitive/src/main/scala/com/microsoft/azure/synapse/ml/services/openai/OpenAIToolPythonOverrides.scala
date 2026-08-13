// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

private[openai] object OpenAIToolPythonOverrides {
  private val Common: String =
    """
      |def setTools(self, value):
      |    import json
      |    if isinstance(value, (list, tuple)):
      |        value = json.dumps(list(value))
      |    elif isinstance(value, dict):
      |        value = json.dumps([value])
      |    elif not isinstance(value, str):
      |        raise TypeError("tools must be a list of dicts, a dict, or a JSON string")
      |    self._java_obj = self._java_obj.setTools(value)
      |    return self
      |
      |def setToolChoice(self, value):
      |    import json
      |    if isinstance(value, dict):
      |        value = json.dumps(value)
      |    elif not isinstance(value, str):
      |        raise TypeError("toolChoice must be a str or a dict")
      |    self._java_obj = self._java_obj.setToolChoice(value)
      |    return self
      |
      |def addFunctionTool(self, name, description, parameters, strict=True):
      |    if not isinstance(parameters, dict):
      |        raise TypeError("parameters must be a dict")
      |    tools = self.getToolsAsList()
      |    tools.append({
      |        "type": "function",
      |        "name": name,
      |        "description": description,
      |        "parameters": parameters,
      |        "strict": strict,
      |    })
      |    return self.setTools(tools)
      |
      |def getToolsAsList(self):
      |    import json
      |    mode = self._java_obj.getToolsParamMode()
      |    if mode == "unset":
      |        return []
      |    if mode == "column":
      |        column = self._java_obj.getToolsCol()
      |        raise ValueError(
      |            "getToolsAsList requires scalar tools; "
      |            "tools is column-bound to {!r}".format(column)
      |        )
      |    if mode != "scalar":
      |        raise RuntimeError("Unknown Java tools parameter mode: {!r}".format(mode))
      |    raw = self._java_obj.getTools()
      |    return json.loads(raw) if raw else []
      |
      |def getToolsCol(self):
      |    return self._java_obj.getToolsCol()
      |
      |def getToolChoiceCol(self):
      |    return self._java_obj.getToolChoiceCol()
      |
      |def getParallelToolCallsCol(self):
      |    return self._java_obj.getParallelToolCallsCol()
      |""".stripMargin

  private val ResponsesOnly: String =
    """
      |def getMaxToolCallsCol(self):
      |    return self._java_obj.getMaxToolCallsCol()
      |
      |def getMetadata(self):
      |    return {
      |        entry.getKey(): entry.getValue()
      |        for entry in self._java_obj.getMetadataJava().entrySet()
      |    }
      |
      |def getInclude(self):
      |    return list(self._java_obj.getIncludeJava())
      |""".stripMargin

  private val ResponsesColumnHelpers: String =
    """
      |def toolCallsColumn(self, outputCol=None):
      |    from pyspark.sql.column import Column
      |    return Column(self._java_obj.toolCallsColumn(outputCol or self.getOutputCol()))
      |
      |def replayItemsColumn(self, outputCol=None):
      |    from pyspark.sql.column import Column
      |    return Column(self._java_obj.replayItemsColumn(outputCol or self.getOutputCol()))
      |""".stripMargin

  private val ChatColumnHelpers: String =
    """
      |def toolCallsColumn(self, outputCol=None):
      |    from pyspark.sql.column import Column
      |    return Column(self._java_obj.toolCallsColumn(outputCol or self.getOutputCol()))
      |""".stripMargin

  private val PromptColumnHelpers: String =
    """
      |def _resolveToolResponseStructCol(self, outputCol, helperName):
      |    if outputCol:
      |        return outputCol
      |    if self.isSet(self.responseStructCol):
      |        responseStructCol = self.getResponseStructCol()
      |        if responseStructCol:
      |            return responseStructCol
      |    raise ValueError(
      |        "{} requires outputCol or a configured responseStructCol; "
      |        "OpenAIPrompt.getOutputCol() is text, not a service response struct".format(helperName)
      |    )
      |
      |def toolCallsColumn(self, outputCol=None):
      |    from pyspark.sql.column import Column
      |    structCol = self._resolveToolResponseStructCol(outputCol, "toolCallsColumn")
      |    return Column(self._java_obj.toolCallsColumn(structCol))
      |
      |def replayItemsColumn(self, outputCol=None):
      |    from pyspark.sql.column import Column
      |    structCol = self._resolveToolResponseStructCol(outputCol, "replayItemsColumn")
      |    return Column(self._java_obj.replayItemsColumn(structCol))
      |""".stripMargin

  val ResponsesMethods: String = Common + ResponsesOnly + ResponsesColumnHelpers
  val ChatMethods: String = Common + ChatColumnHelpers
  val PromptMethods: String = Common + ResponsesOnly + PromptColumnHelpers
}
