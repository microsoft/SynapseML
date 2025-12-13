# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

from synapse.ml.services.openai._OpenAIPrompt import _OpenAIPrompt
from pyspark.ml.common import inherit_doc


@inherit_doc
class OpenAIPrompt(_OpenAIPrompt):
    pass
