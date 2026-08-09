# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import warnings

__all__ = ["OpenAICompletion"]

_OPENAI_COMPLETION_DEPRECATION_MESSAGE = (
    "OpenAICompletion has been removed because the legacy OpenAI Completions API "
    "is deprecated and retired. Use OpenAIResponses, OpenAIChatCompletion, or "
    "OpenAIPrompt with setApiType('chat_completions') or setApiType('responses') instead."
)


def warn_openai_completion_deprecated(stacklevel=2):
    warnings.warn(
        _OPENAI_COMPLETION_DEPRECATION_MESSAGE,
        FutureWarning,
        stacklevel=stacklevel,
    )


warn_openai_completion_deprecated(stacklevel=2)


class OpenAICompletion:
    def __init__(self, *args, **kwargs):
        warn_openai_completion_deprecated(stacklevel=2)
        raise RuntimeError(_OPENAI_COMPLETION_DEPRECATION_MESSAGE)
