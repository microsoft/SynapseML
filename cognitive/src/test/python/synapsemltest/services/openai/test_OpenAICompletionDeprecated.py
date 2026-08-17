# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import sys
import unittest
import warnings

_MODULE_NAME = "synapse.ml.services.openai.OpenAICompletion"
_PACKAGE_NAME = "synapse.ml.services.openai"
_WARNING_TEXT = "OpenAICompletion has been removed"


def _clear_openai_completion_imports():
    sys.modules.pop(_MODULE_NAME, None)
    package = sys.modules.get(_PACKAGE_NAME)
    if package is not None:
        package.__dict__.pop("OpenAICompletion", None)


def _has_openai_completion_warning(caught):
    return any(
        issubclass(warning.category, FutureWarning)
        and _WARNING_TEXT in str(warning.message)
        for warning in caught
    )


class TestOpenAICompletionDeprecated(unittest.TestCase):
    def test_package_import_warns(self):
        _clear_openai_completion_imports()

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            from synapse.ml.services.openai import OpenAICompletion

        package = sys.modules[_PACKAGE_NAME]
        if hasattr(package, "__getattr__"):
            self.assertIsInstance(OpenAICompletion, type)
        else:
            self.assertIsInstance(OpenAICompletion.OpenAICompletion, type)
        self.assertTrue(_has_openai_completion_warning(caught))

    def test_submodule_import_warns(self):
        _clear_openai_completion_imports()

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            from synapse.ml.services.openai.OpenAICompletion import OpenAICompletion

        self.assertIsInstance(OpenAICompletion, type)
        self.assertTrue(_has_openai_completion_warning(caught))

    def test_instantiation_warns_and_raises(self):
        _clear_openai_completion_imports()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            from synapse.ml.services.openai.OpenAICompletion import OpenAICompletion

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            with self.assertRaisesRegex(RuntimeError, _WARNING_TEXT):
                OpenAICompletion()

        self.assertTrue(_has_openai_completion_warning(caught))


if __name__ == "__main__":
    result = unittest.main()
