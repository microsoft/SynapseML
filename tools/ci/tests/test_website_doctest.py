# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import importlib.util
import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location(
    "website_doctest", REPO_ROOT / "website" / "doctest.py"
)
WEBSITE_DOCTEST = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(WEBSITE_DOCTEST)


def test_ci_cognitive_samples_keep_key_auth_and_use_central_region():
    markdown = """```python
cognitiveKey = os.environ.get("COGNITIVE_API_KEY", getSecret("cognitive-api-key"))
model = (DetectFace()
    .setSubscriptionKey(cognitiveKey)
    .setLocation("eastus"))
```
```scala
model.setSubscriptionKey(cognitiveKey).setLocation("eastus")
```
"""

    transformed = WEBSITE_DOCTEST.configure_ci_samples(markdown, "_Face.md")

    assert ".setSubscriptionKey(cognitiveKey)" in transformed
    assert 'getSecret("cognitive-api-key-central")' in transformed
    assert '.setLocation("centralus")' in transformed
    assert 'model.setSubscriptionKey(cognitiveKey).setLocation("eastus")' in transformed


def test_ci_translator_samples_use_aad_on_the_central_endpoint():
    markdown = """```python
translatorKey = os.environ.get("TRANSLATOR_KEY", getSecret("translator-key"))
model = (Translate()
    .setSubscriptionKey(translatorKey)
    .setLocation("eastus"))
```
"""

    transformed = WEBSITE_DOCTEST.configure_ci_samples(markdown, "_Translator.md")

    assert ".setAADToken(cognitiveToken)" in transformed
    assert 'getSecret("translator-key")' not in transformed
    assert '.setSubscriptionRegion("centralus")' in transformed
    assert (
        '.setEndpoint("https://mmlspark-cs-central.cognitiveservices.azure.com/'
        'translator/text/v3.0/")'
    ) in transformed


def test_ci_document_translator_uses_aad_without_requiring_a_location():
    markdown = """```python
translatorKey = os.environ.get("TRANSLATOR_KEY", getSecret("translator-key"))
translatorName = os.environ.get("TRANSLATOR_NAME", "mmlspark-translator")
model = (DocumentTranslator()
    .setSubscriptionKey(translatorKey)
    .setServiceName(translatorName))
```
"""

    transformed = WEBSITE_DOCTEST.configure_ci_samples(markdown, "_Translator.md")

    assert ".setSubscriptionKey(translatorKey)" not in transformed
    assert ".setAADToken(cognitiveToken)" in transformed
    assert ".setServiceName(translatorName)" in transformed


def test_all_live_cognitive_quick_examples_target_working_ci_auth():
    docs = REPO_ROOT / "docs" / "Quick Examples" / "transformers" / "cognitive"

    for name in WEBSITE_DOCTEST.COGNITIVE_KEY_SAMPLES:
        transformed = WEBSITE_DOCTEST.configure_ci_samples(
            (docs / name).read_text(encoding="utf-8"), name
        )
        python_blocks = "\n".join(
            re.findall(r"```python\n(.*?)\n```", transformed, flags=re.DOTALL)
        )
        assert '.setLocation("eastus")' not in python_blocks
        assert ".setAADToken(" not in python_blocks

    translator = WEBSITE_DOCTEST.configure_ci_samples(
        (docs / "_Translator.md").read_text(encoding="utf-8"),
        "_Translator.md",
    )
    translator_blocks = "\n".join(
        re.findall(r"```python\n(.*?)\n```", translator, flags=re.DOTALL)
    )
    assert ".setSubscriptionKey(translatorKey)" not in translator_blocks


def test_markdown_is_preserved_when_ci_transformation_fails(tmp_path, monkeypatch):
    markdown = "_Face.md"
    source = "<!--pytest-codeblocks:cont-->\noriginal content\n"
    path = tmp_path / markdown
    path.write_text(source, encoding="utf-8")

    def fail_transformation(content, markdown_name):
        raise RuntimeError("transformation failed")

    monkeypatch.setattr(WEBSITE_DOCTEST, "configure_ci_samples", fail_transformation)

    with pytest.raises(RuntimeError, match="transformation failed"):
        WEBSITE_DOCTEST.add_python_helper_to_markdown(
            tmp_path, markdown, "test-version"
        )

    assert path.read_text(encoding="utf-8") == source
