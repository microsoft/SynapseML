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


def test_ci_samples_use_aad_without_changing_scala_examples():
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

    transformed = WEBSITE_DOCTEST.use_aad_for_ci_samples(markdown, "_Face.md")

    assert "cognitiveToken = getAccessToken()" in transformed
    assert ".setAADToken(cognitiveToken)" in transformed
    assert '.setCustomServiceName("mmlspark-cs")' in transformed
    assert "model.setSubscriptionKey(cognitiveKey).setLocation" in transformed


def test_ci_samples_rewrite_cognitive_auth_when_location_is_not_adjacent():
    markdown = """```python
cognitiveKey = os.environ.get("COGNITIVE_API_KEY", getSecret("cognitive-api-key"))
model = (RecognizeDomainSpecificContent()
    .setSubscriptionKey(cognitiveKey)
    .setModel("celebrities")
    .setLocation("eastus"))
```
"""

    transformed = WEBSITE_DOCTEST.use_aad_for_ci_samples(markdown, "_ComputerVision.md")

    assert ".setSubscriptionKey(cognitiveKey)" not in transformed
    assert ".setAADToken(cognitiveToken)" in transformed
    assert '.setCustomServiceName("mmlspark-cs")' in transformed


def test_ci_translator_samples_use_custom_endpoint_and_region():
    markdown = """```python
translatorKey = os.environ.get("TRANSLATOR_KEY", getSecret("translator-key"))
model = (Translate()
    .setSubscriptionKey(translatorKey)
    .setLocation("eastus"))
```
"""

    transformed = WEBSITE_DOCTEST.use_aad_for_ci_samples(markdown, "_Translator.md")

    assert ".setAADToken(cognitiveToken)" in transformed
    assert 'getSecret("translator-key")' not in transformed
    assert '.setSubscriptionRegion("eastus")' in transformed
    assert (
        '.setEndpoint("https://mmlspark-cs.cognitiveservices.azure.com/'
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

    transformed = WEBSITE_DOCTEST.use_aad_for_ci_samples(markdown, "_Translator.md")

    assert ".setSubscriptionKey(translatorKey)" not in transformed
    assert ".setAADToken(cognitiveToken)" in transformed
    assert ".setServiceName(translatorName)" in transformed


def test_ci_speech_samples_use_the_required_aad_credential_shapes():
    markdown = """```python
cognitiveKey = os.environ.get("COGNITIVE_API_KEY", getSecret("cognitive-api-key"))
rest = (SpeechToText()
    .setSubscriptionKey(cognitiveKey)
    .setLocation("eastus"))
sdk = (SpeechToTextSDK()
    .setSubscriptionKey(cognitiveKey)
    .setLocation("eastus"))
```
"""

    transformed = WEBSITE_DOCTEST.use_aad_for_ci_samples(markdown, "_SpeechToText.md")

    assert ".setAADToken(speechToken)" in transformed
    assert ".setAADToken(cognitiveToken)" in transformed
    assert ".setCognitiveServiceResourceId(cognitiveResourceId)" in transformed


def test_all_live_cognitive_quick_examples_are_rewritten_for_ci():
    docs = REPO_ROOT / "docs" / "Quick Examples" / "transformers" / "cognitive"
    names = [
        "_ComputerVision.md",
        "_Face.md",
        "_FormRecognizer.md",
        "_SpeechToText.md",
        "_TextAnalytics.md",
        "_Translator.md",
    ]

    for name in names:
        transformed = WEBSITE_DOCTEST.use_aad_for_ci_samples(
            (docs / name).read_text(encoding="utf-8"), name
        )
        python_blocks = "\n".join(
            re.findall(r"```python\n(.*?)\n```", transformed, flags=re.DOTALL)
        )
        assert not re.search(
            r"\.setSubscriptionKey\((?:cognitiveKey|textKey|translatorKey)\)",
            python_blocks,
        )


def test_markdown_is_preserved_when_ci_transformation_fails(tmp_path, monkeypatch):
    markdown = "_Face.md"
    source = "<!--pytest-codeblocks:cont-->\noriginal content\n"
    path = tmp_path / markdown
    path.write_text(source, encoding="utf-8")

    def fail_transformation(content, markdown_name):
        raise RuntimeError("transformation failed")

    monkeypatch.setattr(WEBSITE_DOCTEST, "use_aad_for_ci_samples", fail_transformation)

    with pytest.raises(RuntimeError, match="transformation failed"):
        WEBSITE_DOCTEST.add_python_helper_to_markdown(
            tmp_path, markdown, "test-version"
        )

    assert path.read_text(encoding="utf-8") == source
