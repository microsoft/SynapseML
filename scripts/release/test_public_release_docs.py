# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
PUBLIC_DOCS = [
    ROOT / "scripts" / "release" / "README.md",
    *sorted((ROOT / ".github" / "skills" / "synapseml-release").rglob("*.md")),
    *sorted((ROOT / "reviews" / "pr-2628").glob("*.md")),
]


@pytest.mark.parametrize("path", PUBLIC_DOCS, ids=lambda path: path.name)
def test_public_release_docs_do_not_publish_private_operator_context(path):
    text = path.read_text(encoding="utf-8")
    assert "_git/" not in text
    assert "C:\\Users\\" not in text
    assert "/Users/" not in text
    assert ".copilot/session-state/" not in text
    assert "release_plan_base64=" not in text


def test_public_guide_preserves_required_source_and_approval_boundaries():
    text = PUBLIC_DOCS[0].read_text(encoding="utf-8")
    for required in (
        "master",
        "spark4.0",
        "spark4.1",
        "--dry-run",
        "--approve-plan",
        "--inspect-lock",
        "signing",
        "Same-coordinate Maven retries are unsupported",
    ):
        assert required in text
    assert "Encoding or compressing a document does not redact it" in text


def test_consumer_wheel_gate_precedes_tagging_in_both_procedures():
    guide = PUBLIC_DOCS[0].read_text(encoding="utf-8")
    skill = (ROOT / ".github" / "skills" / "synapseml-release" / "SKILL.md").read_text(
        encoding="utf-8"
    )
    assert guide.index("Consumer-wheel gate") < guide.index("## 1.")
    assert guide.index("Consumer-wheel gate") < guide.index(
        '--approve-plan "$REVIEWED_PLAN_ID" --dispatch'
    )
    assert guide.index("read master's classic protection") < guide.index(
        '--approve-plan "$REVIEWED_PLAN_ID" --dispatch'
    )
    assert skill.index("Consumer-wheel gate") < skill.index("bootstrap entry point")
