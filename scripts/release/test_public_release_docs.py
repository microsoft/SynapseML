# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
GUIDE = ROOT / "scripts" / "release" / "README.md"
SKILL = ROOT / ".github" / "skills" / "synapseml-release" / "SKILL.md"
PUBLIC_DOCS = [
    GUIDE,
    *sorted(SKILL.parent.rglob("*.md")),
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
    text = GUIDE.read_text(encoding="utf-8")
    for required in (
        "master",
        "spark4.0",
        "spark4.1",
        "--dry-run",
        "--approve-plan",
        "--inspect-lock",
        "signing",
        "Same-coordinate Maven retries are unsupported",
        "explicit authorization",
        "next permitted command",
        "Conversation history alone is not the release ledger",
        "payload",
        "RECORD",
        "unchanged primary candidate",
    ):
        assert required in text
    assert "Encoding or compressing a document does not redact it" in text


def test_consumer_wheel_gate_precedes_tagging_in_both_procedures():
    guide = GUIDE.read_text(encoding="utf-8")
    skill = SKILL.read_text(encoding="utf-8")
    assert guide.index("Consumer-wheel gate") < guide.index("## 1.")
    assert guide.index("Consumer-wheel gate") < guide.index(
        '--approve-plan "$REVIEWED_PLAN_ID" --dispatch'
    )
    assert guide.index("read master's classic protection") < guide.index(
        '--approve-plan "$REVIEWED_PLAN_ID" --dispatch'
    )
    assert skill.index("Consumer-wheel gate") < skill.index("bootstrap")


def test_operator_and_agent_share_one_early_entry_procedure():
    guide = GUIDE.read_text(encoding="utf-8")
    skill = SKILL.read_text(encoding="utf-8")
    assert len(guide[: guide.index("## 1.")].splitlines()) < 60
    assert len(skill.splitlines()) <= 30
    assert sorted(SKILL.parent.rglob("*.md")) == [SKILL]
    assert "README.md#1-preview-and-prepare-source" in skill
    for heading in ("## Handoff at every stop", "## Recovery and limits"):
        assert heading in guide


@pytest.mark.parametrize("path", [GUIDE, SKILL], ids=lambda path: path.name)
def test_runbook_and_skill_local_links_resolve(path):
    for link in re.findall(r"\]\(([^)]+)\)", path.read_text(encoding="utf-8")):
        if "://" in link:
            continue
        filename, _, anchor = link.partition("#")
        target = path.parent / filename if filename else path
        assert target.is_file(), link
        if anchor:
            headings = re.findall(
                r"^#{1,6} (.+)$", target.read_text(encoding="utf-8"), re.MULTILINE
            )
            slugs = {
                re.sub(r"[^\w -]", "", heading.lower()).replace(" ", "-")
                for heading in headings
            }
            assert anchor in slugs, link
