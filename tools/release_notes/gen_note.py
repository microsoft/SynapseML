# file: release_note_gen.py
"""
Barebones helper to call Azure OpenAI and generate a one-line release note.

Usage example:
    from release_note_gen import generate_release_note
    result = generate_release_note(commit_text)
    print(result)
"""

import json
import logging
import os
import time
from typing import Any, Dict, Optional
from openai import AzureOpenAI
from dotenv import load_dotenv

# Load .env variables
load_dotenv()

client = AzureOpenAI(
    api_key=os.getenv("AZURE_AI_APIKEY"),
    azure_endpoint=os.getenv("AZURE_AI_ENDPOINT"),
    api_version="2024-10-21",
)


def _extract_json_block(content: str) -> str:
    text = content.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        if lines and lines[0].startswith("```"):
            closing_index = len(lines)
            if len(lines) > 1 and lines[-1].strip() == "```":
                closing_index -= 1
            text = "\n".join(lines[1:closing_index])
    return text

def generate_release_note(
    commit_text: str,
    *,
    max_retries: int = 2,
    retry_delay: float = 1.5,
) -> Dict[str, Any]:
    """Generate a concise SynapseML-style release note for a given commit."""
    prompt = """
Output json of the form:
{
    "commit_hash": "<commit hash>",
    "pr_number": <PR number>,
    "pr_title": "<PR title>",
    "pr_description": "<PR description (omit boilerplate)>",
    "author": "<author name>",
    "author_alias": "<author alias>",
    "merged_at": "<date in datetime format>"
    "ONELINE": "<ONELINE as defined below>",
    "is_candidate_for_release_highlight": <true/false>,
    "pr_type": <one of "feat","fix","docs","perf","build","ci","refactor","chore","test">,
    "feature_category": "<one of "AI Functions","HuggingFace","Azure AI Foundry","General","Build/CI","Tests","Examples","Open AI", "Deep Learning", "Bug Fixes", "Documentation", "Maintenance">,
    "repo_name": "<SynapseML/SynapseML-Internal>",
}
ONELINE: a one-line release note for on this commit as would be included in a release 
announcement to highlight it as a contribution to SynapseML (max 140 characters). 
Output format:(Add/Update/Fix/Improve/Remove/Maintain/Upgrade/Support/<or whatever else fits inline with this type of thing and makes sense) <etc> [#<PR number>]

or in the case that there is no associated PR, ommit the [#<PR number>] part.

""" + f"{commit_text}"

    attempt = 0
    last_error: Optional[Exception] = None
    while attempt <= max_retries:
        response = client.chat.completions.create(
            model=os.getenv("AZURE_AI_DEPLOYMENT"),
            messages=[
                {"role": "system", "content": "Return only JSON structured output and nothing else."},
                {"role": "user", "content": prompt},
            ],
        )

        raw_content = response.choices[0].message.content or ""
        candidate = _extract_json_block(raw_content)

        try:
            parsed = json.loads(candidate)
            if isinstance(parsed, dict):
                return parsed
            raise TypeError(f"Expected dict JSON payload, got {type(parsed).__name__}")
        except (json.JSONDecodeError, TypeError) as exc:
            last_error = exc
            logging.getLogger(__name__).warning(
                "Failed to parse release note JSON on attempt %s/%s: %s", attempt + 1, max_retries + 1, exc
            )
            attempt += 1
            if attempt > max_retries:
                break
            time.sleep(retry_delay * attempt)

    raise ValueError("Failed to obtain valid JSON release note") from last_error
