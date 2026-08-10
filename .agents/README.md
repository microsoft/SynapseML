# Agent configuration

Copilot CLI discovers project skills from `.github/skills/<skill-name>/`.

Do not add `SKILL.md` files under `.agents/skills/`. Keep repo-versioned skills in `.github/skills/` so Copilot CLI can load them consistently.

This directory remains only as a compatibility pointer for agents or tools that inspect `.agents`.
