---
name: code-review
description: Quick review checklist for python and scala code changes before callings it done. 
---

# Code Review

Use this skill when reviewing SynapseML changes.

## Steps

1. Inspect diff: `git --no-pager diff --stat && git --no-pager diff`
2. Run Scala style: `sbt scalastyle test:scalastyle`
3. Run Python format check: `black --check --extend-exclude 'docs/' .`
4. Run targeted tests for touched code.
5. Report only concrete issues with file paths and fixes.
