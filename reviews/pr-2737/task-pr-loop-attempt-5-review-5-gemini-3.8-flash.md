# Code Review Report — Round 5: Validation & Tabletop Coverage

- **Model**: gemini-3.8-flash
- **Round**: 5 (Validation coverage, tabletop traces, missing cases, verifiable claims)
- **Mode**: Direct Sequential Gauntlet
- **Target**: `681bd96990c421de3b91d2b1bf8f8f470764199d` (`upstream/master`)
- **Source**: `chore/pr-loop-feedback-20260924`
- **Scope**: `AGENTS.md`, `.github/skills/synapseml-pr-loop/SKILL.md`, `.github/skills/synapseml-pr-loop/references/readiness-gates.md`, `.github/skills/synapseml-pr-loop/references/loop-control.md`

## Executive Summary
The workflow specification defines robust validation rules, bounded loops, and evidence gates. The 13 tabletop scenarios in `loop-control.md` cover core operational paths (read-only external PRs, missing builds, stale review heads, and rebase recovery). However, Round 5 review identifies gaps in manifest verification for file deletions, missing tabletop traces for newly specified boundary rules, and gauntlet pass cap exhaustion.

## Findings

### 1. Incomplete Verification Recipe for Deleted Files in Manifest Recomputation
- **Location**: `.github/skills/synapseml-pr-loop/references/loop-control.md` (Evidence invalidation)
- **Evidence**: The specification dictates recording deletions in the pre-commit manifest with `mode: "000000"` and `blob: null`. It then prescribes: *"After commit and before readiness, recompute the manifest from HEAD and require an exact match"*.
- **Analysis**: A direct tree inspection of `HEAD` (`git ls-tree -r HEAD`) does not contain tombstones or paths for deleted files. Recomputing the manifest purely from `HEAD` without diffing against `<merge-base>` cannot produce the `mode: "000000"` / `blob: null` entries present in the frozen index manifest. This makes post-commit verification fail or unverifiable whenever a changeset deletes files.
- **Remediation**: Explicitly state that post-commit manifest recomputation derives the path set from `git diff --name-status --no-renames <merge-base> HEAD`, resolving present files from `HEAD` and deleted files as `mode: "000000"` / `blob: null`.

### 2. Missing Tabletop Scenarios for New Edge-Case Contracts
- **Location**: `.github/skills/synapseml-pr-loop/references/loop-control.md` (Workflow acceptance scenarios)
- **Evidence**: The table lists 13 scenarios, but omits tabletop traces for critical procedures newly introduced:
  1. *Sensitive Draft Exclusion*: Step 8 / `loop-control.md` defines redacting private drafts with an exclusion note and rerunning on the same frozen patch as a new attempt. No scenario verifies that this exclusion note does not count as a clean pass or bypass reviewer retry.
  2. *Byte-Budget Prompt Overflow*: Step 8 dictates that an oversized assembled prompt remains blocked unless an explicit split under one manifest is user-approved. No scenario verifies agent stopping behavior on budget overflow.
  3. *Gauntlet Budget Exhaustion*: Scenario 10 verifies "Same failure three times", but no scenario traces reaching the cap of "2 final CI-qualified gauntlet passes" when failures differ between passes.
- **Remediation**: Add explicit rows in the `Workflow acceptance scenarios` table for draft privacy redaction, prompt byte-budget overflow blocking, and distinct-failure gauntlet pass cap exhaustion.

### 3. Unverifiable Manual Prompt Assembly Schema Integrity
- **Location**: `.github/skills/synapseml-pr-loop/SKILL.md` (Step 8)
- **Evidence**: When the generator rejects external paths, prompts are manually assembled from `REVIEW-PROMPTS.md`. Step 8 enforces theme, model selection, pass criteria, and byte budget, but prescribes no structural validation step ensuring the manual assembly matches the parser contract of downstream reviewers before dispatch.
- **Remediation**: Specify a lightweight contract validation check (e.g. verifying expected theme headings and diff delimiters) prior to dispatching manually assembled prompts.

## Static Review Limitations

## Driver dispositions and verification

1. Rebutted. The documented manifest retains explicit deletion records and
   requires comparison against the complete merge-base-to-HEAD changed-path set.
   It does not prescribe `git ls-tree HEAD` as the source of deleted paths.
   A disposable Git-index/tree probe recreated added, modified, and deleted
   records from the diff and final tree, matched the canonical frozen manifest,
   and verified CRLF/LF normalization. A separate probe rejected an extra
   unreviewed path. Canonical ordering was also verified. All three tests passed.
2. Additional tabletop cases were traced without changing source requirements:
   an unsafe draft stays private and its excluded attempt is not a clean pass;
   an oversized prompt blocks without an approved complete split; starting a
   third final pass exceeds the two-pass limit even if failures differ.
   Each expected outcome follows the explicit existing rule. The scenario table
   is a selected checklist, not the sole source of these requirements.
3. Rebutted. Manual prompts are natural-language inputs to reviewers, not a
   downstream parser with mandatory headings or diff delimiters. The workflow
   already requires the installed theme, model/pass criteria, explicit full
   manifest, and byte budget. The driver checks those inputs before dispatch.

The original findings are preserved. A fresh round-5 assessment follows on
unchanged source with this verification evidence.

## Original static review limitations
- Static process review only; no live Git executions, API queries, or script invocations were performed.
- Installed tool behaviors (copilot-toolkit generator path constraints and byte budgets) were evaluated against repository documentation.
