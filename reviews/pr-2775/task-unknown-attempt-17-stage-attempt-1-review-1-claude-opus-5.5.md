---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 17
stage_attempt: 1
round: 1
timestamp: "2026-10-17T11:31:44Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "b052c03512bac6309b1635ce189f6190b4049dcc203796307d3f3bc5fadb1f30"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-17-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read all of `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`. Error messages from `command_json` hide CLI stderr. The `AzureClient` checks cover the endpoint allowlist, refused redirects, no token output, the shared deadline, and timeline 404/empty → `None` only. `list` stops on repeated IDs. `verify_merge` requires ordered `[base, head]` parents. `verify_build` checks repo, sourceBranch, sourceVersion and reason. `summarize_timeline` gates leaf containers, detects parent cycles, and uses the larger of listed and reported issue counts. `collect_tests` handles attempts and inferred publications. `main` rechecks build and timeline stability. Every malformed scalar path I traced ends in `EvidenceError`.
- [x] Read all of `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`. In timeline mode the merge is verified once, `agent_loss` is deduplicated per (identifier, attempt), `azure_completed`/`attention_required` exit 4, and the deadline stays tied to kickoff. `sys.dont_write_bytecode` is set before the sibling import.
- [x] Read `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1` lines 1–690. Covered: `Get-RestEvidence`, rules and classic protection, workflow PR scoping, the fork-head lookup (`state=all`, uniqueness by head SHA), required/unproven checks, the revision recheck, the readiness states, and the `-RunPipeline` guards (one PR, both SHAs, complete evidence, open PR, no `-WaitForReview`).
- [x] Read all of `.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py`. Checked: absolute `PATH` entries outside the cwd with PATHEXT expansion, `GIT_*` stripping, `--no-replace-objects`, `core.fsmonitor=false`, ls-tree mode/type checks, index equal to tree, `ls-files --others` without exclude flags (so ignored files are listed), and byte comparison that tolerates only CRLF differences in non-binary blobs.
- [x] Cross-checked the docs against the code: `references/ci-evidence.md`, `trusted-guidance.md`, `review-gauntlet.md`, `readiness-gates.md`, `ci-triage.md`, `loop-control.md`, both `SKILL.md` files, and `synapseml-external-contributor-review/references/ci-delegation.md`. Every anchor the diff links to exists.
- [x] Checked `pipeline.yaml`. It has jobs only (no explicit stages), so timeline Job identifiers are `<phase>.<job>` and match the collector's `phaseName.jobName` key. `CIHelpers` runs `tools/ci/tests/`. The diff does not change pipeline or workflow definitions.
- [x] Inspected `tools/ci/tests/test_get_pr_readiness.py` (its fake `gh` prints one line per page) and the 31 tests in `tools/ci/tests/test_collect_ci_evidence.py`. No test covers zero-page successful output or a differently-cased Azure aggregate.
- [ ] I ran no commands or tests; this review was read-only. I did not re-run the pytest counts or live GitHub/Azure results reported in the request.

## Issues

### Issue 1: Empty but successful `gh api` output is read as "no branch rules" or "no classic protection"
- **Severity**: Low
- **File**: .github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1
- **Line(s)**: 235–268 (mainly 250–263); results used at 312–313 and 326–341
- **Description**: `Get-RestEvidence` only rejects a body that is literally `null`. If `gh` exits 0 and prints nothing, `($text -join "").Trim()` is `""` and the paged `foreach ($page in $text)` loop runs zero times. No collection error is recorded.
  - For the paged rules read (line 312), `$rules` becomes `@()`. That means no rule-required checks, no `pull_request` approval rules and no unevaluated rules, while `evidenceComplete` stays true.
  - The non-paged classic-protection read (line 328) has no explicit empty-output check. It depends on how an empty native result binds to `ConvertFrom-Json`, so it can quietly leave `$classic = $null`.
  - The branch, workflow, fork-lookup and fresh-branch reads still fail closed through later checks. Only the two policy reads are exposed.
- **Risk**: `ci-evidence.md` says API failures produce `collectionErrors`, "not an empty policy". An empty successful response (a proxy or CLI wrapper problem, or HTTP 204) would instead report `humanApproval: not_required` and drop rule-required checks while `evidenceComplete: true`. `remoteEngineeringGatesClear` could then be true even though an approval or required-check rule exists. This is unlikely because GitHub returns `[]` or an object for these endpoints. It is still a fail-open on the exact policy evidence the helper exists to protect.
- **Suggested Fix**: Treat no output as invalid evidence in both modes before parsing, for example: collect non-null lines, and throw `Empty evidence.` if there are none or they join to `''`/`null`. Add regressions to `test_get_pr_readiness.py`:
  - `data["rules"] = []`, so the fake `gh` prints zero pages.
  - A fixture flag that makes enabled classic protection print nothing with exit 0.
  - Both should assert `evidenceComplete` is false, a collection error is present, and `humanApproval` is `unknown`.

### Issue 2: The "exact" Azure aggregate match is case-insensitive
- **Severity**: Low
- **File**: .github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1
- **Line(s)**: 429, 437, 559; also 336 and 434 (`Sort-Object -Unique`)
- **Description**: The docs say the aggregate is exact: `ci-evidence.md` says it "must match `microsoft.SynapseML` exactly", and the parameter comment says "Azure's aggregate is exact". Rule-derived checks use case-sensitive `-cin`/`-cnotin` (lines 434, 437, 665). The aggregate itself is compared with PowerShell's case-insensitive operators:
  - `-eq` for presence (line 429) and for the unproven check (line 437).
  - `-notin` for the `-RunPipeline` trigger (line 559).
  - `Sort-Object -Unique` (lines 336 and 434) is also case-insensitive, so two required contexts that differ only by case collapse into one before the case-sensitive presence test.
- **Risk**: A successful check named `Microsoft.SynapseML` passes the aggregate presence gate and suppresses the guarded `-RunPipeline` trigger, although GitHub matches required checks case-sensitively. The snapshot can then report an empty `missingRequiredChecks` and `remoteEngineeringGatesClear: true` without the real aggregate. Impact is limited because check-issuer identity remains a manual gate, and a spoofer could use the exact name anyway. This is mainly a spec and consistency defect.
- **Suggested Fix**:
  - Use `-ceq` at line 429 and `-cnotin` at line 559, and `-ceq` at line 437 for consistency.
  - Add `-CaseSensitive` to `Sort-Object -Unique` at lines 336 and 434.
  - Add a regression with a SUCCESS rollup check named `Microsoft.SynapseML`, asserting that `microsoft.SynapseML` stays in `missingRequiredChecks` and remote gates are not clear.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Fixed
- **What changed**: Reject empty or whitespace-only successful REST output before parsing either paged or non-paged evidence.
- **Why**: Missing policy evidence must not report that checks or human approval are unnecessary.
- **How verified**: Zero-page rules and empty classic-protection regressions failed before the fix and passed afterward. Full Windows readiness suite: `python -B -m pytest -q tools/ci/tests/test_get_pr_readiness.py --tb=short` passed (24 tests, 44 subtests).

### Issue 2
- **Status**: Fixed
- **What changed**: Match the canonical Azure aggregate case-sensitively for presence, success, and triggering; retain distinct case-sensitive rule contexts during deduplication. Other caller-supplied prefix matching remains unchanged.
- **Why**: A differently cased check cannot stand in for a required exact context.
- **How verified**: Wrong-case aggregate and case-distinct context regressions failed before the fix. The same full Windows suite passed after the fix; its CI trigger was an offline fixture, not a real build request.

The original feedback above is unchanged. Its original artifact SHA-256 was
`8e47ef0c74ada44a90643879a1e4335f75f9788220a1a03f7caa7645e51eaaa0`.
The reviewed diff SHA-256 was
`817cb641a32a517f3068537b4b2a78d3ccabb8af44b731bba47e7615df6a94ac`,
against target/merge base `0543bf7e5266c587207ae8c28c3b696deeed3ed1`.
This pass stopped on findings; it does not qualify the revised patch.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; approximately 3700 added lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 17,
  "completedPriorPasses": 16,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}
