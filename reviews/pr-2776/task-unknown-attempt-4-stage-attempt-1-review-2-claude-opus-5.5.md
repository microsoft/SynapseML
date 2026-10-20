---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 4
stage_attempt: 1
round: 2
timestamp: "2026-10-20T09:18:15Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "6fb6f09b531daddad2eb4a85dd802dbb41ef01afdf715540f579ebaf8458aea4"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2776/task-unknown-attempt-4-stage-attempt-1-review-2-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Scala changes are limited to documentation and the help string, as AGENTS.md requires.** `core/src/main/scala/com/microsoft/azure/synapse/ml/recommendation/SAR.scala:38-39` changes only Scaladoc. Lines 404-405 change only the `IntParam` doc string. The param name, type, `this` owner and the default at line 424 (`supportThreshold -> 4`) are unchanged. No public signature or persisted parameter shape changes, because Spark persists param values rather than docs. The wrapped two-line constructor matches the neighbouring `similarityFunction` param style. Scala is the right single source because generated Python, R and .NET wrappers inherit the doc string. A search of `core/src/main/python` found no hand-written SAR wrapper that would need the same change.
- [x] **The new help string matches the implementation.** `SAR.scala:235` builds distinct (user, item) pairs. Line 246 builds a binary user vector per item. Line 220 uses `countDistinct` for `occ`. Line 267 applies `cooccurrence >= getSupportThreshold` to every index, including self-pairs. "Minimum distinct-user co-occurrence count for nonzero item-pair similarity" is therefore accurate. The class doc at lines 38-39 ("user-item interaction history") is consistent with the identifier contract at lines 41-42.
- [x] **The page's input contract matches the code.**
  - Docs page lines 20-26: `rating` and `time` are opt-in by column presence (`SAR.scala:178-181`).
  - Time values are cast to string and parsed with `activityTimeFormat` (`:166`, `:181`).
  - The default `startTime` is `Date.toString()` parsed with the default `startTimeFormat` (`:163`), so "leave both unset" is correct.
  - `timeDecayCoeff` is the half-life in days (`:168`: `2^(-Δmin/(coeff·1440))`).
  - Defaults are `user`, `item` and `rating` (`core/src/main/scala/org/apache/spark/ml/recommendation/RecommendationHelper.scala:221-223`) and `time` (`SAR.scala:424`).
  - The `similarityFunction` values are case-sensitive, and any other string falls back to raw co-occurrence (`:269-271`), as page line 71 says.
- [x] **The worked examples check out by hand.** Item sets from the affinity table:
  - I1 = {U1, U2, U3, U4, U5}
  - I2 = {U1, U2, U5, U7}
  - I3 = {U1, U2, U4, U5}
  - I4 = {U2, U3, U4, U5}
  - I5 = {U3, U5, U6}

  Every cell matches docs lines 50-54, including the corrected I1∩I4 = 4. The lift example holds: I4·I2 = 2/16 and I4·I5 = 2/12. User 1's recommendation vector is [44, 34.5, 39, 33.5, 15.5]. The new-user vectors are [2, 1, 1, 2, 3] and [8, 9, 7, 6, 5]. The interpretability terms 20 + 6 + 7.5 = 33.5 (lines 177-178) are correct. The ranking text at lines 121, 144 and 165 agrees with the bolded candidate cells.
- [x] **The behavioural claims about recommendations are confirmed in code.** `SARModel.scala:327` computes affinity × similarityᵀ, which is S·a for symmetric S. `topCandidates` (`:296-304`) ranks all items without excluding seen items. `RankingAdapter.scala:146-149` calls `recommendForAllUsers` without filtering either. So "SAR does not automatically exclude previously seen items" holds even on the adapter path. No incremental-update API exists, as line 165 says.
- [x] **The new test follows the established `website/test` pattern.**
  - `website/test/sarDocs.test.js` uses the same `node:test`, `node:assert/strict`, `fs` and `path`-from-`__dirname` imports and top-level document read as `quantileRegressionScalaDocs.test.js` and `rSetupDocs.test.js`. Its camelCase `*Docs.test.js` name matches theirs.
  - It handles CRLF line endings (`:14`, `:64-65`, and `slice(2, -1)` drops the trailing `\r`).
  - It relies on Node features covered by `website/package.json:5-6` (`"node": ">=24.0"`).
  - `node --test` (`:9`) discovers it.
  - CI enforces it: `.github/workflows/website-deploy.yml:11-16` runs on PRs touching `docs/**` and `website/**`, and lines 56-61 run `npm test` there.
- [x] **Scoping the change to current docs is correct.** The test reads only `docs/` and does not iterate `website/versioned_docs`, unlike `rSetupDocs.test.js`. That is the right choice. The 21 historical `Smart Adaptive Recommendations.md` snapshots keep the old text, and the new page documents behaviour that exists only on master (string identifiers, `SAR.scala:41-42`). Backporting the page or asserting against snapshots would be incorrect.
- [x] **No stale copies of the old text remain outside the historical snapshots.** Searches of `core/src`, `docs`, `tools` and `website/src` found no other "Minimum number of ratings per item" or "items description" text.
- [x] **Observed design trade-off, judged not a defect.** The test finds tables by position (`sarDocs.test.js:13-14`) and compares floats exactly (`:56-57`, `:76-78`). Today's values are exact binary fractions. Inserting or reordering a table fails loudly, through the non-numeric cell assertion (`:20`), the even-pairing check (`:42`) or the User 1 vector check (`:43-47`), so incoherent docs cannot pass silently. Matching tables by header label would be optional hardening.
- [x] **Pre-existing, outside-manifest observations, not counted as issues.**
  - `docs/Quick Examples/estimators/core/_Recommendation.md:291,360` and `SARSpec.scala:35` use `"jacccard"`. This silently selects co-occurrence, exactly as the new page line 71 now documents. It belongs in a separate follow-up.
  - The unchanged wording at docs line 144 ("Assuming that a same item isn't recommended again") agrees with the caller-side filtering framing at lines 121 and 165; only the phrasing is awkward.
- [ ] **Build and test execution not performed by this reviewer.** This was a read-only lens. The npm test, website build, sbt, scalastyle and codegen results above are driver-supplied evidence. Remote CI for the pending commit has not yet run.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "Repository six-lens requirement overrides the low-complexity estimate.",
    "models": "Installed review-code direct Medium contract, complementary to the OpenAI primary.",
    "stageBehavior": "Repository review-gauntlet.md requires six sequential themes unless parallel mode was requested.",
    "safety": "Trusted external-contributor guidance and the explicit maintainer follow-up request."
  },
  "changeSize": "Three source files: one algorithm page, two Scala documentation/help-text corrections, and three JavaScript documentation consistency tests.",
  "cyclomaticComplexity": "Low: prose, parameter help text, deterministic table comparisons and arithmetic parsing; no algorithm or service changes.",
  "rationale": "Use one complementary reviewer per lens for the mandatory six-lens policy. Require all six clean on the same frozen source patch.",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoningEffort": "max",
  "modelSource": "Current task-tool live catalog supports claude-opus-5.5 with max reasoning.",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md from independently trusted guidance at 5a3e29f1a3c0d3b9649a2168279ef4f3be06dcc4",
  "phase": "Pre-commit pass 4; remote validation follows publication."
}
