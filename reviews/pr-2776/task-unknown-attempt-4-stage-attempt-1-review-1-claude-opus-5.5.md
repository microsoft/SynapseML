---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 4
stage_attempt: 1
round: 1
timestamp: "2026-10-20T09:08:22Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "00331ab636485400c9a22113c7f1eab6f3c0f5998200a8bb769b37d9c894af16"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2776/task-unknown-attempt-4-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Scala change scope.** I read the full `core/src/main/scala/com/microsoft/azure/synapse/ml/recommendation/SAR.scala`. Only two things changed: the Scaladoc text (L38–39) and the `supportThreshold` help string (L404–405). `setDefault` (L424–426), the param names, the types and the fit logic are unchanged. Spark ML metadata does not store `Param.doc`, so saved models are unaffected. The new help string contains no quotes, `<`, `>` or `&`, so it is safe in the generated Python, R and .NET wrappers.
- [x] **Co-occurrence counts distinct users.** In `SAR.scala`:
  - `createInteractionMatrix` builds the matrix from distinct (user, item) pairs with value 1.0.
  - `itemFeaturesVector` multiplies a user indicator vector by that matrix, so repeated interactions by the same user count once.
  - `collectItemCounts` uses `countDistinct`.
  - This matches the docs at L46, the cell example at L56 and the diagonal description.
- [x] **Similarity function and threshold.** `similarityFeature` sets a value to `0.0f` whenever `cooccurrence < getSupportThreshold`, and this includes self-pairs where index equals itemId. It matches `"jaccard"`, `"lift"` and everything else (`case _`) with case-sensitive string comparison, and anything else gets raw co-occurrence. The defaults are a threshold of 4 and `"jaccard"`. This confirms the docs at L71 and L73 and the new help string.
- [x] **Input contract.** The docs at L20–L26 match the code:
  - `transformSchema` accepts only string or numeric identifiers and requires the rating column to be numeric if it exists.
  - Rating and time are used only if `dataset.columns.contains(...)` finds them. Without a rating column the weight is `lit(1.0)`.
  - The time column is converted with `cast(StringType)` and parsed by `SimpleDateFormat(activityTimeFormat)`.
  - Default column names are `rating` (`RecommendationHelper.scala` L223) and `time`.
  - The decay weight is `2^(-Δminutes/(coeff·1440))`, so `timeDecayCoeff` is a half-life in days.
- [x] **Time-format behavior.** With lenient `SimpleDateFormat`, the `h` field turns an hour of 12 into 0, and AM is assumed when there is no AM/PM marker. Hours 13–23 still parse correctly in lenient mode, so only `12:xx` is affected, exactly as L24 says. When `startTime` is unset, the code parses `Date.toString()` with `startTimeFormat`. That means the advice "leave both unset" is correct: setting only `startTimeFormat` would make that parse fail.
- [x] **Seen items and model updates.** Neither `SARModel` (`rankScores`/`topCandidates`) nor `RankingAdapterModel.transform` (in either `allUsers` or `normal` mode) removes items a user has already seen. That confirms L121. There is no incremental-update API, which confirms L165.
- [x] **Recomputed all arithmetic by hand from the affinity table:**
  - Item 1 and Item 4 share users {U2, U3, U4, U5}, so their co-occurrence is 4 (the corrected value). The rest of the 5×5 matrix also matches.
  - User 1 scores: [44, 34.5, 39, 33.5, 15.5].
  - New-user scores: [2, 1, 1, 2, 3] and [8, 9, 7, 6, 5].
  - Item 4 explanation for User 1 (L177–178): 4×5 + 2×3 + 3×2.5 = 33.5.
  - The lift example at L63 still holds: items 2 and 5 both co-occur twice with Item 4, and occur 4 and 3 times overall.
  - All ranking statements hold after filtering out seen items.
- [x] **New test file `website/test/sarDocs.test.js`.**
  - The regexes work with both LF and CRLF line endings, skip the header and separator rows, and read blank cells as 0.
  - No `-0` values can occur, so the strict deep-equality checks are safe.
  - There is no risk of runaway regex backtracking.
  - The test fails loudly if the order of the tables changes.
  - CI runs it: `.github/workflows/website-deploy.yml` runs `npm test` on pull requests that touch `docs/**` or `website/**`.
- [x] **No stale copies elsewhere.** I searched the current `docs/`, `core/src`, `website/src` and `website/test` for the old help string and for "catalog". The only matches are the intentional new wording on this page. The `jacccard` typo in the Quick Examples page is older and outside this PR's three files.
- [ ] **Did not run builds or tests.** This was a read-only review. The npm, sbt and codegen results listed in the plan were supplied by the driver and I did not reproduce them.
- [ ] **Optional notes, not counted as issues** (no changed text is wrong):
  - The new-user example cannot be reproduced directly through the `SARModel` API, because unknown users are dropped. L165 already says it shows the scoring calculation.
  - The page could advise setting `startTime` when the data is historical.
  - The page could say outright that a rating column, if present, must be numeric.
  - Older runtime limits are out of scope: null time or rating values, the default start-time parse depending on the machine's locale, and the case-sensitive check for whether a column exists.

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
