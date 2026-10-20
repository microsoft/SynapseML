---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 6
stage_attempt: 1
round: 4
timestamp: "2026-10-20T11:41:03Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "5833fb10b0e3981ff461e5a25a5fa5086beac65d4ff5dee9cc027246ecdfb3b9"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2776/task-unknown-attempt-6-stage-attempt-1-review-4-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Optional columns and the rating type check.** Traced `core/src/main/scala/com/microsoft/azure/synapse/ml/recommendation/SAR.scala:75,178-184,312-320`.
  - Both columns are detected by exact name: `columns.contains` and `fieldNames.contains`.
  - The rating column must be numeric.
  - The weight is `lit(1.0)` when neither column matches.
  - The defaults are `rating` (`RecommendationHelper.scala:223`) and `time` (`SAR.scala:424-426`).
  - This confirms doc line 22.
- [x] **Time parsing and decay.** Traced `SAR.scala:162-176`.
  - When `startTime` is unset, the code parses `Date.toString()` with `startTimeFormat`.
  - Activity times are converted with `cast(StringType)` and parsed with `SimpleDateFormat(activityTimeFormat)`.
  - The weight is `2^(-Δmin/(T·1440))`, so the half-life is `T` days.
  - Sums are computed as `Double` and stored with `.toFloat`.
  - This confirms doc lines 24 and 26. The lenient `h` field maps hour 12 to 0, which matches the JDK 11 probe in the supplied validation evidence. I did not re-run that probe.
- [x] **Similarity calculation.** Traced `SAR.scala:214-278`.
  - Item counts use `countDistinct` users.
  - The interaction matrix holds distinct (user, item) pairs with value 1.0.
  - Multiplying each item's user-indicator row by that matrix gives distinct-user co-occurrence counts.
  - The `>= supportThreshold` gate also applies to the diagonal.
  - `"jaccard"` and `"lift"` are matched case-sensitively; any other string falls back to raw co-occurrence.
  - This confirms doc lines 46, 56, 65, 71 and 73, and the new help string at `SAR.scala:404-405`.
- [x] **Co-occurrence matrix (doc lines 48–54).** Recomputed from the affinity table at lines 83–91.
  - Item user counts are 5, 4, 4, 4 and 3.
  - Items 1 and 4 share four users (Users 2, 3, 4 and 5).
  - All 25 cells match.
  - The lift example at line 63 still holds: both co-occurrences are 2, and 2/12 > 2/16.
- [x] **Scores and ranking claims.** Recomputed similarity × affinity for each example.
  - User 1 → 44, 34.5, 39, 33.5, 15.5.
  - Item-5-only user → 2, 1, 1, 2, 3.
  - User with items 2 and 5 → 8, 9, 7, 6, 5.
  - Interpretability sum: 20 + 6 + 7.5 = 33.5.
  - The ranking text at lines 121, 123, 146 and 167 holds: the top three are already seen, so five candidates are needed.
- [x] **Recommendation API behaviour.** Traced `SARModel.scala`.
  - Method names and parameters at lines 168 and 180 match `core/src/main/python/synapse/ml/recommendation/SARModel.py`.
  - Lines 296–309 sort by `(-score, index)` and call `take(numItems)`, with no seen-item exclusion.
  - Line 327 computes users × similarityᵀ, which equals the doc's calculation because the similarity matrix is symmetric.
- [x] **Test file, line by line.** Traced `website/test/sarDocs.test.js`.
  - Line 14 finds nine table blocks in document order and tolerates CRLF line endings.
  - The slices at lines 17–18 and 52 are correct, with no off-by-one errors.
  - A blank cell becomes 0 via `Number('')`, while a nonblank `"0"` still counts as an interaction (line 38).
  - Index 3 means Item 4 (lines 69 and 84).
  - All values are exact binary fractions, so strict equality is safe.
  - Reordering the tables makes the tests fail loudly; I found no way for them to pass vacuously.
- [x] **Test discovery.** `website/package.json` runs `node --test` with Node ≥24, which picks up this file. The style matches the sibling website tests.
- [x] **Scope checks.**
  - No old catalog, item-description or help-string text remains in `docs/` or `core/src/main`.
  - The `SAR.scala` diff changes only documentation and the help string.
  - The supplied validation evidence was noted but not re-executed.
- [x] **Out-of-scope observations (not counted as issues).**
  - The released copy under `website/versioned_docs/version-1.1.3/...` still shows the old 28.5 and catalog text. The brief says this is intentional.
  - `docs/Quick Examples/estimators/core/_Recommendation.md:291,360` uses `"jacccard"`, which silently falls back to raw co-occurrence.
  - When `startTime` is unset, parsing assumes an English JVM locale.
  - Null rating or time values are not handled.
  - Wording such as line 146, and the example header's non-default `User ID`/`Item ID` columns, is consistent with the documented contract.

## Issues

### Issue 1: Interpretability paragraph still implies a built-in automatic explanation feature
- **Severity**: Low
- **File**: `docs/Explore Algorithms/Other Algorithms/Smart Adaptive Recommendations.md`
- **Line(s)**: 182 (context 171–180)
- **Description**: The PR now separates SynapseML's behaviour from the original SAR design. For example, line 121 says SAR "does not automatically exclude previously seen items", and line 167 says the example is "not an incremental-update API". The unchanged sentence at line 182 still says "A message like this can be displayed automatically for each recommendation." SynapseML returns only `(item, rating)` scores (`SARModel.scala:168,180,225-231,296-309`). It produces no per-term contributions or explanations. Building such a message needs caller code that reads the model's internal factor tables and ID mappings.
- **Risk**: This is the same kind of confusion behind microsoft/SynapseML#767: readers look for an API that the page implies but SynapseML does not provide. It has no numeric impact.
- **Suggested Fix**: Reword it, for example: "An application can compute these per-item contributions to generate a message like this; SynapseML's SAR returns recommendation scores only." No test change is needed.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

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
  "rationale": "Use one complementary reviewer per lens for the mandatory six-lens policy on the same frozen source patch; retain any evidence-based finding dispositions separately from original verdicts.",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoningEffort": "max",
  "modelSource": "Current task-tool live catalog supports claude-opus-5.5 with max reasoning.",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md from independently trusted guidance at 5a3e29f1a3c0d3b9649a2168279ef4f3be06dcc4",
  "phase": "Pre-commit pass 6; remote validation follows publication."
}

## Driver disposition after the original review

The single Low finding repeats the earlier optional wording suggestion.
The paragraph derives the contribution-based explanation and says such a
message "can be displayed"; it does not say that SynapseML returns that
message or exposes an explanation-generation method. Application code can
implement the described display. Naming that actor explicitly would be
clearer, but the existing statement is not a promise of a built-in API.

No source change is required. Preserve this original `ISSUES_FOUND` verdict
and the separate disposition, and continue the remaining lenses on the same
frozen source.
