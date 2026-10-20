---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 4
stage_attempt: 1
round: 3
timestamp: "2026-10-20T09:30:43Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "e2c2af081edd542c6219759ad9aa5c13b0dc9a71dbf5eed88ade59ddb6e62cf7"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2776/task-unknown-attempt-4-stage-attempt-1-review-3-claude-opus-5.5.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] I reviewed all three manifest paths: `docs/Explore Algorithms/Other Algorithms/Smart Adaptive Recommendations.md`, `core/src/main/scala/com/microsoft/azure/synapse/ml/recommendation/SAR.scala` and `website/test/sarDocs.test.js`. I did not open any review or artifact directory.
- [x] I recalculated every worked example by hand. All 25 co-occurrence cells match the seven-user affinity table, including Items 1 and 4 at 4. The User 1 scores are 44, 34.5, 39, 33.5 and 15.5. The two new-user score vectors are (2,1,1,2,3) and (8,9,7,6,5). The interpretation sum is 20+6+7.5=33.5. The lift claim holds: cooc(4,2) and cooc(4,5) are both 2, and occ(5)=3 is less than occ(2)=4. The ranking statements and the User 1 and User 2 affinity-order statements are also correct.
- [x] I traced the SAR fit path in `SAR.scala`:
  - Co-occurrence counts distinct users: `.distinct()` pairs at 226-239 and `countDistinct` at 214-224.
  - The threshold check `cooccurrence >= getSupportThreshold` at 267 also covers the diagonal.
  - The similarity function matches `jaccard` and `lift` exactly and treats any other value as raw co-occurrence (269-272).
  - Optional time and rating columns are detected at 178-185. Time strings are parsed at 162-169, and affinities are converted to `Float` at 174.
  - Defaults are set at 424-426. The new `supportThreshold` help text at 404-405 matches the code. Param help text is not part of saved model data.
- [x] I traced scoring in `SARModel.scala`. A score is affinity times similarity: `dotProduct` at 225-229 and the block-matrix multiply in `rankScores`. `topCandidates` (296-309) ranks all items and keeps the top N without removing seen items. Unknown or null IDs produce no output rows (178, 207). `RankingAdapter.scala:149` calls `recommendForAllUsers(getK)` directly and does not filter either.
- [x] I checked `sarDocs.test.js` for ways it could pass when it should fail:
  - Table lookup by position and the `% 2` pairing check.
  - Blank cells are read as 0.
  - Windows line endings (`\r?\n`, `slice(2,-1)`, `\r?$`) are handled.
  - Exact float comparison is safe because every value is a multiple of 0.5.
  - The regexes match only the numeric expansion and sum lines.

  Every malformed-input case I found makes the test fail rather than pass.
- [x] I cross-checked the existing tests:
  - `SARSpec.scala:35` uses the `"jacccard"` typo, which exercises the co-occurrence fallback.
  - `SARSpec.scala:154-155` sets `startTime` and `startTimeFormat` together.
  - `SARSpec.scala:212` requests extra recommendations before filtering seen items.
  - `RankingTestBase.scala:79` sets a `timestamp` time column that the data lacks, so SAR ignores it silently.
  - `SARIdentifierSpec.scala` covers dropped unknown and null IDs.
- [x] A grep of `core/src` and `docs` found no leftover copy of the old help text "Minimum number of ratings per item".
- [x] I found these existing limitations outside this PR and did not count them as issues:
  - Null time values make `SimpleDateFormat.parse(null)` throw an exception.
  - If every rating for a user-item pair is null, the summed affinity is null and `getDouble` at `SAR.scala:174` fails.
  - When `startTime` is unset, SAR parses the English output of `Date.toString` with a formatter that uses the default locale (`SAR.scala:162-163`). This can fail on JVMs with a non-English locale, even when there is no time column.
  - The architecture diagram is hosted on an external image site.
  - `docs/Quick Examples/estimators/core/_Recommendation.md` uses `"jacccard"` and a nonexistent `timestamp` column. That file is outside the three-path manifest.
- [x] I considered the driver-supplied validation evidence (npm test and build, sbt style/compile/testOnly, codegen and the JDK parsing check) but did not run any of it myself.

## Issues

### Issue 1: The seen-item filtering advice ignores SARModel's top-N cutoff
- **Severity**: Low
- **File**: docs/Explore Algorithms/Other Algorithms/Smart Adaptive Recommendations.md
- **Line(s)**: 121 (context: `core/src/main/scala/com/microsoft/azure/synapse/ml/recommendation/SARModel.scala:296-309`)
- **Description**: The new text tells callers to remove previously seen items themselves. However, `recommendForAllUsers(numItems)` and `recommendForUserSubset(dataset, numItems)` keep only the top `numItems` items before the caller sees anything. Each item's similarity with itself is the highest value in its row for co-occurrence, Jaccard (1) and lift (1/occ). As a result, seen items tend to fill the top of the list. In this page's own example, User 1's three highest scores (44, 39, 34.5) belong to the seen Items 1, 3 and 2. Calling `recommendForAllUsers(3)` and then filtering as described returns nothing for User 1, and getting both Items 4 and 5 requires `numItems=5`. The repository's reference test already requests extra items for this reason: `recommendForAllUsers(10 + usersProducts.length)` in `SARSpec.scala:212`.
- **Risk**: Callers who follow the new advice can get short or empty recommendation lists, with no error.
- **Suggested Fix**: Add a sentence after line 121, for example: "`recommendForAllUsers(numItems)` and `recommendForUserSubset(dataset, numItems)` return only the top `numItems` items, including previously seen items, which often rank highest. Request the desired number plus the number of items each user has already seen, then filter."

### Issue 2: A misnamed optional rating or time column is ignored without error
- **Severity**: Low
- **File**: docs/Explore Algorithms/Other Algorithms/Smart Adaptive Recommendations.md
- **Line(s)**: 22, plus the example header at 30 (context: `SAR.scala:178-179`, `SAR.scala:317`)
- **Description**: The page says columns "matching" `ratingCol` and `timeCol` are used whenever present. SAR finds them with exact, case-sensitive checks (`dataset.columns.contains` and `schema.fieldNames.contains`). Spark matches column names case-insensitively by default, so users may expect `Time` or `Rating` to count. The page's own example table uses the header `Time`. A column whose case differs, or a misspelled `setTimeCol` or `setRatingCol` value, is ignored silently: SAR trains with weight 1 per interaction and no time decay. A wrong `userCol` or `itemCol`, by contrast, raises an error through `schema(columnName)` (`SAR.scala:309`). The repository already contains this mistake: `RankingTestBase.scala:79` sets a `timestamp` column the data lacks. The page also only implies that a rating column, if present, must be numeric; otherwise `fit` fails (`SAR.scala:315-321`).
- **Risk**: Users may believe time decay or rating weights are applied when they are not, and recommendations differ from what they intended.
- **Suggested Fix**: Add to line 22: "SAR detects these columns by exact, case-sensitive name. If no column exactly matches `timeCol` or `ratingCol`, SAR trains without time decay or rating weights and raises no error. If a rating column is present, it must be numeric."

### Issue 3: The `startTime` advice leaves out historical data, where old weights can become zero
- **Severity**: Low
- **File**: docs/Explore Algorithms/Other Algorithms/Smart Adaptive Recommendations.md
- **Line(s)**: 26, related to 24 and 30-35 (context: `SAR.scala:162-169`, `174`, `409`; `SARSpec.scala:154-155`)
- **Description**: Line 26 says to leave `startTime` and `startTimeFormat` unset to use the current time. It omits the parameter's own stated purpose: "Set time custom now time if using historical data" (`SAR.scala:409`).
  - **How weights are stored:** Each event's weight is 2^(-age/half-life) measured from the reference time. It is computed as a `Double` but stored as a 32-bit `Float` (`SAR.scala:174`).
  - **Why precision matters:** Moving the reference time scales all weights equally, so rankings stay the same only while `Float` can still represent the weights.
  - **Default 30-day half-life:** Events about 10.4 years older than the reference time start losing precision. Events about 12.3 years older round to `0.0f`.
  - **Shorter half-lives:** The cutoff scales with the half-life. It is about 2.9 years at 7 days and about 5 months at 1 day.
  - **This page's example:** The timestamps are from 2015 and are already in that reduced-precision range relative to now.
  - **Late-1990s data:** Data such as MovieLens 100K, converted with the page's suggested `yyyy-MM-dd HH:mm:ss` pattern and fit with `startTime` unset, would produce all-zero affinities. Every score would then tie at 0, and `topCandidates` would return items in internal-index order (`SARModel.scala:308`).

  The repository's reference test sets `startTime` to a date in its dataset's period (`SARSpec.scala:154-155`).
- **Risk**: Recommendations can be silently arbitrary for old data or short half-lives. Absolute scores also change with the date the model is fit.
- **Suggested Fix**: Add to line 26: "For historical or offline data, set `startTime` (in the `startTimeFormat` format) close to the most recent interaction time, as the parameter's help text advises. Weights are stored as 32-bit floats, so with the current-time default, events many half-lives old can lose precision or become zero." This is a documentation change only, not a runtime change.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 2
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 3
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
  "rationale": "Use one complementary reviewer per lens for the mandatory six-lens policy. Require all six clean on the same frozen source patch.",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoningEffort": "max",
  "modelSource": "Current task-tool live catalog supports claude-opus-5.5 with max reasoning.",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md from independently trusted guidance at 5a3e29f1a3c0d3b9649a2168279ef4f3be06dcc4",
  "phase": "Pre-commit pass 4; remote validation follows publication."
}

## Driver disposition after the original review

All three findings are accepted and addressed in the pending source patch:

- Issue 1: Documented that the recommendation APIs truncate before caller-side
  filtering; callers must request extra candidates before filtering and taking
  the desired count. The worked example now explicitly requests five candidates
  to retain User 1's two unseen items. This follows `SARModel.scala:296-309`.
- Issue 2: Documented exact, case-sensitive optional-column detection, silent
  omission of absent or misspelled optional columns, and numeric rating values.
  The example header now uses the default `time` spelling. This follows
  `SAR.scala:178-185` and `SAR.scala:315-321`.
- Issue 3: Added historical-data reference-time guidance and the possibility of
  precision loss or zero weights with 32-bit affinity storage. This follows
  `SAR.scala:162-174`; no numerical cutoff or runtime guarantee was added.

The original findings and open resolution entries above are retained unchanged.
Publication and affected website validation are pending. These source changes
invalidate pass 4; a complete new six-lens pass is required before committing.

Affected validation subsequently passed: `cd website && npm test` (37 passed,
zero failures or skips) and `cd website && npm run build` (optimized production
build). Six rendered-page assertions confirmed the new optional-column,
historical-time and top-N guidance. The unchanged Scala and JavaScript blobs
retain their earlier validation evidence. Publication remains pending.
