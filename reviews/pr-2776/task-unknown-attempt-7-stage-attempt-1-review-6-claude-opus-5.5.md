---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 7
stage_attempt: 1
round: 6
timestamp: "2026-10-20T15:55:26Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "13d11b2daaec96156e5080075e29dcfdf0c540246f00a4a39547d21a141fe488"
---

## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2776/task-unknown-attempt-7-stage-attempt-1-review-6-claude-opus-5.5.md
- **Issues Found**: 4
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] I read all three manifest files in full, and they match the supplied diff. The only `SAR.scala` changes are the Scaladoc at line 38 and the `supportThreshold` help text at lines 404-405. No logic, defaults, signatures or serialization changed.
- [x] I recomputed the guide's numbers by hand from the seven-user affinity table (guide lines 83-91):
  - All 25 co-occurrence cells (lines 48-54) are distinct-user counts.
  - User 1's scores are 44, 34.5, 39, 33.5 and 15.5 (lines 115-119).
  - The new-user scores are 2/1/1/2/3 and 8/9/7/6/5 (lines 139-143 and 161-165).
  - The interpretability sum is 4·5 + 2·3 + 3·2.5 + 4·0 + 2·0 = 33.5 (lines 179-180).
  - The lift example on line 63 still holds: cooc(4,2) = cooc(4,5) = 2, and occ(5) = 3 < occ(2) = 4.
  - The ranking text on lines 121, 123, 146 and 167 matches these scores.
- [x] I checked the input-contract text (guide lines 20-26) against `core/src/main/scala/com/microsoft/azure/synapse/ml/recommendation/SAR.scala`:
  - Rating and time columns are detected by exact name with `dataset.columns.contains` (lines 178-179).
  - The rating column must be numeric; `transformSchema` (lines 72-77) enforces this.
  - Time values are cast to strings and parsed with `SimpleDateFormat` (lines 165-169 and 180-185).
  - `startTime` is always parsed (lines 162-163), which supports the advice to set both `startTime` and `startTimeFormat` or neither.
  - The half-life is in days (line 168), and the defaults are at lines 424-426.
  - Affinities are stored as `Float` (lines 173-174).
  - Lenient `SimpleDateFormat` reads `h` = 12 as hour 0 (AM by default), which supports the guide's 12-hour warning.
- [x] I checked the similarity behavior in `SAR.scala`:
  - The interaction matrix counts each user-item pair once (lines 226-239).
  - Item counts use `countDistinct` (lines 214-224).
  - The `>=` threshold applies to every pair, including the diagonal (line 267).
  - `similarityFunction` matching is case-sensitive, and any other value falls back to raw co-occurrence (lines 268-272).
  - The new `supportThreshold` help text is accurate.
- [x] I checked the recommendation claims in `core/src/main/scala/com/microsoft/azure/synapse/ml/recommendation/SARModel.scala`:
  - `recommendForAllUsers` (line 168) and `recommendForUserSubset` (line 180) use `sortBy(...).take(numRecommendations)` (lines 308-309) and do not exclude seen items.
  - There is no incremental-update method.
  - `RankingAdapter` does not filter seen items either.
  - The Python `SARModel.py` uses the same method and argument names.
- [x] I traced how `website/test/sarDocs.test.js` parses the guide:
  - The regex finds nine tables; the input table is discarded, leaving six example tables.
  - Blank cells parse as 0, and the empty trailing affinity column is ignored.
  - The regexes handle CRLF line endings.
  - All products are multiples of 0.5, so strict equality is exact.
  - The style matches `website/test/rSetupDocs.test.js`.
- [x] I checked `SARSpec.scala`, `SARIdentifierSpec.scala` and `RankingTestBase.scala`. The documented behavior for identifiers, nulls, unknown IDs, thresholds and `cooc` matches these tests. Searching `core/src`, `docs` and `tools` found no leftover copies of the old help text or "items description".
- [x] Performance and observability: there are no runtime or logging changes, and the new test adds one synchronous file read. Silently ignoring a missing rating or time column and falling back for an unknown `similarityFunction` are existing behaviors. The guide now describes them accurately, and I did not count them against this patch.
- [x] I considered the driver-supplied validation results as reported. I did not run any commands myself.
- [x] Not counted (outside the three changed files): `docs/Quick Examples/estimators/core/_Recommendation.md` (lines 289-292 and 358-361) calls `setSimilarityFunction("jacccard")`, a misspelling. It also calls `setTimeCol("timestamp")`, but the sample data has no such column. Under the behavior the new guide documents, the misspelled value runs plain co-occurrence and no time decay is applied. This could be fixed in a separate change.

## Issues

### Issue 1: The example table mixes real and display column names, and the user/item defaults are not stated
- **Severity**: Low
- **File**: `docs/Explore Algorithms/Other Algorithms/Smart Adaptive Recommendations.md`
- **Line(s)**: 20, 22, 30
- **Description**: The patch lowercases only the third header to `time` so it matches the default `timeCol`. The other two headers stay `User ID` and `Item ID`, which match neither default (`user` and `item`; see `core/src/main/scala/org/apache/spark/ml/recommendation/RecommendationHelper.scala:221-222`). Line 22 gives the `rating` and `time` defaults, but line 20 does not give the `userCol` and `itemCol` defaults. The section stresses exact, case-sensitive column names, yet one header is a real column name and two are display labels. This patch introduced the inconsistency.
- **Risk**: A reader who builds a DataFrame from this table and keeps the default parameters gets a missing-`user` column error. They then have to find the defaults elsewhere. The error is immediate and visible, so the impact is low.
- **Suggested Fix**: Rename the headers to `user | item | time`; the tests skip this table, so they are unaffected. You could also, or instead, add "(defaults: `user` and `item`)" to line 20.

### Issue 2: The co-occurrence paragraph says "easy to update" without saying SynapseML has no update path
- **Severity**: Low
- **File**: `docs/Explore Algorithms/Other Algorithms/Smart Adaptive Recommendations.md`
- **Line(s)**: 56-57 (compare with 167)
- **Description**: The patch rewrote this paragraph to describe SynapseML specifically ("In SynapseML, co-occurrence counts…"). The same paragraph still says "The advantage of co-occurrence is that it's easy to update." SynapseML rebuilds the counts and every similarity measure from the full DataFrame on each `fit` (`SAR.scala` lines 79-106 and 280-301), and `SARModel` has no update method, as line 167 now says. This is not a direct contradiction, since additive counts are a property of the math. In this paragraph, though, it reads as a SynapseML feature.
- **Risk**: Readers may look for an incremental update, or pick `cooc` expecting an update benefit that SynapseML does not offer.
- **Suggested Fix**: Qualify the sentence, for example: "Conceptually, co-occurrence is easy to update because counts are additive; in SynapseML, refit SAR to incorporate new interactions."

### Issue 3: The new tests find tables by position and don't check the new ranking advice
- **Severity**: Low
- **File**: `website/test/sarDocs.test.js`
- **Line(s)**: 13-14, 47-66 (guide lines 121, 123, 146, 167)
- **Description**: The test assigns tables by their order in the file: `const [, similarityTable, affinityTable, ...examples] = markdown.match(...)`. If anyone adds a table before the co-occurrence matrix, such as a parameter table under Input, every variable shifts. The tests would then fail with misleading messages such as "Invalid numeric table cell: Item 1". The ranking claims this patch adds are also untested:
  - Item 4 comes before Item 5 (line 121).
  - The top three are already seen, so request five (line 123).
  - Items 1 and 4 rank first (line 146).
  - After excluding items 2 and 5, the order is 1, 3, 4 (line 167).

  This PR exists to fix exactly this kind of mismatch between the numbers and the text.
- **Risk**: A later edit could keep the tables consistent with each other but make the filtering advice wrong. An unrelated new table would cause confusing test failures.
- **Suggested Fix**: Find each table by its header label, such as `User 1 aff`, `User 1 rec` or `New User rec`, or by a first data row starting with `**User 1**`. Then assert the orders using the computed scores:
  - For User 1, the top three are {1, 2, 3} and the unseen order is [4, 5].
  - For the first new user, min(score 1, score 4) > max(score 2, score 3).
  - For the second new user, the unseen order is [1, 3, 4].

### Issue 4: The edited Scaladoc still calls SAR "fast scalable", wording the guide dropped
- **Severity**: Low
- **File**: `core/src/main/scala/com/microsoft/azure/synapse/ml/recommendation/SAR.scala`
- **Line(s)**: 38 (supporting lines 173, 232-237, 245, 256, 264)
- **Description**: The patch rewrote this sentence but kept "fast scalable adaptive algorithm", while the rewritten guide introduction (guide line 12) dropped that claim. Memory use in the current implementation grows with the number of users (U) and items (I); this patch does not change that:
  - `createInteractionMatrix` collects every distinct user-item pair on the driver and broadcasts it (lines 232-237).
  - Each user's affinity row is a dense `Array[Float]` as long as the item count (line 173).
  - Each item builds dense vectors as long as the user count and the item count (lines 245, 256, 264).

  Together, the stored model grows as O(U·I + I²). The limitation predates this patch, but the wording sits on a line the patch changed.
- **Risk**: Readers of the API docs with large catalogs may expect it to scale, but the driver and executor memory use above will not. The Scaladoc and the guide also now describe SAR differently.
- **Suggested Fix**: Match the guide, for example: "SAR is an adaptive algorithm for personalized recommendations based on user-item interaction history." Optionally add a short note on catalog size to the guide. Handle runtime scaling in a separate change.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending driver triage
- **How verified**: Pending

### Issue 2
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending driver triage
- **How verified**: Pending

### Issue 3
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending driver triage
- **How verified**: Pending

### Issue 4
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending driver triage
- **How verified**: Pending

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "Trusted repository policy requires a final six-lens CI-qualified review.",
    "models": "Installed direct Medium contract, resolved independently from the current live task-tool model catalog.",
    "stageBehavior": "Repository review-gauntlet.md requires sequential lenses unless parallel mode was requested.",
    "safety": "Authorized maintainer follow-up and independently trusted external-contributor guidance."
  },
  "changeSize": "Three product paths: one SAR guide, Scala documentation/help text, and three JavaScript documentation consistency tests.",
  "cyclomaticComplexity": "Low: documentation and deterministic table/arithmetic assertions; no runtime algorithm or service changes.",
  "rationale": "The six-lens policy requires Medium despite the low code complexity. Keep the product patch frozen; preserve original findings and evidence-based dispositions.",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoningEffort": "max",
  "modelSource": "The current task-tool catalog lists claude-opus-5.5 as the available Opus frontier and explicitly supports max reasoning. It is complementary to the OpenAI primary.",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md at trusted guidance SHA 5a3e29f1a3c0d3b9649a2168279ef4f3be06dcc4",
  "phase": "Final CI-qualified pass 1; artifact review attempt 7 follows six separately recorded pre-commit passes."
}

## Driving-agent disposition

The original review above is preserved, including its `ISSUES_FOUND` verdict
and original resolution placeholders. All four findings are nonblocking
suggestions; no reviewed source changed. These dispositions supersede the
placeholders without representing the raw review as clean.

1. **Illustrative headers and identifier defaults: nonblocking suggestion.**
   Guide line 20 explicitly directs callers to configure `userCol` and
   `itemCol`; line 28 calls the table an illustration, not a default-ready
   loading example. The existing `User ID` and `Item ID` headings remain
   valid configured column names, and `SAR.scala:72-81` immediately validates
   their presence. Lowercasing `time` avoids silently dropping the optional
   time weighting described at line 22; the required identifier columns do
   not have that silent-omission behavior. Listing their defaults or aligning
   all display headers would be clearer, but the current text does not claim
   that this table works with unconfigured identifier parameters.

2. **Additive-count wording: nonblocking suggestion.**
   The reviewer explicitly recognizes that "easy to update" is a mathematical
   property, not a direct contradiction. That sentence at guide line 57 is
   retained from the baseline. The new text at line 167 expressly distinguishes
   the scoring calculation from an incremental-update API.
   `SAR.scala:79-106` constructs a new model from the fitting dataset; no
   update API is added or advertised. Repeating the refit caveat beside the
   earlier mathematical discussion is optional clarity, not a current API
   correction.

3. **Table binding and ranking coverage: nonblocking suggestion.**
   The current nine-table fixture binds correctly at `sarDocs.test.js:13-26`,
   and the reviewer independently confirms all current ranking statements.
   Lines 32-85 cover the corrected co-occurrence, vector, score, and
   interpretation relationships. Hypothetical inserted tables fail closed;
   coordinated future fixture changes may warrant extra prose/ranking
   assertions. Those maintenance extensions do not identify a present
   incorrect example or a runtime regression.

4. **Retained scaling adjective: nonblocking suggestion.**
   The frozen diff shows that "fast scalable adaptive algorithm" already
   existed in `SAR.scala:38`; this edit removes the incorrect item-description
   input claim. The reviewer likewise identifies the collection and dense
   factor storage as pre-existing, with no changed performance behavior.
   The adjective is not a stated complexity bound or unlimited-catalog
   guarantee. Removing it or documenting concrete sizing limits would improve
   API documentation, but it is separate from this correction's interaction
   contract and verified examples. This disposition does not claim that the
   existing implementation has unlimited or linear memory scaling.
