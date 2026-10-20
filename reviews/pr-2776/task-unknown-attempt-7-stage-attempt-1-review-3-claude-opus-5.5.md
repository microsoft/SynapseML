---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 7
stage_attempt: 1
round: 3
timestamp: "2026-10-20T15:10:05Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "72ea7a56325bd47a613138aa2f4700a011c6dfde541f2d3d15770f28a4fc75c1"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2776/task-unknown-attempt-7-stage-attempt-1-review-3-claude-opus-5.5.md
- **Issues Found**: 4
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read all three changed files in full: `docs/Explore Algorithms/Other Algorithms/Smart Adaptive Recommendations.md`, `core/src/main/scala/com/microsoft/azure/synapse/ml/recommendation/SAR.scala` and `website/test/sarDocs.test.js`.
- [x] Traced `SAR.scala`:
  - Rating and time columns are detected by exact name (lines 178-179).
  - `validateNumericColumnIfPresent` checks the rating type.
  - `startTime` is parsed eagerly (line 162). The `timeDecay` UDF is lazy and computes `getTimeDecayCoeff * 24 * 60` in `Int` (lines 165-168).
  - Affinities are summed and stored as `Float` (lines 174 and 191).
  - Co-occurrence uses distinct (user, item) pairs (line 237).
  - The support threshold also applies to self-pairs, and unknown similarity names fall back to raw co-occurrence (lines 267 and 271).
  - Defaults were confirmed in `SARParams.setDefault` and `core/src/main/scala/org/apache/spark/ml/recommendation/RecommendationHelper.scala:221-223` (`user`, `item`, `rating`).
- [x] Traced `SARModel.scala`:
  - `topCandidates` (lines 296-309) ranks every item, including items that score zero. Ties go to the lower internal index, and it does not filter seen items.
  - `getSourceFactorSubset` and `attachIdMapping` use inner joins, so unknown users are dropped (lines 178 and 405).
  - `RankingAdapter.scala:149` passes `getK` straight through.
- [x] Recomputed every example from the affinity table, and all match the page:
  - Item 1 and Item 4 share Users 2, 3, 4 and 5, so their co-occurrence is 4.
  - User 1 scores are 44, 34.5, 39, 33.5 and 15.5.
  - The two new-user score vectors are 2/1/1/2/3 and 8/9/7/6/5.
  - The interpretability sum is 20 + 6 + 7.5 = 33.5.
  - The lift sentence's premise holds: co-occurrence with Item 4 is 2 for both Item 2 and Item 5, and Item 5 has 3 users versus 4 for Item 2.
- [x] Checked the time-format claims against lenient `SimpleDateFormat` behavior:
  - With `h`, an hour of 12 is read as hour 0, and hours 13–23 still parse correctly.
  - The `Z` field accepts the zone name produced by `Date.toString()`.
  - This matches the driver's JDK 11 probes. I did not run anything myself.
- [x] Checked how robust the test parser is:
  - The regexes handle CRLF line endings.
  - Blank cells parse as 0, because `Number('')` is 0.
  - Tables are picked by position, but adding or removing a table makes the tests fail rather than pass silently.
  - Exact equality is safe because every current value is a multiple of 0.5.
  - Grep found no trailing whitespace after table pipes.
- [x] Grepped `core/src`, `docs`, `website/src` and `website/test`: no stale "Minimum number of ratings per item", "items description" or "catalog data" text remains, and no test pins the old help string.
- [x] Reviewed `SARSpec.scala`, `SARIdentifierSpec.scala` and `RankingTestBase.scala`:
  - `SarTLCSpec` requests `10 + usersProducts.length` candidates.
  - Unknown or null users produce no rows.
  - No test covers null rating/time values or `timeDecayCoeff` bounds.
- [x] Left out as pre-existing or unrelated:
  - The quick example in `docs/Quick Examples/estimators/core/_Recommendation.md` uses the misspelling `"jacccard"`, which silently means raw co-occurrence. That file is unchanged.
  - The default `startTime` parse relies on the driver JVM having an English locale.
  - The diagram is hosted externally on imgur.
- [ ] I did not inspect the rendered website HTML. This was a read-only source review, so build and test results come from the driver's validation evidence. I ran no commands.

## Issues

### Issue 1: Input contract omits null, unparseable and epoch-numeric values; failures only appear after `fit()`
- **Severity**: Low
- **File**: docs/Explore Algorithms/Other Algorithms/Smart Adaptive Recommendations.md
- **Line(s)**: 22, 24 (runtime context: `core/src/main/scala/com/microsoft/azure/synapse/ml/recommendation/SAR.scala` lines 162-174, 191, 237)
- **Description**: The patch spells out the contract for the optional rating and time columns, but not what happens with bad values. The runtime behavior below already existed before this patch:
  - **Time values.** The `timeDecay` UDF parses the column's string form. A null time throws `NullPointerException`, and a non-matching string throws `ParseException`. A numeric epoch column becomes a string of digits that neither documented pattern can parse.
  - **Null ratings.** `sum` skips nulls. Such an interaction still counts toward co-occurrence but adds no affinity. If every rating for a user-item pair is null, `pair.getDouble(1)` throws.
  - **Timing.** All of this sits in lazy model columns. `fit()` succeeds, and the error appears at the first `transform` action, `recommendFor*` call or save. A `startTime`/`startTimeFormat` mismatch, by contrast, fails inside `fit()` (line 162), even when there is no time column.
- **Risk**: Real event logs often have missing timestamps, epoch seconds or slightly different formats. Users get a successful fit and then an unclear UDF failure in a later scoring job, or affinities that are silently too low.
- **Suggested Fix**: Add to lines 22/24:
  - Rating and time values must be non-null; drop or fill them first.
  - Convert epoch numbers first, for example cast to timestamp and use `yyyy-MM-dd HH:mm:ss`.
  - Time-format or null-time errors show up when affinities are first computed, not during `fit()`.

  Validating at fit time would be a separate runtime change.

### Issue 2: `timeDecayCoeff` range and how to disable decay are undocumented; 0, negative or huge values silently corrupt weights
- **Severity**: Low
- **File**: docs/Explore Algorithms/Other Algorithms/Smart Adaptive Recommendations.md
- **Line(s)**: 26 (runtime context: `SAR.scala` lines 168, 418)
- **Description**: Line 26 now describes `timeDecayCoeff` as the half-life in days, but does not give a valid range. The parameter has no validator, and the divisor is computed in `Int` (this runtime behavior is pre-existing):
  - **`0`:** every past interaction gets weight 0, and an event exactly at the reference minute gets NaN.
  - **Negative values:** older events are boosted instead of decayed.
  - **Values above 1,491,308:** the divisor overflows. For example, `Int.MaxValue * 24 * 60 == -1440`, which acts as a half-life of −1 day. Events 128 or more days old become Float `Infinity`, and `transform`'s dot product then gives NaN where the similarity is zero.

  Setting 0 or a huge value is an obvious way to try to switch decay off.
- **Risk**: Affinities can silently become all zero, Infinity or NaN. Scores then tie and fall back to identifier order, with no error.
- **Suggested Fix**: Document that the half-life must be a positive number of days, at most 1,491,308. To disable decay, omit the time column (or point `timeCol` at a name that is not present) instead of using 0 or a huge value. Track a validator or wider arithmetic as a separate runtime follow-up.

### Issue 3: Caller-side filtering guidance omits candidate sizing, zero-score fillers and missing new users
- **Severity**: Low
- **File**: docs/Explore Algorithms/Other Algorithms/Smart Adaptive Recommendations.md
- **Line(s)**: 123, 125-167
- **Description**: Line 123 has three gaps:
  - **(a) No sizing rule.** To guarantee enough results, request the desired count plus the user's number of distinct seen items, capped at catalog size. `SARSpec.scala` itself requests `10 + usersProducts.length`.
  - **(b) Zero-score fillers.** `topCandidates` returns items that score zero, ordered by sorted original ID. With the defaults (`jaccard`, threshold 4) on this page's own data, Item 5 has only 3 users, so every similarity involving it is zeroed.
    - User 6, whose only item is Item 5, scores zero everywhere and would be "recommended" Item 1, Item 2 and so on, purely by identifier.
    - User 7 ends up the same way after seen items are filtered.
  - **(c) Unseen users vanish.** Users not in the training data get no row from `recommendForUserSubset` or `transform`, and nothing reports it. So the "New User" example can only be scored after refitting.
- **Risk**: Following the guidance literally can return items ordered only by identifier, still-short lists for heavy users, or users that are silently missing.
- **Suggested Fix**: Extend line 123:
  - Request at least the desired count plus the user's seen-item count, or the catalog size.
  - Candidates that score zero have no co-occurrence support at the current `supportThreshold`; drop them or backfill.
  - Users not in the training data produce no rows until the model is refit.

  Optionally relabel "New User" as "Example user".

### Issue 4: Ranking claims added by this patch are not checked by the tests and rest on 1-point margins
- **Severity**: Low
- **File**: website/test/sarDocs.test.js
- **Line(s)**: 32-85 (prose at doc lines 121, 123, 167)
- **Description**: The tests check the tables and arithmetic, but none of the ordering claims:
  - Line 123's "top three are already seen" depends on Item 2 (34.5) beating Item 4 (33.5).
  - Line 121 claims Item 4 ranks before Item 5.
  - Line 167 depends on 8 > 7 > 6.

  A consistent edit can break these with all tests still passing. For example, change User 1's Item 2 affinity from 3.00 to 2.00 in both tables and update the interpretability lines. Item 4 (31.5) then outranks Item 2 (30.5), so line 123's reason becomes false. This is the same kind of stale prose this PR fixes.
- **Risk**: The docs can silently drift in exactly the area this PR corrects, and readers would then request the wrong number of candidates.
- **Suggested Fix**: Add a test that derives rankings from the parsed score tables:
  - User 1's top three indices are {0,1,2}, and score[3] > score[4].
  - In the second new-user example, the unseen items rank Item 1, Item 3, Item 4.
  - Optionally, assert the lift sentence's premise.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending
- **How verified**: Pending

### Issue 2
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending
- **How verified**: Pending

### Issue 3
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending
- **How verified**: Pending

### Issue 4
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending
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

1. **Input-value and lazy-failure guidance: nonblocking suggestion.**
   The guide's input section (lines 22-26) requires a numeric rating column
   and a time pattern matching the value's string representation; it does not
   promise null imputation, epoch conversion, or eager validation. The reviewer
   explicitly identifies the null handling and lazy evaluation as pre-existing.
   `SAR.scala:162-174,178-195` confirms the unchanged parser, optional-column
   branches, aggregation, and float conversion. More ingestion/error-timing
   guidance would be useful, but there is no demonstrated contradiction in the
   current valid-input examples. A general null-rating prohibition would also
   simplify the actual mixed-null aggregation behavior, rather than describe
   an enforced runtime contract.

2. **Half-life bounds: nonblocking suggestion.**
   `SAR.scala:168,418,424` confirms pre-existing integer arithmetic, the absent
   range validator, and the default of 30 days. The guide describes that
   parameter and its default; it does not recommend zero, negative, or
   extreme values to disable decay. Lines 22-26 already describe decay as
   conditional on a matching time column. Validation/overflow hardening and
   exhaustive invalid-parameter guidance are separate work, not a defect
   introduced by this documentation correction.

3. **Filtering and new-user guidance: nonblocking suggestion.**
   The guide explicitly selects `cooc` with `supportThreshold=1` at line 97,
   so the review's default-threshold example does not contradict its worked
   recommendations. `SAR.scala:267-276` and guide line 73 explain the threshold;
   `SARModel.scala:296-309` confirms top-N selection includes seen and zero-score
   candidates. Guide line 123 already warns of short or empty filtered results
   and gives the exact sufficient count for this example. A general sizing
   formula is optional additional guidance. The new-user vectors are expressly
   scoring illustrations, not an incremental-update API (line 167);
   `SARModel.scala:176-178,397-410` separately documents and implements the
   known-user subset contract. No example calls that API with an unknown user.
   Do not adopt the proposed universal statement that zero scores mean no
   co-occurrence support: allowed zero/negative rating weights can also produce
   zero scores, including by cancellation.

4. **Additional ranking assertions: nonblocking suggestion.**
   The reviewer independently recomputed and confirmed every current ranking.
   `sarDocs.test.js:32-85` intentionally checks distinct-user co-occurrence,
   vector/table agreement, all worked matrix products, and the interpretability
   arithmetic. Its tests do not claim exhaustive prose coverage. A future
   coordinated edit of the fixture and arithmetic could require revisiting the
   ranking prose; extra ordering assertions would improve maintenance, but
   the hypothetical edit is not a current incorrect result or an introduced
   runtime regression. The present rankings and published validation remain
   valid.
