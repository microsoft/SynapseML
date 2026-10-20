---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 6
stage_attempt: 1
round: 3
timestamp: "2026-10-20T11:23:51Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "c07faf3226d28065906c7899cfc1139004436ed8f42bcaffe9b602fc9e1f12b8"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2776/task-unknown-attempt-6-stage-attempt-1-review-3-claude-opus-5.5.md
- **Issues Found**: 6
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] I recalculated all worked examples in `docs/Explore Algorithms/Other Algorithms/Smart Adaptive Recommendations.md` from the affinity table at L85-L91:
  - Occurrence counts are 5/4/4/4/3, and cooc(Item 1, Item 4) is 4 (Users 2-5), as shown at L50.
  - User 1 scores are 44/34.5/39/33.5/15.5. The new-user scores are 2/1/1/2/3 and 8/9/7/6/5.
  - The explanation adds up: 20 + 6 + 7.5 = 33.5 (L179-L180).
  - The lift text (Items 2 and 5 each co-occur twice with Item 4) and the ranking text (User 1's top three are seen Items 1, 3 and 2) still match.
  - I found no arithmetic errors.
- [x] I traced input handling in `core/src/main/scala/com/microsoft/azure/synapse/ml/recommendation/SAR.scala`:
  - L72-L77: at fit time, the rating column's type is the only rating check.
  - L110-L111: null user and item IDs are rejected immediately.
  - L162-L163: the reference time is always parsed, eagerly, on the driver.
  - L165-L176: the time-parsing and array-building functions are lazy.
  - L178-L185: rating and time columns are found by exact name.
  - L191: affinities are summed, and null values are ignored.
  - L92-L98: the model stores the affinity DataFrame without computing it.
- [x] I confirmed the documented similarity behavior in `SAR.scala`:
  - Co-occurrence counts distinct users (L235).
  - The threshold applies to self-pairs too (L267).
  - Similarity names are case-sensitive, and any unknown name falls back to raw co-occurrence (L269-L271).
  - The new help string (L404-L405) changes text only; the name, type and default of 4 (L424) are unchanged.
- [x] I traced recommendation output in `core/src/main/scala/com/microsoft/azure/synapse/ml/recommendation/SARModel.scala`:
  - L248: the number of recommendations must be positive.
  - L296-L310: every item is a candidate, including zero scores, sorted by score and then index.
  - L178, L207 and L409: unknown IDs create no output rows.
  - L435-L436: `transform` uses inner joins.
- [x] `core/src/main/python/synapse/ml/recommendation/SARModel.py` L16-L20 provides `recommendForAllUsers(numItems)` and `recommendForUserSubset(dataset, numItems)`, matching the page.
- [x] I checked the related tests:
  - `SARSpec.scala` L212 requests `10 + usersProducts.length` candidates before removing seen items, which is the pattern the page now describes.
  - `SARSpec.scala` L35 uses the typo `"jacccard"`, which silently falls back to co-occurrence, as the page says.
  - `SARIdentifierSpec.scala` L297 shows unknown or null IDs are dropped during scoring.
  - `RankingTestBase.scala` L79 sets the time column to `timestamp`, which the test data lacks, so no decay is applied.
- [x] I checked `website/test/sarDocs.test.js` for Windows line endings (CRLF): the regexes allow `\r`, and `slice(2, -1)` drops it. Blank cells mean no interaction, while nonblank zeros count. The explanation-line regexes match only the numeric lines. A search found no table rows with trailing spaces that would split a table.
- [x] `website/package.json` requires Node >=24 (L5-L7), which supports `replaceAll` and `node:test`. Its test script, `node --test` (L9), finds `website/test/*.test.js`.
- [x] There is nothing concurrent here. The changes are docs, help text and a read-only synchronous test. The parsing function creates a new date formatter on each call (`SAR.scala` L166), so no formatter is shared between threads.
- [ ] I did not run npm or sbt because this review is read-only. The npm, build, sbt and codegen results come from the driver's evidence and were not reproduced.

## Issues

### Issue 1: The input section omits null and bad values, and when their errors appear
- **Severity**: Low
- **File**: docs/Explore Algorithms/Other Algorithms/Smart Adaptive Recommendations.md
- **Line(s)**: 22-24 (also `SAR.scala` L75, L92-L98, L165-L176, L191)
- **Description**: The page explains how SAR finds the columns, that ratings must be numeric, and how to choose a time pattern. It doesn't say what happens with null or unparseable values.
  - `fit()` checks only the rating column's type. The time parsing and affinity steps are stored lazily on the model.
  - A time string that doesn't match `activityTimeFormat` throws `ParseException`. A null time throws a `NullPointerException` (NPE). Because parsing is lenient, a near-miss pattern can also give a wrong date without any error.
  - Null ratings are silently left out of the sum when the same user-item pair has other rows with ratings (L191).
  - If every rating for a user-item pair is null, the affinity is null, and `pair.getDouble(1)` (L174) throws an NPE.
  - All of these errors appear only when the model is first used (`transform`, `recommendFor*` or `save`), not during `fit()`.
  - A column that happens to be named exactly `time`, such as a Spark timestamp, turns on decay with the default slash-separated 12-hour pattern. It then fails on first use.
- **Risk**: `fit()` succeeds, and the failure shows up later as an error inside a Spark function, far from the actual cause. Partly null ratings silently change affinities.
- **Suggested Fix**: Add text along these lines:
  - User, item, rating and time values must be non-null.
  - `fit()` checks column types only. Time values that don't parse, and pairs whose ratings are all null, fail when the model is first used or saved.
  - To turn off rating weights or time decay, drop or rename the column, or set `ratingCol`/`timeCol` to a name that isn't in the DataFrame.

### Issue 2: The seen-item filtering advice gives no candidate count and doesn't warn about zero-score fillers
- **Severity**: Low
- **File**: docs/Explore Algorithms/Other Algorithms/Smart Adaptive Recommendations.md
- **Line(s)**: 121-123 (also `SARModel.scala` L296-L310; `SAR.scala` L267)
- **Description**: The new advice is to "Request extra candidates, filter out seen items, then keep the desired number." It has two gaps:
  1. **How many to request.** It doesn't say. A safe number is the desired count plus the most distinct items any target user has interacted with, capped at the number of items. `SARSpec.scala` L212 uses this pattern.
  2. **Zero-score fillers.** `topCandidates` ranks every item, including those that score zero, and breaks ties by the internal index, which is the sorted original ID.
     - With the defaults (`jaccard`, `supportThreshold=4`), any item with fewer than 4 users has an all-zero similarity row, so it scores 0 for every user.
     - Many unseen items also share no users with a given user's items, so they score 0 too.
     - When a user has too few positive-score candidates, the top-K list is filled with these zero-score items in ID order.

  The page's example (`cooc`, threshold 1) gives every item a positive score, so it doesn't show the problem.
- **Risk**: Users who follow the advice can show unsupported, ID-ordered items as recommendations, or get short lists for heavy users.
- **Suggested Fix**: Say to request at least the desired count plus the largest number of distinct items any target user has interacted with. After removing seen items, also drop candidates whose score isn't positive, because the top-K list is padded with zero-score items.

### Issue 3: The reference-time advice covers only old events; a `startTime` before the data can overflow
- **Severity**: Low
- **File**: docs/Explore Algorithms/Other Algorithms/Smart Adaptive Recommendations.md
- **Line(s)**: 26 (also `SAR.scala` L167-L168, L174)
- **Description**: The page says to set `startTime` "close to the most recent interaction time" and warns only that old events can become tiny or zero.
  - Each event's weight is 2^(-(startTime - t)/half-life). Any event after `startTime` gets a weight above 1, and it grows exponentially.
  - The value is converted to a 32-bit float at L174. It becomes +Infinity after about 128 half-lives: about 3,840 days at the default of 30, or 128 days with `timeDecayCoeff=1`.
  - Infinite affinities make scores infinite, or NaN where the similarity is 0 (Inf × 0).
  - The name `startTime` also makes it easy to think it means the start of the data.
  - `timeDecayCoeff` values of 0 or less aren't rejected. A value of 0 gives zero, infinite or NaN weights, and a negative value reverses the decay.
- **Risk**: A misread or wrong reference time can silently produce Infinity or NaN scores.
- **Suggested Fix**: Say "set `startTime` at, or slightly after, the most recent interaction". Note that events after the reference time get weights above one and can overflow the 32-bit affinity. Optionally say that `timeDecayCoeff` must be a positive whole number.

### Issue 4: "Leave both unset" fails when Java's default locale isn't English (existing bug)
- **Severity**: Low
- **File**: docs/Explore Algorithms/Other Algorithms/Smart Adaptive Recommendations.md
- **Line(s)**: 26 (also `SAR.scala` L162-L163)
- **Description**: The default reference time is created as a string with `Calendar.getInstance().getTime.toString`. `Date.toString()` always uses English day and month abbreviations ("Tue", "Oct").
  - That string is parsed with `new SimpleDateFormat(getStartTimeFormat)`, which uses the JVM's default formatting locale.
  - With a non-English default locale, such as a local driver with German or French regional settings, `EEE`/`MMM` can't match the English names, and `fit()` throws `ParseException`.
  - This parse always runs, on the driver, before the time column is checked (L178). So it breaks every `fit()` with the default `startTime`, even when there is no time column.
- **Risk**: The setup the page recommends fails completely in those environments. This bug already existed; the PR only documents the path.
- **Suggested Fix**: Keep this PR to docs. Optionally add a caveat: "The default reference time assumes an English JVM locale; otherwise set `startTime` and a numeric `startTimeFormat` explicitly." Track the code fix separately, for example parsing with `Locale.US` or building the default without converting to a string and back.

### Issue 5: Users and items missing from the training data return nothing, with no error
- **Severity**: Low
- **File**: docs/Explore Algorithms/Other Algorithms/Smart Adaptive Recommendations.md
- **Line(s)**: 125-167 (also `SARModel.scala` L178, L409, L435-L436; `SARIdentifierSpec.scala` L297)
- **Description**: The "New User" examples and the closing sentence explain that a fitted model doesn't pick up new interactions. They don't say what the API does with such a user:
  - `recommendForUserSubset` returns no row for IDs that weren't in the training DataFrame. It raises no error and has no popularity fallback.
  - `transform` uses inner joins, so it drops rows whose user or item is unknown. The output can have fewer rows than the input.

  This is the user-side version of the cold-item behavior the page now covers for microsoft/SynapseML#767. Adding it is optional, but the cold-start question makes it relevant.
- **Risk**: A caller who passes a new user gets an empty result. An evaluation pipeline can silently lose rows.
- **Suggested Fix**: Add one sentence: "Users and items absent from the fit DataFrame are not scored: `transform` drops their rows and `recommendForUserSubset` returns no row for them; refit with their interactions."

### Issue 6: The docs test finds tables by position and compares decimals exactly
- **Severity**: Low
- **File**: website/test/sarDocs.test.js
- **Line(s)**: 13-14, 49, 56-64, 69-71
- **Description**: The test picks tables by their order in the page:
  - It skips the first table, assumes the next two are the co-occurrence and affinity tables, and treats every later table as an affinity/score pair.
  - The explanation test assumes row 3 and `examples[1]`.

  Adding any table shifts every assignment. This could be a parameter table under Input, which the new parameter text invites, or a Jaccard matrix. The failures would then point at unrelated content, for example "Invalid numeric table cell: User 1", or the assertion at L49, which has no message. If `markdown.match` finds no tables, it returns null and the file throws a `TypeError` while loading.

  Scores are compared with exact `deepEqual`. That works only because today's values (2.5, 7.5) are exact in binary; an affinity such as 2.3 would fail even when the page is correct. The ranking sentences (for example, "the top three are already seen") aren't tested.
- **Risk**: Correct doc edits can fail with confusing messages, which adds maintenance work. The chance of a test wrongly passing is low because failures are obvious.
- **Suggested Fix**:
  - Find tables by their header label ("User 1 aff", "User 1 rec", "New User aff/rec") or by the text just before them.
  - Assert that each table exists, with a clear message, including at L49.
  - Compare numbers with a small tolerance.
  - Optionally, check the ranking sentences against the calculated order.

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
- **Why**: Pending driver triage (existing runtime bug; a separate follow-up is suggested)
- **How verified**: Pending

### Issue 5
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending driver triage (optional clarification)
- **How verified**: Pending

### Issue 6
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending driver triage
- **How verified**: Pending

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

All six observations are preserved. None identifies a new defect in the
changed source that requires another correction:

1. **Invalid/null values and lazy execution:** These are existing validation
   and execution characteristics. The page requires numeric ratings and a
   matching time representation; it does not claim that nullable values are
   accepted, that malformed timestamps are repaired, or that every data error
   is detected eagerly by `fit`. A comprehensive preprocessing and Spark
   execution-timing guide is additional input-hardening documentation.
2. **General candidate bound and score filtering:** The page warns of short
   or empty filtered results and gives the exact count for its example. It
   promises neither a universally sufficient fixed margin nor positive-score
   results. A general bound is optional serving guidance. Requiring callers
   to discard every nonpositive score would also impose an application policy
   beyond this API contract, which permits numeric rating weights.
3. **Reference time and invalid half-lives:** The page tells readers to choose
   a reference time close to the most recent interaction, not many half-lives
   before it. The overflow counterexample requires departing from that advice.
   Nonpositive-half-life validation and extreme-weight handling are unchanged
   runtime limitations. Extra parameter-domain and overflow guidance would
   broaden the reference, not correct a promise that those inputs are safe.
4. **Default-locale limitation:** This is explicitly an existing runtime bug,
   not introduced by the documentation/help changes. The page describes how
   the current-time default works; it does not add a guarantee of support for
   every JVM formatting locale. Preserve this as a separate runtime-hardening
   follow-up, not a reason to change the catalog-input correction.
5. **Unknown identifiers:** The page expressly labels hypothetical updated
   scores as reuse of a fixed matrix for illustration, not an incremental
   model API. It promises no automatic new-user or new-item fallback.
   The additional unknown-ID warning is useful optional serving guidance.
6. **Fixture selection, decimal tolerance and ranking coverage:** The current
   table layout is the regression fixture and all values are exact half-units.
   It fails closed for structural changes. Semantic selectors, future decimal
   tolerances and broader prose assertions are optional test maintenance,
   not current arithmetic or interaction-count defects.

This triage does not claim that the existing runtime limitations are fixed or
that every raw review verdict is clean. No reviewed source changed; continue
the remaining lenses with the original findings and these dispositions intact.
