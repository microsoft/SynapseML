---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 6
stage_attempt: 1
round: 6
timestamp: "2026-10-20T12:07:03Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "c3f2087d6568b2d86fbf95d2108ff67e864b2e7876c23e8b53fade06bf137df6"
---

## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2776/task-unknown-attempt-6-stage-attempt-1-review-6-claude-opus-5.5.md
- **Issues Found**: 7
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] I rebuilt the co-occurrence matrix from the seven-user affinity table in `docs/Explore Algorithms/Other Algorithms/Smart Adaptive Recommendations.md` (lines 83–91). The item counts are 5, 4, 4, 4 and 3. Items 1 and 4 share four users (Users 2, 3, 4 and 5). All 25 cells match lines 48–54.
- [x] I recalculated every score:
  - User 1: [44, 34.5, 39, 33.5, 15.5].
  - New user, first step: [2, 1, 1, 2, 3].
  - New user, second step: [8, 9, 7, 6, 5].
  - Interpretability: 20 + 6 + 7.5 = 33.5.
  - The ranking and filtering claims at lines 121, 123 and 167 hold. The lift example at line 63 is still true after the matrix fix: both pairs co-occur twice, with item counts 4 and 3.
- [x] I checked the input rules against `core/src/main/scala/com/microsoft/azure/synapse/ml/recommendation/SAR.scala`:
  - Rating and time columns are found by exact name (lines 178–179, `validateNumericColumnIfPresent`).
  - Time values are cast to strings and parsed with `SimpleDateFormat`. The half-life formula is at lines 162–169.
  - Affinities are stored as 32-bit floats (lines 172–174).
  - Co-occurrence uses a distinct user-item matrix and distinct-user counts.
  - `similarityFunction` matching is case-sensitive and falls back to raw co-occurrence. The threshold also applies to the diagonal (lines 267–275).
  - Defaults are at lines 424–426 and `RecommendationHelper.scala:221–223` (`user`, `item`, `rating`).
- [x] I checked the recommendation claims against `SARModel.scala`:
  - Top-K ranking includes items the user has already seen (lines 296–310).
  - Output contains only item and rating (lines 372–378).
  - Unknown users are dropped (lines 178, 409, 435–436; `SARIdentifierSpec.scala:64`).
  - The tests already filter seen items in caller code after over-fetching (`SARSpec.scala:212`).
- [x] The Scala diff only changes the class doc (lines 38–39) and the `supportThreshold` help text (lines 404–405). No logic, default, signature or serialization changed. The old help string appears nowhere else in `core/`, `docs/`, `website/` or `tools/`.
- [x] I reviewed `website/test/sarDocs.test.js`. It handles CRLF line endings, uses the intended blank-cell meaning, and its arithmetic is exact because every value is a multiple of 0.5. `node --test` picks it up (`website/package.json:9`). CI runs it on the source `docs/` file (`.github/workflows/website-deploy.yml:56–61`).
- [x] Rendering path: `website/docusaurus.config.js` (lines 11–13 and 144) and `website/legacyMarkdownPreprocessor.js`. The page's headings and its sidebar entry (`website/sidebars.js:158`) are unchanged, so no links break.
- [x] I considered the driver's validation results (npm test and build, sbt style and compile, 29 SAR tests, codegen, JDK 11 time-parsing probe). I did not run them; this review was read-only.
- [ ] Not flagged because they are older, unrelated runtime limits or outside this change:
  - The interaction matrix is collected to the driver, and scoring is dense across all users and items.
  - Parsing the default `startTime` depends on the JVM locale.
  - Null rating or time values cause failures.
  - Misspelled optional columns produce no warning.
  - `"jacccard"` is misspelled in `docs/Quick Examples/estimators/core/_Recommendation.md` and `SARSpec.scala:35`.
  - The diagram is hosted on imgur.

## Issues

### Issue 1: The "New User" example doesn't say that a fitted model can't score unseen users, and keeps old "same item" wording
- **Severity**: Low
- **File**: docs/Explore Algorithms/Other Algorithms/Smart Adaptive Recommendations.md
- **Line(s)**: 125–147 (table labels at 127, 137, 149, 159)
- **Description**: Line 125 introduces "a user with a single interaction" right after the `recommendForAllUsers`/`recommendForUserSubset` guidance, and the tables call this user "New User". The page never says this user is outside the seven training users or that the scores are calculated by hand. A fitted `SARModel` can't score a user that was absent during `fit`: `recommendForUserSubset` and `transform` silently drop unknown IDs (SARModel.scala:178, 409, 435–436). If the user were added to the training data instead, the displayed matrix would change (Item 5's count becomes 4). Line 167 covers updates after fitting, but not scoring a user who was absent during fitting. Line 146 also keeps "Assuming that a same item isn't recommended again". That wording is ungrammatical and doesn't match the caller-side filtering wording on lines 121–123 and 167.
- **Risk**: A reader may call `recommendForUserSubset` for a new user and get an empty result with no error. They may also read line 146 as describing model behavior, which contradicts line 121.
- **Suggested Fix**: On line 125, describe a hypothetical new user who is not one of the seven users above, and say the scores are calculated by hand from the fixed similarity matrix. Add that a fitted `SARModel` returns no rows for users absent during fitting. Reword line 146 to: "After excluding the already-interacted Item 5, items 1 and 4 …".

### Issue 2: The Interpretability section suggests SynapseML generates explanations automatically
- **Severity**: Low
- **File**: docs/Explore Algorithms/Other Algorithms/Smart Adaptive Recommendations.md
- **Line(s)**: 171, 182
- **Description**: "A message like this can be displayed automatically for each recommendation" comes from the original SAR service description. SynapseML returns only `recommendations: array<struct<item, rating>>` (SARModel.scala:372–378), with no per-term contributions or reasons. The PR already makes this original-versus-SynapseML distinction for catalogs (line 10) and matrix updates (line 167), but not here.
- **Risk**: Users may look for an explanation API or output column that doesn't exist.
- **Suggested Fix**: State that SynapseML returns only item IDs and scores, and that an application can build such a message by calculating these per-item contributions itself.

### Issue 3: The input example mixes display labels with real column names
- **Severity**: Low
- **File**: docs/Explore Algorithms/Other Algorithms/Smart Adaptive Recommendations.md
- **Line(s)**: 20, 30
- **Description**: The header `time` was lowercased to match the default `timeCol`, but `User ID` and `Item ID` were kept. The default identifier columns are `user` and `item` (RecommendationHelper.scala:221–222). Line 22 gives defaults for `ratingCol` and `timeCol`, but line 20 gives none for `userCol` and `itemCol`.
- **Risk**: It's unclear which headers are real column names. If a reader recreates the table and uses default parameters, `transformSchema` fails because there is no `user` column.
- **Suggested Fix**: Use `user | item | time` as headers and add "(defaults: `user` and `item`)" on line 20. Alternatively, keep the labels and show `userCol="User ID"` and `itemCol="Item ID"`.

### Issue 4: The advice to request extra candidates doesn't say how many, or what it costs
- **Severity**: Low
- **File**: docs/Explore Algorithms/Other Algorithms/Smart Adaptive Recommendations.md
- **Line(s)**: 123
- **Description**: "Request extra candidates" doesn't say how many. One `numItems` value applies to every user in the call, and the top-K list includes items the user has already seen (SARModel.scala:296–310). To guarantee `k` unseen items, request `k` plus the most items any requested user has already interacted with, capped at the number of items. `SARSpec.scala:212` already uses this pattern. Output rows and lookup joins grow with users × `numItems`. This is an optional addition.
- **Risk**: Too few candidates give heavy users short lists; too many inflate the output for everyone.
- **Suggested Fix**: Add one sentence giving this rule, the example's value (2 + 3 = 5), and a note that larger values increase output size.

### Issue 5: The worked formula renders as one run-on paragraph with stray asterisks
- **Severity**: Low
- **File**: docs/Explore Algorithms/Other Algorithms/Smart Adaptive Recommendations.md; website/test/sarDocs.test.js
- **Line(s)**: docs 173–180; test 72–73
- **Description**: These lines have no hard line breaks. The site adds only remark-math (`website/docusaurus.config.js:144`), and the preprocessor leaves these lines alone. Lines 173–180 therefore merge into one paragraph, and each `\*` shows as a literal asterisk. The result reads "…*= **20** + 6 + 7.5 + 0 + 0 = **33.5***", and line 174 has the same problem. The formatting predates the PR, but the PR edited these lines, and the new test's patterns (`^\\\*= `, `^_= `) now depend on it.
- **Risk**: The corrected arithmetic, the PR's main fix, is hard to read on the published page.
- **Suggested Fix**: Optional: put each step on its own line (backslash line breaks with `=`), or use a KaTeX `aligned` block or a fenced block. Update the two test patterns to match. Nearby line 67 also has a typo: `cooccur(Item1, Item 2)`.

### Issue 6: The docs test finds tables by position and has assertions without messages
- **Severity**: Low
- **File**: website/test/sarDocs.test.js
- **Line(s)**: 13–14, 49, 59–60, 71
- **Description**: The test assigns all tables by position, and every table after the affinity table goes into `examples`. Adding any table, such as a parameter summary or a table after Interpretability, silently shifts the assignments. The test would then fail with misleading errors: "Invalid numeric table cell", bare assertions with no message (lines 49 and 59–60), or a TypeError from `split(...)[1]` (line 71). It also accepts any even number of example tables, so deleting an example pair would still pass.
- **Risk**: Doc edits become harder to diagnose, and the test catches less.
- **Suggested Fix**:
  - Find tables by header text (for example `| User 1 aff |`) or by the sentence before them.
  - Assert six example tables and their expected labels.
  - Add messages to lines 49 and 59–60.
  - Assert that the Interpretability heading exists before splitting on it.

### Issue 7: The `similarityFunction` help text doesn't list the exact, case-sensitive values
- **Severity**: Low
- **File**: core/src/main/scala/com/microsoft/azure/synapse/ml/recommendation/SAR.scala
- **Line(s)**: 384–386 (behavior at 268–271)
- **Description**: The help text names "Lift", "Co-occurrence" and "Jaccard" but not the accepted values `jaccard`, `lift` and `cooc`. Matching is case-sensitive, and any other value silently becomes raw co-occurrence. The docs page now says this on line 71, but the help text that appears in the Python and Scala API docs doesn't. The PR already fixed the `supportThreshold` help text next to it for the same reason. This change is optional.
- **Risk**: A user who passes `"Jaccard"` silently gets raw co-occurrence instead of Jaccard.
- **Suggested Fix**: Change only the help string, for example: "Similarity function: jaccard (default), lift, or cooc. Case-sensitive; other values use raw co-occurrence…". Then rerun codegen.

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

### Issue 4
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 5
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 6
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 7
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

All seven Low observations are retained as optional or pre-existing
improvements; no reviewed source changes follow this lens:

1. **Hypothetical users and filtering wording:** Line 167 explicitly calls
   these fixed-matrix updates a scoring illustration, not an incremental API.
   Lines 121-123 establish caller-side exclusion. No API call, automatic
   unknown-user fallback or refit-equivalence guarantee is shown. Additional
   serving guidance and grammatical polish are optional.
2. **Explanation display:** The contribution calculation supports a message
   an application can display. The paragraph does not claim that SAR emits
   explanation text or offers an explanation method. Explicitly naming the
   application would clarify wording without correcting a false API claim.
3. **Illustrative identifier labels:** Line 20 requires configuring the two
   identifier columns, and the table is labeled illustrative. It does not
   claim its display labels are default names.
4. **General overfetch bound and cost:** The page gives the precise count for
   its example and warns of short or empty results. A general bound and output
   size discussion would be additional usage/performance guidance; no universal
   margin or cost guarantee is made.
5. **Formula presentation:** The escape/emphasis and soft-line-break structure
   predates this change. The PR corrects the numbers without changing that
   markup structure. The production page builds and contains the correct
   expansion and 33.5 total. A separate formatting improvement would improve
   readability, but this is not a newly introduced rendering regression or
   incorrect arithmetic.
6. **Fixture selection and diagnostics:** Current table selection and all
   three regressions are correct. Their scope is the worked-example fixture,
   not a general structural validator or a guarantee that every future example
   remains present. More labels and messages would ease future maintenance.
7. **Similarity help values:** The unchanged parameter help names the
   mathematical measures; it does not specify those capitalized names as
   literal setter values. The changed algorithm page now supplies the exact
   case-sensitive values and fallback. Expanding this additional help string
   is optional and outside the incorrect threshold-help correction.

The six-lens pre-commit review is complete on the frozen source after driver
triage. The original verdicts remain `ISSUES_FOUND`; no claim is made that the
reviewers returned clean verdicts or that the deferred runtime/maintenance
work was implemented. Publication and current-revision CI are still pending.
