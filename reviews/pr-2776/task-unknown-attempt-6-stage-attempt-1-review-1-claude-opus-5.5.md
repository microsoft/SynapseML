---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 6
stage_attempt: 1
round: 1
timestamp: "2026-10-20T10:57:41Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "95f16bd423089d4cbeaf17054aebdf672eeb8ef522baef3d70b3a744f313fbdc"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2776/task-unknown-attempt-6-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] I read the whole diff and the post-change versions of all three manifest files: `docs/Explore Algorithms/Other Algorithms/Smart Adaptive Recommendations.md`, `core/src/main/scala/com/microsoft/azure/synapse/ml/recommendation/SAR.scala` and `website/test/sarDocs.test.js`.
- [x] I rebuilt the 5×5 co-occurrence matrix from the seven-user affinity table. All 25 cells match, including the corrected cooccur(Item 1, Item 4) = 4 (Users 2, 3, 4 and 5). The lift example still holds: cooccur(2,4) = cooccur(5,4) = 2, and 2/12 > 2/16.
- [x] I recalculated each score as similarity × affinity, and all match the page:
  - User 1 [5, 3, 2.5, 0, 0] gives [44, 34.5, 39, 33.5, 15.5].
  - The new user [0, 0, 0, 0, 1] gives [2, 1, 1, 2, 3].
  - The new user [0, 2, 0, 0, 1] gives [8, 9, 7, 6, 5].
  - The interpretability sum is 20 + 6 + 7.5 = 33.5.
- [x] The ranking statements are correct:
  - Item 4 ranks above Item 5 once seen items are removed.
  - User 1's top three are all seen items, so asking for all five is right.
  - Excluding Items 2 and 5 leaves the order Items 1, 3, 4.
- [x] I checked the input rules on the page against the code:
  - Rating and time columns are matched by exact name (`dataset.columns.contains` at `SAR.scala:178-179`; `schema.fieldNames.contains` in `validateNumericColumnIfPresent`).
  - A matching rating column must be numeric. Without one, each interaction has weight 1.0.
  - Time values are converted to strings and parsed with `SimpleDateFormat(activityTimeFormat)`.
  - The reference time comes from `startTime`/`startTimeFormat`, or from `Date.toString()` when unset (`SAR.scala:162-163`).
  - The half-life formula is `2^(-minutes/(coeff*1440))`.
  - Affinities are stored as 32-bit floats (`seqToArray`).
  - Defaults are set at `SAR.scala:424-426`; the constants `user`/`item`/`rating` are at `RecommendationHelper.scala:221-223`.
- [x] I checked the similarity rules:
  - The interaction matrix is built from distinct user–item pairs, and item counts use `countDistinct`, so co-occurrence counts distinct users.
  - The `>=` threshold at `SAR.scala:267` also applies to an item paired with itself.
  - `similarityFunction` matching is case-sensitive, and any unrecognised string falls back to raw co-occurrence (`SAR.scala:271`).
- [x] I confirmed the noon claim from `SimpleDateFormat` rules: with `h`, the hour 12 is stored as 0 and defaults to AM. Hours 13–23 still parse correctly because parsing is lenient. So "12:xx is read as 00:xx" is exactly right.
- [x] Recommendation API:
  - `recommendForAllUsers(numItems)` and `recommendForUserSubset(dataset, numItems)` have matching signatures in `SARModel.scala` and the Python `SARModel.py`.
  - Neither `SARModel` nor `RankingAdapter` (`allUsers` mode, `RankingAdapter.scala:148-149`) filters out items the user has already seen.
  - Unknown users are dropped through inner joins (`SAR.scala:374`; `SARModel.scala:409`, `435`), matching `SARIdentifierSpec` ("SAR drops unknown and null identifiers during scoring").
- [x] The `SAR.scala` change only touches the class doc comment (lines 38–39) and the help string (lines 404–405). The param name, type and default are unchanged, so saved models and method signatures are unaffected. Changed lines are no longer than 120 characters.
- [x] I traced the test logic:
  - The table regex returns the tables in the order the test expects: interaction, similarity, affinity, then six example tables.
  - Windows (CRLF) line endings are handled by `\r?` and `slice(2, -1)`.
  - A user counts as interacting when the cell is nonblank, matching how the implementation treats any row as an interaction.
  - All values are exact in binary, so strict `deepEqual` cannot fail on rounding.
  - The interpretability regexes match only the two numeric lines.
- [x] Security: no secrets, no executable change and no user-controlled input. The test only reads a fixed repository path.
- [x] Scope triage. These pre-existing or outside-the-PR observations are not counted as issues:
  - (a) `docs/Quick Examples/estimators/core/_Recommendation.md:285-292` and `354-361` use `setSimilarityFunction("jacccard")`, which silently runs raw co-occurrence. They also set `setTimeCol("timestamp")` on a DataFrame that has no such column. This is a follow-up candidate.
  - (b) `SAR.scala:42` says "Caller-visible outputs use the original types". Recommendation outputs instead cast integral numeric IDs to `IntegerType` (`recommendationOutputMapping`).
  - (c) Null time values (null reaches `SimpleDateFormat.parse`) and user–item pairs whose ratings are all null (`getDouble` on null in `seqToArray`) make fit fail with unclear errors. This is a runtime limitation; documenting it is optional.
  - (d) The default reference time is parsed with the JVM's default locale, so fit fails on non-English locales.
  - (e) `numericRows` treats a blank similarity cell as 0.
- [ ] Commands were not run by this reviewer (read-only lens). The npm, sbt, codegen and noon-parsing results are the driver's evidence and were not re-run.
- [ ] The external architecture image (`i.imgur.com`) was not viewed. Its caption is only checked against the documented scope.

## Issues

### Issue 1: The new-user examples don't say that a fitted `SARModel` silently returns nothing for users it never saw
- **Severity**: Low
- **File**: docs/Explore Algorithms/Other Algorithms/Smart Adaptive Recommendations.md
- **Line(s)**: 125, 145–147, 167
- **Description**: Line 125 (changed in this PR) introduces a made-up user whose single Item 5 interaction is not in the seven-user data behind the similarity matrix. User 6 has affinity 2.00, and adding the new user would raise occ(Item 5) from 3 to 4. Line 167 says the example only "illustrates the scoring calculation" and that a fitted `SARModel` "does not automatically incorporate new interactions". It leaves out the API behavior a reader will actually hit:
  - `recommendForUserSubset` and `transform` silently drop users that were absent from the training DataFrame, through inner joins at `SAR.scala:374` and `SARModel.scala:409` and `435`. `SARModel.scala:178` documents this: "unknown user identifiers do not create output rows".
  - Refitting with the new interactions recomputes the co-occurrences. For example, cooccur(Item 2, Item 5) becomes 2. So the displayed scores cannot be reproduced through the API.

  Line 146 also keeps the old "Assuming that a same item isn't recommended again" wording. The other paragraphs now describe filtering as something the caller does.
- **Risk**: The issue behind this PR (microsoft/SynapseML#767) is about cold start. A reader could call `recommendForUserSubset` for a new user and get an empty DataFrame with no error. They could also expect a refit to reproduce these tables.
- **Suggested Fix**: Add one sentence at line 125 or line 167, for example: "This hypothetical user is not in the fitted model; `recommendForUserSubset` and `transform` return no rows for users absent from the training DataFrame. Refit with their interactions to score them, which also recomputes item similarities." Optionally reword line 146 to match the caller-side filtering wording used elsewhere on the page.

### Issue 2: The example table header mixes a real default column name with display labels, and the `userCol`/`itemCol` defaults are not documented
- **Severity**: Low
- **File**: docs/Explore Algorithms/Other Algorithms/Smart Adaptive Recommendations.md
- **Line(s)**: 20, 22, 30
- **Description**:
  - Line 30 now renames `Time` to `time` so it matches the default `timeCol`.
  - Line 22 gives the `rating`/`time` defaults and the exact-name rule.
  - Line 20 does not give the `userCol`/`itemCol` defaults, which are `user` and `item` (`RecommendationHelper.scala:221-222`, applied at `SAR.scala:424-426`).
  - The same header row keeps the display labels `User ID`/`Item ID`.

  The header is now half real column names and half labels, which undercuts the page's precise column-name contract.
- **Risk**: A reader who builds a DataFrame from this table and relies on the defaults gets a fit error that column `user` does not exist (`validateIdentifierColumn` → `schema(columnName)`). The failure is loud, so the impact is low, but the mixed header blurs which names must match exactly.
- **Suggested Fix**: Add "(defaults: `user` and `item`)" to line 20. Then either rename the headers to `user | item | time`, or keep the labels and state that the example assumes `userCol="User ID"` and `itemCol="Item ID"`.

### Issue 3: The Interpretability paragraph still suggests SynapseML generates explanation messages automatically
- **Severity**: Low
- **File**: docs/Explore Algorithms/Other Algorithms/Smart Adaptive Recommendations.md
- **Line(s)**: 182
- **Description**: This PR updated the arithmetic in this section (lines 179–180). Elsewhere it removed claims about features SynapseML lacks: the catalog learner, automatic model updates and automatic removal of seen items. Line 182 still says "A message like this can be displayed automatically for each recommendation." `SARModel` returns only identifiers and scores: `aggregateRecommendations` emits `{item, rating}` structs, and `transform` adds only a prediction column. It has no explanation output and no per-item breakdown of a score.
- **Risk**: Readers may look for an explanation API that does not exist. That is the same kind of gap between the docs and the code that led to microsoft/SynapseML#767.
- **Suggested Fix**: Reword it, for example: "An application can build a message like this from the per-item contributions; SynapseML returns scores only and does not generate explanation text."

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
  "rationale": "Use one complementary reviewer per lens for the mandatory six-lens policy on the same frozen source patch; retain any evidence-based finding dispositions separately from original verdicts.",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoningEffort": "max",
  "modelSource": "Current task-tool live catalog supports claude-opus-5.5 with max reasoning.",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md from independently trusted guidance at 5a3e29f1a3c0d3b9649a2168279ef4f3be06dcc4",
  "phase": "Pre-commit pass 6; remote validation follows publication."
}

## Driver disposition after the original review

The original three Low findings are retained. They do not require a source
change:

1. **Hypothetical-user serving behavior:** The page explicitly says these
   updated scores reuse the same similarity matrix, illustrate the scoring
   calculation rather than an incremental API, and are not automatically
   incorporated into a fitted model (line 167). It does not present an API call
   that produces these hypothetical scores or promise that refitting preserves
   the old matrix. Earlier text establishes caller-side filtering. An explicit
   unknown-user/refit warning would be additional serving guidance, not a
   correction of the mathematical example or an existing API promise.
2. **Identifier headers/defaults:** Line 20 explicitly tells readers to
   configure `userCol` and `itemCol`; line 28 calls the table an illustration.
   The page never claims that `User ID` and `Item ID` are default names.
   Enumerating defaults or spelling out those illustrative settings would be
   optional clarification. This repeats an already-triaged suggestion.
3. **Explanation messages:** The paragraph demonstrates how an application can
   derive an explanation from the displayed contributions and says a message
   "can be displayed". It does not say that `SARModel` emits explanation text
   or has an explanation-output API. Naming the application explicitly would
   improve wording, but the current statement of possibility is not a claim
   of a nonexistent model method.

No current numerical, input-contract, runtime or test defect was identified in
this lens. Continue the remaining lenses on the unchanged frozen patch.
These are driver dispositions, not a rewritten reviewer verdict.

For precision, the review's security checklist phrase "no executable change"
means no runtime algorithm change here: this patch does add executable Node
documentation tests. Those tests read one fixed repository file and contain
no network, process-execution or credential-access logic.
