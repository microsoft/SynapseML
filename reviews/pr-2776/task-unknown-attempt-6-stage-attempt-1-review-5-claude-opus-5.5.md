---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 6
stage_attempt: 1
round: 5
timestamp: "2026-10-20T11:52:13Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "e5eecd5233ac22662620947b5ccbbf44daf3e676346c1c5a8f828c5c9422c946"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2776/task-unknown-attempt-6-stage-attempt-1-review-5-claude-opus-5.5.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read all of `website/test/sarDocs.test.js` (85 lines) and `docs/Explore Algorithms/Other Algorithms/Smart Adaptive Recommendations.md`. Traced which table each name refers to: lines 13–14 skip the interaction table (doc lines 30–35). `similarityTable` is the table at doc lines 48–54, `affinityTable` is at 83–91, and `examples` holds six tables in three aff/rec pairs (lines 103–109/113–119, 127–133/137–143, 149–155/159–165).
- [x] Checked that the parsing handles Windows line endings (CRLF). `tableRows` (lines 16–20) drops the trailing `\r` through `slice(2, -1)`. The table regex and the two interpretation regexes (lines 72–73) allow `\r?` before line ends, and `.` never matches `\r`.
- [x] Rebuilt the co-occurrence matrix from the seven-user table by hand. All 25 cells match, including the corrected cooc(1,4)=4 (Users 2, 3, 4, 5). Under the test's "nonblank cell" rule, explicit zero or negative affinities would still count as interactions.
- [x] Recomputed S·a for every example. User 1 [5,3,2.5,0,0] gives [44,34.5,39,33.5,15.5]. The new user [0,0,0,0,1] gives [2,1,1,2,3]. [0,2,0,0,1] gives [8,9,7,6,5]. The interpretation total is 20+6+7.5=33.5. All values are exact binary fractions, so the strict `deepEqual` comparisons are safe.
- [x] Checked that the ranking statements in the text are currently correct. User 1's top three are items 1, 3, 2 (all already seen), and the unseen order is 4 then 5, so `numItems=5` is needed. The new-user orders are [1,4,2,3] and [1,3,4]. Bold cells in the score tables mark the unseen items. The lift example (doc line 63) holds: sim[4][2]=sim[4][5]=2 and occ(5)=3<occ(2)=4.
- [x] Checked the documented behavior against `core/src/main/scala/com/microsoft/azure/synapse/ml/recommendation/SAR.scala`:
  - Exact-name column detection and default weight 1.0: lines 178–184.
  - Half-life calculation: line 168.
  - Distinct-user counting: `countDistinct` at line 220 and distinct pairs at line 235.
  - Threshold also applies to self-pairs, and any other similarity name falls back to raw co-occurrence: lines 267–271.
  - Numeric rating requirement: line 314.
  - Defaults: line 424.
  - New help string: lines 404–405.
- [x] Checked `core/src/main/scala/com/microsoft/azure/synapse/ml/recommendation/SARModel.scala`. `topCandidates` (lines 296–309) sorts every destination item and keeps the top `numItems` without excluding seen items, which matches the docs.
- [x] Reviewed existing tests in `SARSpec.scala`, `SARIdentifierSpec.scala`, `RankingTestBase.scala` and `core/src/test/python/synapsemltest/recommendation/test_ranking.py`:
  - No test under `core/src/test` checks that a non-numeric rating is rejected (searched for "must be numeric").
  - `SarTLCSpec` (`SARSpec.scala` lines 203 and 212) asks for extra results and filters seen items itself, so it passes whether or not SAR excludes seen items.
  - None of the readable fixtures repeat a (user, item) row.
- [x] CI runs the new test. `.github/workflows/website-deploy.yml` triggers on `docs/**` and `website/**` pull requests and runs `npm test`, and `website/package.json` sets `"test": "node --test"`, which finds `website/test/*.test.js`.
- [x] Searched `core/src`, `docs`, `website/test` and `tools` for the old help text "Minimum number of ratings" and found no stale copies. The parameter name is unchanged and help text isn't saved with models, so save/load is unaffected.
- [ ] I did not run `npm test`, `npm run build`, sbt, or the codegen checks. I relied on static tracing plus the validation results the driver supplied.
- [ ] I did not inspect `core/src/test/resources/demoUsage.csv.gz` because it is compressed binary, so I can't say whether the TLC tests already cover repeated user-item rows.

## Issues

### Issue 1: The doc tests don't check the ranking and seen-item statements, or the lift example
- **Severity**: Low
- **File**: website/test/sarDocs.test.js; docs/Explore Algorithms/Other Algorithms/Smart Adaptive Recommendations.md
- **Line(s)**: test 18, 47–66; doc 63, 121, 123, 146, 167 (score tables 113–119, 137–143, 159–165)
- **Description**: The tests prove each score table equals S·a, but not the conclusions the text draws from those scores:
  - (a) "request all five candidates … since the top three are already seen" (line 123), added in this PR.
  - (b) "Item 4 is recommended before Item 5" (line 121).
  - (c) "items 1 and 4 … before items 2 and 3" (line 146).
  - (d) "Item 1 … followed by Item 3 and then Item 4" (line 167).
  - (e) Bold cells in the score tables mark the unseen items, but line 18 strips `**`, so the test never sees them.
  - (f) The lift example on line 63 depends on the Item 4 row and column this PR edited.

  All of these are correct today. However, the corrected Item 4 score (33.5) is only 1.0 below the already-seen Item 2 (34.5), so statement (a) can easily break. The text before this PR had exactly this kind of mistake: "items 1 and 4 … before items 2 and 3" sat under User 1's table, where items 2 and 3 were already seen (diff hunk `@@ -117,13 +115,14 @@`). Tests that only check arithmetic would not have caught it.
- **Risk**: Someone can edit the tables consistently, keep `npm test` green, and leave the advice on how many candidates to request out of date. Readers following that advice can get short or empty results, which is the exact problem line 123 warns about.
- **Suggested Fix**: Add one test that:
  - Treats an item as seen when its entry in `examples[0]`, `examples[2]` or `examples[4]` is nonzero. The New User vectors use explicit 0 for items the user never touched.
  - Ranks unseen items by score, breaking ties by item number.
  - Asserts that User 1's top three by score are {1,2,3} and the unseen order is [4,5].
  - Asserts the new-user orders [1,4,2,3] and [1,3,4].
  - Reads each score cell before stripping `**` and asserts the bold items are exactly the unseen items.
  - Asserts `sim[3][1] === sim[3][4] && sim[4][4] < sim[1][1]` for the lift example.

  Keep the expected orders as literals so the text and the test have to change together.

### Issue 2: The parser drops table labels, so "distinct users", "seven users" and table identity aren't actually checked
- **Severity**: Low
- **File**: website/test/sarDocs.test.js; docs/Explore Algorithms/Other Algorithms/Smart Adaptive Recommendations.md
- **Line(s)**: test 13–14, 16–20, 32–45, 49; doc 46
- **Description**:
  - `tableRows` discards the header row and every row label (`slice(2)` and `slice(2, -1)`).
  - Tables are picked purely by position. The first table is thrown away without any check, and `examples` only has to be non-empty with an even count.
  - As a result, test 1 counts table rows rather than distinct user labels. If someone added a second `**User 1**` row (say, to show a repeated interaction), the test would count it as another user. It would then demand a matrix that contradicts the distinct-user definition the test is named after.
  - Nothing checks the "seven users" wording (doc line 46), or that Item 1–5 appear in the same order in the similarity, affinity and vector tables.
  - Inserting a new table, such as an input example, fails with a misleading "Invalid numeric table cell" error or a mis-paired example instead of a clear message.
- **Risk**: The test doesn't fully check the distinct-user rule this PR corrects. Counts quoted in the text can go stale, and failures are confusing to diagnose.
- **Suggested Fix**:
  - Have the parser return `{label, cells}` for each row.
  - In test 1, group nonblank items by user label (or assert the labels are unique) and assert there are 7 users.
  - Assert the skipped first table's header matches `User ID | Item ID | time`.
  - Assert the similarity and affinity headers list Item 1–5 in order.
  - Assert `examples.length === 6`, and that each pair's headers contain `aff` and then `rec`.

### Issue 3: No runtime test ties the documented behavior or worked example to SAR (optional; covers existing behavior)
- **Severity**: Low
- **File**: core/src/test/scala/com/microsoft/azure/synapse/ml/recommendation/SARIdentifierSpec.scala (suggested home); behavior in SAR.scala 178–184, 235, 267–271, 314, 424 and SARModel.scala 296–309
- **Line(s)**: N/A (missing test)
- **Description**: `sarDocs.test.js` only checks the page against itself. The page now states several facts about how SynapseML behaves, and no committed runtime test pins any of them:
  - Repeated user-item rows don't raise co-occurrence.
  - Top-k results include items the user has already seen.
  - Ratings must be numeric.
  - Optional columns are matched by exact, case-sensitive name.
  - The default time format uses the 12-hour `h` field, so `12:xx` parses as midnight.

  Specifically, no test rejects a non-numeric rating. `SarTLCSpec.testProductRecommendations` would pass even if SAR excluded seen items. The readable fixtures never repeat a (user, item) row. The driver's probes were one-off checks and aren't committed.
- **Risk**: A future runtime change would make this page wrong while every suite stays green. Examples: filtering seen items, switching the default `activityTimeFormat` to `HH`, or matching column names case-insensitively.
- **Suggested Fix**: This is optional and shouldn't block the PR.
  - Add a small Scala test that fits `SAR().setSimilarityFunction("cooc").setSupportThreshold(1)` on the page's seven-user table, using the affinities as ratings. Split one cell into two rows (for example, User 1/Item 1 as 2.5 + 2.5).
  - Assert the decoded item similarities equal the page's matrix.
  - Assert `recommendForAllUsers(5)` for User 1 returns [(Item 1, 44), (Item 3, 39), (Item 2, 34.5), (Item 4, 33.5), (Item 5, 15.5)], and that `recommendForAllUsers(3)` returns only seen items.
  - Add `intercept[IllegalArgumentException]` for a string rating column, and check that a column named `Rating` is ignored when `ratingCol="rating"`.

  If this is out of scope, record the deferral together with the probe evidence.

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

The three Low observations are deferred without source changes:

1. **Broader prose/formatting coverage:** The new regressions check the three
   intended relationships: interaction presence to co-occurrence, affinity
   times similarity to scores, and interpretation arithmetic to the score.
   They do not claim to validate every ranking sentence, bold cell or lift
   explanation. Those current claims were independently recomputed and are
   correct. Additional assertions would improve future coverage but are not
   needed to fix a current incorrect result.
2. **Row labels and fixture shape:** An affinity matrix has one row per user;
   the current fixture has seven distinct user rows and consistent item order.
   Adding a duplicate user row would violate that matrix shape, rather than
   introduce a supported affinity value. The test is a regression for this
   fixed worked example, not a general validator for arbitrary malformed
   matrix tables. Identity, uniqueness and clearer structural-error checks
   are useful optional guards. This differs from the corrected zero-affinity
   case: zero and negative values are valid cells within the existing shape.
3. **Additional runtime coverage:** Deferred as the reviewer explicitly
   recommends. No algorithm, default, schema or serialization behavior changes
   in this PR. The existing three SAR suites passed all 29 tests; codegen and
   Scala style/compilation passed; source tracing and the JDK parsing probe
   checked the documented behavior. A fuller suite pinning these existing
   contracts can be separate work rather than broadening this documentation
   contribution.

No remaining current-source defect was found in this lens. Keep the original
`ISSUES_FOUND` verdict and this explicit scope/coverage disposition, and run
the final lens on the unchanged frozen patch.
