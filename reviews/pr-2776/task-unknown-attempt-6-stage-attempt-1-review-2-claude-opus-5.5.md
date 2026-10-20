---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 6
stage_attempt: 1
round: 2
timestamp: "2026-10-20T11:08:22Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "62716f00ef59ca1d15da20b20855ef025238db55f1639f0bdb9026f490942e6b"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2776/task-unknown-attempt-6-stage-attempt-1-review-2-claude-opus-5.5.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **The Scala change only touches documentation.** In `core/src/main/scala/com/microsoft/azure/synapse/ml/recommendation/SAR.scala`, only lines 38–39 (class Scaladoc) and 404–405 (`supportThreshold` help text) change. The param's name, type, setter, getter and default (`supportThreshold -> 4`, lines 424–426) are unchanged. Spark ML persistence stores param values, not help text, so serialization is unaffected. No stale copy of "Minimum number of ratings per item" remains in `core/src`, `docs` or `tools`.
- [x] **The new help text and similarity wording match the code.**
  - Co-occurrence counts distinct users: `createInteractionMatrix` calls `.distinct()` (line 235), `itemFeaturesVector` collapses duplicates (line 246), and `countDistinct` is used at line 220.
  - The threshold applies to every pair, including self-pairs: `cooccurrence >= getSupportThreshold` at line 267, with `countI` taken from the diagonal at line 263.
  - `jaccard` and `lift` are case-sensitive, and any other value falls back to raw counts (lines 269–271).
- [x] **I traced the input contract.**
  - `ratingCol` and `timeCol` are detected by exact name: `dataset.columns.contains` (lines 178–179) and `schema.fieldNames.contains` (line 317).
  - Time values are cast to strings and parsed with `activityTimeFormat` (lines 181–182).
  - When `startTime` is unset, the current time string is parsed with `startTimeFormat` (line 163).
  - The half-life formula is at line 168, and affinities are stored as 32-bit floats (line 174).
  - The `user`, `item` and `rating` defaults come from `Constants` in `core/src/main/scala/org/apache/spark/ml/recommendation/RecommendationHelper.scala`.
- [x] **I traced the recommendation API claims.**
  - `SARModel.scala` `recommendForAllUsers` and `recommendForUserSubset` (lines 168 and 180) reach `topCandidates` `.take(numRecommendations)` (line 309). Seen items are not filtered.
  - The subset method only uses identifiers to pick factors that were computed during fit (`getSourceFactorSubset`, line 397).
  - The Python `core/src/main/python/synapse/ml/recommendation/SARModel.py` exposes the same method names.
- [x] **I recomputed every worked number by hand.**
  - All 25 co-occurrence cells from the seven-user affinity table, including Items 1/4 = 4.
  - User 1 scores: [44, 34.5, 39, 33.5, 15.5].
  - New-user vectors: [2, 1, 1, 2, 3] and [8, 9, 7, 6, 5].
  - Interpretation: 4·5 + 2·3 + 3·2.5 = 33.5.
  - The lift example still holds: items 2 and 5 each co-occur twice with Item 4, and their occurrence counts are 4 and 3.
- [x] **The new test follows existing test conventions.** I compared it with `website/test/quantileRegressionScalaDocs.test.js`, `rSetupDocs.test.js` and `installDocs.test.js`. It uses the same `node:test`/`assert` style, the same `<topic>Docs.test.js` naming and the same `docs/` read path. `website/package.json` runs `node --test`, which picks it up. The CRLF handling in `tableRows` and in the interpretation regexes is correct.
- [x] **Leaving versioned docs unchanged is consistent with the stated scope.** There are 21 versioned copies of the page under `website/versioned_docs/`. `website/versions.json` lists 1.1.3 first, and `website/docusaurus.config.js` has no `lastVersion` override. Readers therefore see this fix under `/docs/next/` until the next release snapshot. I did not count this as a defect.
- [x] **Observations I did not count as issues** (pre-existing or optional):
  - `docs/Quick Examples/estimators/core/_Recommendation.md` lines 291 and 360, and `SARSpec.scala` line 35, use `setSimilarityFunction("jacccard")`. Under the newly documented contract this silently means raw co-occurrence. Worth a follow-up.
  - The `similarityFunction` help text (`SAR.scala` lines 384–386) does not list the accepted values.
  - The inherited ALS param `coldStartStrategy` is not a cold-start learner.
  - No Scala test fits the page's seven-user example. The JS tests check the page against itself; I verified the runtime behavior by reading the code and the existing `SarTLCSpec` reference-matrix tests.
- [ ] **I did not run any commands** (this is a read-only review). I relied on the driver's evidence: `npm test` (37 passed), `npm run build`, `sbt scalastyle "Test / scalastyle" compile "Test / compile"`, the three SAR suites (29 passed) and `sbt core/codegen`.

## Issues

### Issue 1: The doc test identifies tables by their order in the page, not by their content
- **Severity**: Low
- **File**: website/test/sarDocs.test.js
- **Line(s)**: 13-14, 49, 56-66, 69-70, 84
- **Description**: `const [, similarityTable, affinityTable, ...examples] = markdown.match(...)` picks tables purely by position:
  - table 0 is skipped;
  - table 1 is treated as co-occurrence and table 2 as affinity;
  - every remaining table is read as alternating affinity/score pairs.

  Nothing checks that each table is the one intended. The other website doc tests match on content (regex, IDs, `includes`), not on order.
- **Risk**: Reasonable edits would shift every table, for example:
  - a parameter table in the expanded Input section;
  - a Jaccard or lift matrix in the similarity discussion;
  - a contribution table under Interpretability.

  The resulting failures would be misleading. Examples are "Invalid numeric table cell: User 1", or "Co-occurrence of items 1 and 2" when the test is actually reading a Jaccard table. The pairing check at line 49 has no message at all. Docs contributors could end up changing correct content to satisfy a test they don't understand. Also, a `null` match throws when the file loads, not inside a named test.
- **Suggested Fix**: Find tables by stable labels. Take the table that follows the "co-occurrence matrix uses the seven users" sentence, and the one after "An example User-to-Item affinity matrix". Pick the example tables in order by their header labels (`User 1 aff`, `User 1 rec`, `New User aff`, `New User rec`). At minimum, check each selected table's header or first-row label with a clear message, and add a message to the line 49 assertion. Do the parsing inside the tests so a parse failure is reported per test.

### Issue 2: The Input section gives defaults only for the optional columns, and the example header is only partly aligned
- **Severity**: Low
- **File**: docs/Explore Algorithms/Other Algorithms/Smart Adaptive Recommendations.md
- **Line(s)**: 20, 22, 30
- **Description**: Line 22 gives defaults for the optional `ratingCol` and `timeCol` and stresses exact, case-sensitive names. Line 20 names the required `userCol` and `itemCol` but omits their defaults, `user` and `item` (`SAR.scala` lines 424–426). The example table changes only its third header to the default `time`. `User ID` and `Item ID` match neither default.
- **Risk**: The only input example mixes real column names with display labels. A reader who builds a DataFrame with these headers and default params gets a "`user` does not exist" error from `transformSchema`. A reader who copies the `time` convention still doesn't learn the identifier defaults. This weakens the input contract that microsoft/SynapseML#767 asks about.
- **Suggested Fix**: On line 20, add "(defaults: `user` and `item`)". Make the header consistent, preferably `user | item | time`, so the table becomes a valid DataFrame for the default params once `activityTimeFormat` is set as line 24 advises. No test change is needed, because the test skips this table.

### Issue 3: The middle worked example still implies that seen items are excluded automatically
- **Severity**: Low
- **File**: docs/Explore Algorithms/Other Algorithms/Smart Adaptive Recommendations.md
- **Line(s)**: 146 (compare 121, 167)
- **Description**: The PR removed the misplaced duplicate of this paragraph and rewrote the first and third conclusions to say that the caller filters seen items ("If the caller filters out…", "After excluding the already-interacted items 2 and 5…"). The middle conclusion that was kept still uses the original wording: "Assuming that a same item isn't recommended again…".
- **Risk**: The three examples now describe the same step in inconsistent ways. A passive "assuming" reads like model behavior or a hidden setting, which line 121 says does not exist.
- **Suggested Fix**: Replace it with: "After excluding Item 5, which this user already interacted with, items 1 and 4 have the highest score and would be recommended before items 2 and 3." The tests don't parse this sentence.

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

All three findings are retained as optional improvements, with no source change:

1. **Positional fixture selection:** The current page has exactly the
   interaction table, the two matrices and the three affinity/score pairs that
   this regression is designed to check. Its numeric and pairing assertions
   fail closed when that fixture structure changes. Selecting by prose or
   labels and improving parse diagnostics could ease future maintenance, but
   does not correct a wrong result in the current fixture. This is the same
   maintainability suggestion raised in earlier lenses.
2. **Identifier defaults:** The Input section explicitly instructs callers to
   configure `userCol` and `itemCol` before presenting an illustrative table.
   It makes no claim that `User ID` and `Item ID` are the default names.
   Enumerating those defaults or changing illustrative labels is optional
   guidance, not a missing required API parameter or an incorrect contract.
3. **Conditional filtering wording:** Lines 121-123 expressly establish that
   the caller, not SAR, removes seen items after retrieving candidates. The
   later "Assuming ..." sentence describes the ranking under that condition;
   it does not assert a hidden setting or automatic model behavior. Repeating
   the actor would be a wording improvement, not a correctness fix.

No new current-source defect was found. Continue on the frozen patch while
preserving this original `ISSUES_FOUND` report and the separate driver triage.
