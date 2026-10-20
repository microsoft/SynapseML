---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 5
stage_attempt: 1
round: 2
timestamp: "2026-10-20T09:59:37Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "12a8d7d16acfb5089c83baaa2892eff0457370ba1b674120253001b94fb33144"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2776/task-unknown-attempt-5-stage-attempt-1-review-2-claude-opus-5.5.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Reviewed the full diff from the target to the index for all three paths in the manifest:
  - `core/src/main/scala/com/microsoft/azure/synapse/ml/recommendation/SAR.scala`: Scaladoc at L38-39 and the `supportThreshold` help text at L404-405.
  - `docs/Explore Algorithms/Other Algorithms/Smart Adaptive Recommendations.md`: the full page.
  - `website/test/sarDocs.test.js`: L1-79.
- [x] Traced the page's input-contract claims to `SAR.scala`. All of them match the page:
  - Optional columns are detected by exact name with `dataset.columns.contains` (L178-179).
  - The rating column must be numeric, enforced by `validateNumericColumnIfPresent` (L75, L314).
  - Time values are cast to strings and parsed with `SimpleDateFormat` (L181-182).
  - `timeDecayCoeff` is a half-life in days (L168), and affinities are stored as `Float` (L174).
  - Co-occurrence counts distinct users, because (user, item) pairs are deduplicated (L235) and item vectors are binary.
  - The support threshold also applies to the diagonal (L267).
  - Any other `similarityFunction` value falls through to raw co-occurrence (L271).
  - Default values are set at L424.
- [x] Traced the recommendation claims to `SARModel.scala`. Both `recommendForAllUsers` (L168) and `recommendForUserSubset` (L180) rank every item, then keep the top results with `.take(numRecommendations)` (L309). Seen items are not excluded, and unknown users produce no rows (L178). This matches the page's caller-side filtering and no-automatic-update text.
- [x] Recomputed every worked example by hand from the seven-user affinity table:
  - Co-occurrence matrix: for example, cooc(Item 1, Item 4) is 4 (Users 2-5) and occ(Item 5) is 3.
  - User 1 scores: 44, 34.5, 39, 33.5 and 15.5.
  - Single-interaction user: 2, 1, 1, 2 and 3; after adding Item 2: 8, 9, 7, 6 and 5.
  - Interpretability sum: 4·5 + 2·3 + 3·2.5 = 33.5.
  - The prose rankings at L121, L146 and L167, and the lift example comparing items 2 and 5 against item 4, also agree.
- [x] Checked repository conventions for the Scala change:
  - The help-text fix is in the code-generation source (`SARParams`), not in generated Python.
  - No signature, default or serialization changes.
  - No test asserts the old text; I searched `core/src/test` and `core/src/main`.
  - The hand-written `core/src/main/python/synapse/ml/recommendation/SARModel.py` only passes the `recommendFor*` calls through, with the parameter names the page uses.
- [x] Checked placement and style of the new test:
  - It follows the other tests in `website/test/` (`quantileRegressionScalaDocs`, `rSetupDocs`, `installDocs`, `legacyMarkdownPreprocessor`): CommonJS, `node:test`, `node:assert/strict` and paths built from `__dirname`.
  - Its handling of Windows line endings (`\r?\n`, `slice(2, -1)`) is correct.
  - `node --test` in `website/package.json` finds the file, and `.github/workflows/website-deploy.yml` runs `npm test` on pull requests that touch `docs/**` or `website/**`.
- [x] Confirmed the column defaults in `core/src/main/scala/org/apache/spark/ml/recommendation/RecommendationHelper.scala` L221-223: `UserCol = "user"`, `ItemCol = "item"` and `RatingCol = "rating"`. `SARParams` sets `timeCol` to `"time"`.
- [x] Considered but did not raise these, because they are out of scope or predate this PR:
  - `website/versioned_docs/version-1.1.3/...`, the default page per `website/versions.json`, still has the old catalog input text and the 28.5 arithmetic. Leaving it is a deliberate plan decision, and copying the new text there unchanged would be wrong because newer identifier behavior differs from 1.1.3.
  - `SARSpec.scala` L35 and `docs/Quick Examples/estimators/core/_Recommendation.md` use the misspelling `"jacccard"`, which silently selects raw co-occurrence. Both files are unchanged.
  - The hidden `<!--- --->` block at doc L77-79 still has outdated event-type wording, but it is not rendered.
  - Locale-dependent parsing of the default `startTime` and null time values are existing runtime limitations.
- [x] Treated the validation results supplied by the driver (npm test with 37 passing, website build, sbt scalastyle and compile, 29 SAR tests, codegen) as evidence only. I ran no commands.
- [x] Independence: I did not open or search any `reviews/` path or other review artifact.

## Issues

### Issue 1: Example table mixes a real column name with display labels; required column defaults are not stated
- **Severity**: Low
- **File**: docs/Explore Algorithms/Other Algorithms/Smart Adaptive Recommendations.md
- **Line(s)**: 20, 22, 30
- **Description**: The PR changed the example header `Time` to `time` (L30) so it matches the default `timeCol`. That makes the header row look like actual DataFrame column names. The headers next to it, `User ID` and `Item ID`, are not the defaults (`user` and `item`, in `RecommendationHelper.scala` L221-222), and the example never configures them. The Input section gives defaults for the optional `ratingCol` and `timeCol` (L22) but not for the two required identifier columns (L20). So the page never tells the reader what `User ID` and `Item ID` should be.
- **Risk**: A reader who builds a DataFrame from this table and keeps the default parameters gets a missing-column failure from `validateIdentifierColumn`, or may think the identifier header names matter. The failure is visible rather than silent, so the impact is low. It does weaken the PR's goal of an exact input contract.
- **Suggested Fix**: Either change the headers to `user | item | time`, or keep the display labels and add "(defaults: `user` and `item`)" after `userCol`/`itemCol` at L20. Neither change affects `sarDocs.test.js`, which skips the first table.

### Issue 2: Caller-side filtering wording was updated in two of the three worked examples
- **Severity**: Low
- **File**: docs/Explore Algorithms/Other Algorithms/Smart Adaptive Recommendations.md
- **Line(s)**: 121, 123, 145-147, 167
- **Description**:
  - The PR removed a misplaced copy of "Assuming that a same item isn't recommended again…" and replaced it with explicit caller-side wording at L121 ("If the caller filters out… SAR does not automatically exclude previously seen items"). It also rewrote the last example to "After excluding the already-interacted items…" (L167). The original sentence was left at L146.
  - The page now describes the same behavior in three different ways. L146 is the only one that doesn't say who removes the items ("isn't recommended again"), so it suggests the system avoids repeats. `SARModel` does not: it ranks all items and keeps the top ones (`SARModel.scala` L309).
  - L123 says "Request extra candidates" but gives no rule for how many. The repository's own test asks for the desired count plus the number of items the user has seen (`recommendForAllUsers(10 + usersProducts.length)`, `SARSpec.scala` L212). The five-candidate example follows that rule without stating it.
- **Risk**: The section this PR set out to clarify is now inconsistent. Readers may still assume SAR removes duplicates, or choose a small fixed buffer and get short lists for users with many interactions.
- **Suggested Fix**: Change L146 to match the caller-side wording, for example: "If the caller excludes Item 5, which this user already interacted with, items 1 and 4 have the highest score and would be recommended before items 2 and 3." Optionally, make L123 explicit: "request at least the desired count plus the number of items each target user has already interacted with, up to the total number of items." The tests do not need to change.

### Issue 3: The docs test identifies tables by their position in the page
- **Severity**: Low
- **File**: website/test/sarDocs.test.js
- **Line(s)**: 13-14, 20, 43
- **Description**:
  - `const [, similarityTable, affinityTable, ...examples] = markdown.match(/(?:^\|.*\|\r?\n)+/gm)` decides what each table is by its order in the document, and quietly discards the first (interaction) table.
  - The Input section, which this PR expanded with parameter text, comes before all of these tables. Adding any table there, or between the two matrices (for example a parameter summary or a Jaccard example), shifts every assignment so all three tests read the wrong tables.
  - The tests still fail rather than pass, but with misleading messages: "Invalid numeric table cell: Item 1" (L20), "Co-occurrence of items 1 and 1", or a bare falsy `assert.ok` (L43). None of them points to the edit that caused the failure.
  - The other docs tests in `website/test/` match specific content (regexes and phrases), not table positions.
- **Risk**: Unrelated edits to the page will produce confusing failures. A contributor could fix the wrong table or delete the check.
- **Suggested Fix**:
  - Find the two matrices between `## Collaborative Filtering` and `## Making Recommendations`, and the affinity/score pairs between `### User Recommendations` and `#### Interpretability`.
  - Before parsing, check each table's identity and fail with a clear message if it is wrong. For example: the header lists `Item 1` to `Item 5`, row labels are `**Item N**` or `**User N**`, and example headers end in ` aff` or ` rec`.
  - Add a failure message to the assertion at L43.

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
  "phase": "Pre-commit pass 5; remote validation follows publication."
}

## Driver disposition after the original review

The original feedback above is preserved. These three suggestions do not
identify an incorrect current claim or a failing supported use case, so no
source change is made:

1. **Illustrative headers / identifier defaults:** The Input section explicitly
   instructs callers to configure `userCol` and `itemCol` (line 20), and the
   table is introduced as an illustration (line 28), not an executable example
   using default parameters. Keeping its display labels requires configuring
   those names, exactly as instructed. Listing the required-column defaults or
   renaming both labels would be a useful optional clarification, not a fix for
   a claim that those labels are default column names.
2. **Filtering wording / candidate count:** Lines 121-123 explicitly establish
   caller-side filtering, state that SAR does not exclude seen items, explain
   truncation before filtering, and warn of short or empty results. The later
   sentence is conditional ("Assuming ..."), not a claim that the model performs
   that filtering. It correctly describes the displayed scores under that
   condition. Repeating the actor and adding a general overfetch bound are
   optional elaborations; no automatic filtering or fixed-buffer guarantee is
   promised.
3. **Table-position coupling:** This regression deliberately checks the worked
   example fixture in this one page, whose table order is fixed in the reviewed
   source. It is not a general Markdown-table parser. A future inserted table
   may require updating the fixture selection and can produce less helpful
   diagnostics, as the review notes, but it fails rather than silently
   accepting wrong calculations. Current tables, all three arithmetic checks,
   and the mutation evidence are correct. Semantic selectors and richer
   diagnostics are optional test-maintainability work, not a current correctness
   regression.

Disposition: no current source defect requiring a fix. Continue the remaining
lenses on the unchanged frozen patch. This is the driver's evidence-based
triage, not a replacement of the reviewer's original `ISSUES_FOUND` verdict.
