---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 5
stage_attempt: 1
round: 5
timestamp: "2026-10-20T10:38:45Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "2cbda8cbd7523f5666bd290f279eacf4a669ad62338059633d875095a34d971c"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2776/task-unknown-attempt-5-stage-attempt-1-review-5-claude-opus-5.5.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] I reviewed the full diff for the three manifest paths. I also read the pending `website/test/sarDocs.test.js` (79 lines) and `docs/Explore Algorithms/Other Algorithms/Smart Adaptive Recommendations.md`. I did not open any review-artifact directory.
- [x] I recalculated every number shown in the docs by hand:
  - All 25 co-occurrence cells from the seven-user affinity table. For example, Items 1 and 4 share Users 2, 3, 4 and 5, so the cell is 4.
  - User 1 scores: 44, 34.5, 39, 33.5 and 15.5.
  - New User scores: 2/1/1/2/3, then 8/9/7/6/5.
  - The interpretability calculation: 20 + 6 + 7.5 = 33.5.

  All values currently agree.
- [x] I traced `core/src/main/scala/com/microsoft/azure/synapse/ml/recommendation/SAR.scala` against the docs and the new help string (lines 404–405):
  - Co-occurrence counts distinct users. The code takes distinct user-item pairs (lines 233–237) and uses `countDistinct` (line 220).
  - The threshold check applies to every item pair, including an item paired with itself (line 267). Any unrecognised `similarityFunction` value falls back to raw co-occurrence (line 271).
  - Rating and time columns are found by exact name match (lines 178–179). Times are cast to strings and parsed with `SimpleDateFormat` (lines 166 and 181). Without a rating column, each interaction weighs 1.0 (line 184). Affinities are stored as 32-bit floats (line 174). A rating column must be numeric (line 317).
  - All seven documented defaults match `setDefault` (line 424).
- [x] I traced `SARModel.scala`. `topCandidates` (lines 296–309) returns the top `numRecommendations` without removing items the user has already seen. `dotProduct` (line 225) computes the same similarity × affinity scores as the docs. So the example using `cooc` and `supportThreshold=1` would reproduce exactly, because every count is at least 1 and the half-unit affinities are exact in float.
- [x] I checked existing Scala coverage:
  - `SARSpec.scala` lines 83–107 compare TLC golden files for cooc, lift and jaccard at thresholds 1 and 3. Line 212 filters seen items on the caller side after `recommendForAllUsers(10 + usersProducts.length)`.
  - `SARIdentifierSpec.scala` covers identifier, schema and save/load paths.
  - The `RankingTestBase.scala` fixture has no duplicate user-item pairs and no `timestamp` column.
  - The Scala change is only a help string, so it needs no new test. No test, doc or hand-written Python refers to either the old or new string.
- [x] The new test runs in CI for this PR:
  - In `website/package.json`, `"test": "node --test"` finds `test/**/*.js` by default.
  - `.github/workflows/website-deploy.yml` runs `npm ci`, `npm test` and `npm run build` for PRs that touch `docs/**` or `website/**`.
- [x] I checked how the assertions work:
  - `node:assert/strict` makes deep comparisons strict.
  - Table parsing tolerates CRLF line endings: the regex uses `\r?\n`, and `slice(2, -1)` drops the trailing `\r`.
  - The Interpretability regexes skip the symbolic `\*= **sim(...)` line.
  - No loop passes without checking anything: there are 25 co-occurrence assertions and 3 score-table pairs.
- [x] I reasoned through possible doc edits without running anything. The tests catch changes to the matrix, the User 1 vector or scores, the recommendation tables and the interpretability numbers. They miss the changes described in Issue 1.
- [x] I treated the validation results supplied by the driver as given: npm tests, the website build, the SAR suites, codegen, scalastyle, the JDK 11 time-parsing probe and the in-memory mutations. I ran no commands.
- [ ] No Scala test fits SAR on the doc's seven-user example to tie the docs to runtime behavior, and no explicit duplicate-interaction test exists apart from what the binary TLC fixture might contain. This is optional, not blocking: the PR changes no runtime logic, and the code trace above shows the example reproduces exactly.
- [ ] No automated check compares the documented defaults with `setDefault`. This is optional; I checked it by hand above.

## Issues

### Issue 1: Example-consistency claims added by this PR are not tested
- **Severity**: Low
- **File**: `website/test/sarDocs.test.js`; `docs/Explore Algorithms/Other Algorithms/Smart Adaptive Recommendations.md`
- **Line(s)**: test 41–79; doc 24, 30, 97, 121, 123, 145–146, 167
- **Description**: The tests check the numbers but none of the conclusions the page draws from them. These are not tested:
  - **(a) Example settings (line 97).** The page says the examples use `similarityFunction="cooc"` and `supportThreshold=1`. The tests assume this without checking it, because they score with the raw co-occurrence matrix. They would still pass if the sentence named the defaults, which would change the matrix and every score.
  - **(b) Ranking statements (lines 121, 123, 145–146 and 167).** This includes "top three are already seen" and "request all five candidates".
  - **(c) The input table.** Its `time` header and 24-hour values are not checked against the case-sensitive default `timeCol` and the `HH` pattern recommended on line 24.

  Here is a concrete edit the tests would miss. Set User 1's Item 1 affinity to 7 in both tables and update the derived numbers consistently. Item 4 (41.5) then ranks above Item 2 (40.5), so line 123 becomes false, yet all three tests still pass.
- **Risk**: These derived statements, especially the new `numItems` guidance, are where tables and text can drift apart again, which is the kind of error this PR fixes (28.5 vs. 33.5). CI would stay green while the page misleads users.
- **Suggested Fix**: Add assertions based on the parsed tables:
  - For User 1, take the seen items as the nonzero entries of the User 1 vector. Rank items by score, breaking ties by item index, and assert:
    - the top three are all seen items;
    - the unseen order is [4, 5];
    - the smallest `numItems` that keeps both unseen items is 5.
  - For the New User examples, assert the unseen-item orders: Items 1 and 4 ahead of 2 and 3, then 1, 3, 4.
  - Assert that line 97 contains `similarityFunction="cooc"` and `supportThreshold=1`, and that applying that threshold leaves the parsed matrix unchanged.
  - Assert that the input table's header is `time` and that each time value matches `^\d{4}/\d{2}/\d{2}T\d{2}:\d{2}:\d{2}$`.

### Issue 2: Position-based table lookup and exact float comparison can fail correct pages
- **Severity**: Low
- **File**: `website/test/sarDocs.test.js`
- **Line(s)**: 13–14, 43, 50–58, 63–64, 76
- **Description**: The tests identify tables only by their position in the page:
  - The first table is skipped.
  - The next two are assumed to be the similarity and affinity matrices.
  - Every later table is assumed to be part of an affinity/recommendation pair.
  - Test 3 hardcodes `[3]`, `examples[0]` and `examples[1]`.

  Line 43 is the only check on the table list, and it checks only that the count is even. It does not check for exactly three pairs or their headers. Separately, values are compared with exact deep-strict equality, which is safe only because every current value is a multiple of 0.5.
- **Risk**: Correct edits can fail with misleading errors:
  - A new table added before the matrices (for example, a parameter table in the Input section) fails with "Invalid numeric table cell".
  - A table appended at the end fails the even-count check.
  - A two-decimal weight such as 2.30 makes `3 * 2.3` evaluate to 6.8999999999999995, which does not equal the displayed 6.9 at line 76.
- **Suggested Fix**:
  - Select tables by their header and row labels: the Item-labelled matrix, the User-labelled affinity matrix, `User 1 aff/rec` and `New User aff/rec`.
  - Assert the exact expected set of tables, with clear failure messages.
  - Compare calculated values with a small tolerance, or after rounding to the displayed precision.

### Issue 3: The co-occurrence check counts positive values, not interactions
- **Severity**: Low
- **File**: `website/test/sarDocs.test.js`
- **Line(s)**: 19, 32
- **Description**: `numericRows` turns blank cells into 0, and the test counts a user only when `row[i] > 0 && row[j] > 0`. But the page defines an interaction as any nonblank cell (doc line 46). SAR also counts every distinct user-item pair, whatever its rating or time decay (`SAR.scala` lines 220 and 233–237). The test would wrongly exclude an interaction whose affinity is zero or negative:
  - a zero rating;
  - negative feedback;
  - a decayed event that, as new doc line 26 notes, contributes zero weight;
  - a small positive value displayed as 0.00.
- **Risk**: This mismatch is hidden for now. If such a cell is ever added, the test would demand a co-occurrence matrix that SynapseML would not produce, and editing the page to make the test pass would publish wrong counts.
- **Suggested Fix**: Keep blank cells distinct when parsing, for example by returning `null` for an empty cell and converting it to 0 only for arithmetic. Then count nonblank cells in the co-occurrence test.

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

- **Issue 1:** The current settings, rankings and timestamps were checked
  against the implementation and the rendered page, but the automated checks
  intentionally cover matrix and arithmetic consistency, not every prose
  claim. Additional derived-ranking assertions would improve regression
  coverage. No current incorrect ranking or setting was found.
- **Issue 2:** Table-selection diagnostics and tolerance for hypothetical
  future decimal fixtures remain optional maintenance improvements. The
  current fixed table layout and half-unit values are handled correctly.
  The table-position concern duplicates the earlier architecture review.
- **Issue 3: accepted, not yet fixed.** A read-only in-memory mutation changed
  User 7's Item 2 cell from `1.00` to `0.00`, keeping it nonblank. Under the
  page's stated interaction-presence rule, Item 2 still has four distinct users.
  The co-occurrence test failed with `4 !== 3`; the other two tests passed.
  This confirms that the test's positive-value predicate does not implement
  the documented nonblank-cell rule. No source file was changed by the probe.

The current page's positive-valued examples remain correct, and no SAR runtime
behavior was changed. The accepted test-oracle issue remains open. The current
fix-cycle budget is exhausted, so source changes and publication are paused
pending authorization for an additional focused cycle. This pass is not a
completed qualifying pre-commit review.

### Subsequent correction and validation

After the maintainer directed continuation, the focused test correction
separated textual cell parsing from numeric conversion. Co-occurrence now
counts nonblank cells, including zero and negative affinities; numeric scoring
still treats blank cells as zero. The production documentation and SAR runtime
were unchanged.

`cd website && npm test` passed all 37 tests with no failures or skips. Read-only
zero-affinity and negative-affinity mutations each passed all three SAR document
checks. The inconsistent User 1 vector and obsolete 28.5 interpretation
mutations still failed exactly their intended checks, with the other two
passing. Issue 3 is corrected locally; publication is pending. The source
change requires a complete new pre-commit pass rather than reusing this one.
