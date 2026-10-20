---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 7
stage_attempt: 1
round: 5
timestamp: "2026-10-20T15:40:00Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "2681f9f891f64c6c27d9c7d60ebdc71db116b646c1c57c216cba955b5f1c4289"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2776/task-unknown-attempt-7-stage-attempt-1-review-5-claude-opus-5.5.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Independence.** I reviewed only these files:
  - the three manifest files;
  - `SAR.scala` and `SARModel.scala`;
  - `SARSpec.scala`, which includes `SARModelSpec` and `SarTLCSpec`;
  - `SARIdentifierSpec.scala` and `RankingTestBase.scala`;
  - the `Constants` object in `RecommendationHelper.scala`;
  - `core/src/test/python/synapsemltest/recommendation/test_ranking.py`;
  - the sibling `website/test/*.test.js` files;
  - `website/package.json` and `.github/workflows/website-deploy.yml`.

  I did not open or search `reviews/` or any review artifact.
- [x] **Diff fidelity.** The current contents of the three manifest files match the supplied patch:
  - `docs/Explore Algorithms/Other Algorithms/Smart Adaptive Recommendations.md`
  - `website/test/sarDocs.test.js` (85 lines)
  - `core/src/main/scala/com/microsoft/azure/synapse/ml/recommendation/SAR.scala`

  In `SAR.scala`, only the scaladoc (lines 38–39) and the `supportThreshold` help text (lines 404–405) change. Logic, the `setDefault` values (lines 424–426), signatures and serialized params are unchanged.
- [x] **Recomputed the worked example by hand.** All values below are correct.
  - **Co-occurrence matrix:** I derived interaction presence from the affinity table (doc lines 83–91). That gives item counts of 5/4/4/4/3 and cooc(Item 1, Item 4) = 4 (users 2, 3, 4 and 5). All 25 cells of doc lines 48–54 match.
  - **User 1 scores:** 44, 34.5, 39, 33.5, 15.5.
  - **New-user scores:** 2, 1, 1, 2, 3 and then 8, 9, 7, 6, 5.
  - **Interpretability expansion (doc lines 179–180):** 20 + 6 + 7.5 = 33.5.
  - All operands are exact binary fractions (multiples of 0.5), so exact `deepStrictEqual` is sound for today's values.
- [x] **Traced how the tests parse the page.**
  - The regex at test lines 13–14 yields nine tables. The interaction table is discarded, which leaves six example tables, or three affinity/score pairs.
  - `.slice(2, -1)` drops the row label and any trailing carriage return.
  - Interaction presence uses the raw nonblank strings (line 38), so `0.00` and negative values still count as interactions.
  - Blank cells parse as 0.
  - The pre-patch values (3, 28.5 and `3 \* 5`) would fail tests 1, 2 and 3 respectively.
- [x] **Checked the documented runtime claims against the code.**
  - Rating and time columns are detected by exact name and cast to string: `SAR.scala` lines 178–182 and 317.
  - Co-occurrence counts distinct users: lines 220 and 235.
  - The threshold applies to every pair, including the diagonal. Similarity names are matched case-sensitively, and unknown names fall back to raw co-occurrence: lines 267–271.
  - A rating column with a matching name must be numeric: lines 314–319.
  - `SARModel.topCandidates` (lines 296–309) ranks every item and does not exclude seen items.
  - `rankScores` multiplies affinity rows by similarity rows, which matches the doc's arithmetic.
- [x] **CI wiring works.**
  - In `website/package.json`, `"test": "node --test"` discovers `website/test/sarDocs.test.js`.
  - `.github/workflows/website-deploy.yml` runs `npm ci` and `npm test` on PRs that touch `docs/**` or `website/**`, so a docs-only edit triggers this test.
- [x] **Searched existing coverage in `core/src/test`.**
  - No test asserts that a non-numeric rating column is rejected (`must be numeric`).
  - No test covers rating or time column names that differ only in case.
  - No test checks the documented defaults or reproduces the worked example.
  - Some paths are exercised but not asserted:
    - The misspelled `jacccard` in `SARSpec` exercises the fallback to raw co-occurrence.
    - `RankingTestBase.scala:79` sets a time column the fixture lacks, which exercises the silent omission of an absent column.
- [x] **No stale help text.** "Minimum number of ratings per item" no longer appears under `core/src`, `docs` or `tools`.
- [x] **Mocks: not applicable.** The new tests are deterministic file-parsing and arithmetic checks with no mocks, network or randomness.
- [ ] **Tests and builds were not executed** (read-only lens). The runtime results come from the driver: website `npm test` with 37 passing, 29 SAR specs, and Azure build 241817102.
- [ ] **TLC fixtures not inspected.** `demoUsage.csv.gz` and the `sim_*.csv.gz` files are gzipped, so I could not check whether repeated user-item rows indirectly pin the distinct-user co-occurrence behavior.

## Issues

### Issue 1: Ranking and candidate-count conclusions are not asserted
- **Severity**: Low
- **File**: `website/test/sarDocs.test.js`, which guards `docs/Explore Algorithms/Other Algorithms/Smart Adaptive Recommendations.md`
- **Line(s)**: test lines 47–85; doc lines 93, 121, 123, 145–146 and 167
- **Description**: The tests prove the tables are arithmetically consistent, but they don't check the conclusions the text draws from those tables. This patch added or moved several of those conclusions:
  - Line 121: Item 4 ranks before Item 5 after seen items are filtered out.
  - Line 123: "request all five candidates … since the top three are already seen." This holds only because Item 2 (34.5) narrowly outranks Item 4 (33.5).
  - Lines 145–146: the relocated ordering for the new user.
  - Line 167: the ordering after Item 2 is added to the cart.

  **A mutation that survives:** change User 1's Item 2 affinity from 3.00 to 1.00, and update every dependent cell consistently:
  - scores become 38, 26.5, 33, 29.5, 13.5;
  - the expansion becomes `4 \* 5 + 2 \* 1 + 3 \* 2.5` = 29.5.

  All three tests still pass. However, the unseen Item 4 now ranks third, so line 123 is false. Line 93's "followed by items 2 and 3" also becomes wrong.
- **Risk**: The mistake this patch fixed, where text under one example described a different example, can come back without any test failing. The new guidance on how many candidates to request could then mislead readers.
- **Suggested Fix**: Add a test that computes rankings from the parsed tables and asserts:
  - User 1's seen items are the nonzero entries in the User 1 affinity vector.
  - User 1's top three are all seen items.
  - User 1's unseen order is Item 4, then Item 5.
  - For the first new-user example, the top two unseen items are {1, 4}, followed by {2, 3}.
  - After the cart addition, the unseen order is 1, 3, 4.
  - The second new-user vector equals the first plus a weight of 2 on Item 2.

  Pair each check with an `assert.match` on its sentence, so the text and the tables have to change together.

### Issue 2: The issue #767 input contract and the stated defaults have no regression guard
- **Severity**: Low
- **File**: `website/test/sarDocs.test.js`; `core/src/main/scala/com/microsoft/azure/synapse/ml/recommendation/SAR.scala`
- **Line(s)**: doc lines 10–26, 71–73 and 97; `SAR.scala` lines 178–179, 314–319 and 424–426
- **Description**: The main fix in this patch is that SAR takes interaction data only, with no catalog DataFrame. Nothing asserts that fix or the new input contract.
  - **Website test:** it checks only the worked example. The sibling tests pin their fixes with `assert.doesNotMatch` or `assert.match`, or cross-check a source of truth: `installDocs.test.js`, `quantileRegressionScalaDocs.test.js` and `rSetupDocs.test.js`. By contrast:
    - nothing fails if the `catalog data` input bullet comes back;
    - nothing compares the defaults stated in doc lines 22–26, 73 and 97 with `setDefault` in `SAR.scala` (lines 424–426).
  - **Scala and Python tests:** none asserts two behaviors the doc now advertises:
    - rejection of a non-numeric rating column (lines 314–319);
    - silent ignoring of a `Rating` or `Time` column whose name differs from the configured one only in case (lines 178–179).

  **Two changes that no assertion I found would catch:**
  - re-adding the catalog bullet;
  - changing `supportThreshold -> 4` to 3, which would leave doc lines 73 and 97 out of date.
- **Risk**: A regression of #767, or drift between the documented and actual defaults, could merge unnoticed. The page now acts as the contract, but no test checks it.
- **Suggested Fix**:
  1. Add `assert.doesNotMatch(markdown, /^-\s+catalog data\b/im)` and `assert.match(markdown, /no separate catalog DataFrame/)`.
  2. Have the test read `SAR.scala` and assert each default against the doc, in pairs. Cover `supportThreshold`, `activityTimeFormat`, `startTimeFormat`, `similarityFunction`, `timeCol` and `timeDecayCoeff`.
  3. Optionally, add these Scala tests in `SARIdentifierSpec`:
     - a string `rating` column fails with `must be numeric`;
     - a `Rating` column is ignored when `ratingCol="rating"`;
     - fitting the doc's 7×5 example with `cooc` and `supportThreshold=1` reproduces doc lines 48–54, and User 1's `recommendForAllUsers(5)` returns the documented order and scores with seen items included.

### Issue 3: Locating tables by position and comparing floats exactly makes the test fragile
- **Severity**: Low
- **File**: `website/test/sarDocs.test.js`
- **Line(s)**: 13–14, 49, 64 and 69–70
- **Description**: The test finds each table by its position in the document. It checks only that the number of example tables is even, and that assertion has no message.
  - **Added tables shift every binding.** Examples include a purchase/click weight table near doc line 75 or a Jaccard table after line 69. Either would produce misleading failures such as "Co-occurrence of items 1 and 2" or a bare "falsy value" error.
  - **Exact comparison rejects correct decimals.** Scores use exact `deepStrictEqual`, so a correct future example with a value that isn't an exact binary fraction would fail. For example, an affinity of 2.3 gives a computed 6.8999999999999995 against a displayed 6.9.

  These failures are all false failures; none of them lets a wrong page pass.
- **Risk**: Maintenance friction and misleading failures could discourage expanding the tutorial or push someone to weaken the test.
- **Suggested Fix**:
  - Find tables by their header labels instead of position. Use `User 1 aff`, `User 1 rec`, `New User aff` and `New User rec` for the examples. Identify the co-occurrence and affinity tables by their `**Item 1**` or `**User 1**` first-column labels.
  - Assert exactly three affinity/score pairs, with a message.
  - Compare computed values at the precision displayed in each cell.

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
and original resolution placeholders. All three findings are nonblocking
suggestions; no reviewed source changed. These dispositions supersede the
placeholders without representing the raw review as clean.

1. **Ranking and candidate-count assertions: nonblocking suggestion.**
   The reviewer confirms every present matrix, score, and interpretation is
   correct. The current rankings at guide lines 121-123, 145-146, and 167 follow
   those scores. `sarDocs.test.js:32-85` deliberately guards the corrected
   distinct-user counts, vector/table relationship, matrix products, and
   interpretation arithmetic. The proposed mutation changes the fixture and
   every dependent number together; it identifies possible additional prose
   coverage, not a current faulty calculation. Sentence-matching and further
   ordering assertions are optional maintenance improvements. Any future
   seen-item oracle should preserve the page's nonblank interaction semantics
   (guide line 46 and test line 38), rather than generally equate seen items
   with nonzero numerical affinity.

2. **Input-contract/default regression guards: nonblocking suggestion.**
   Guide lines 10-26, 71-73, and 97 accurately describe the current interaction
   API and configuration. `SAR.scala:178-185,314-321,424-426` confirms optional
   exact-name matching, numeric rating validation, and the stated defaults.
   This patch changes neither those runtime behaviors nor those defaults.
   Source inspection and the published compile, wrapper, and SAR validation
   establish the current contract; the new JavaScript tests target the changed
   worked-example relationships. Sibling text guards are useful precedent,
   not a requirement to pin every sentence or pre-existing runtime branch.
   Reintroducing a catalog bullet or changing the runtime defaults would be
   a separate future change requiring review. Extra text/default assertions
   and optional Scala tests improve coverage but fix no demonstrated defect
   in this patch.

3. **Table selection and decimal comparisons: nonblocking suggestion.**
   `sarDocs.test.js:13-26,47-64,67-85` correctly parses the current nine tables
   and compares values that are all exact binary half-integers. The review
   explicitly confirms that hypothetical inserted tables or different decimal
   fixtures fail closed rather than allow an incorrect page to pass. This
   focused test is not a general Markdown or arbitrary-precision arithmetic
   library. Label-aware selection and display-precision comparisons may be
   appropriate if the tutorial's structure or number representation changes;
   neither is needed for the current fixture or its verified regressions.
