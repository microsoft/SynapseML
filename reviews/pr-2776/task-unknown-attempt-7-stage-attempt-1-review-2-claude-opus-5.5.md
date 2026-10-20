---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 7
stage_attempt: 1
round: 2
timestamp: "2026-10-20T14:52:24Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "0161480d693799612ce3cfedf9357c4b2564ea41e312c080adf06979bb25c4de"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2776/task-unknown-attempt-7-stage-attempt-1-review-2-claude-opus-5.5.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] I checked the guide's input contract against `SAR.fit` in `core/src/main/scala/com/microsoft/azure/synapse/ml/recommendation/SAR.scala`. Lines 178–179 detect the time and rating columns with an exact `dataset.columns.contains` check. Lines 75 and 317 validate the rating column only when its exact name is present. Lines 181–182 cast the time column to a string. Line 166 parses that string with `SimpleDateFormat`, and line 168 uses `2^(-Δ/(coeff·days))`, so the coefficient is a half-life in days. Line 174 stores affinities as `Float`. These all match guide lines 20–26.
- [x] I checked the co-occurrence and threshold semantics. `createInteractionMatrix` (SAR.scala:226–239) keeps only `.distinct()` user-item pairs. `collectItemCounts` (214–224) uses `countDistinct`. `similarityFeature` (260–277) applies `>= getSupportThreshold` to every pair, including the diagonal. Line 271 sends any unrecognized function name to `case _ =>` raw co-occurrence. Guide lines 46, 56, 65–73 and the new `supportThreshold` help (SAR.scala:404–405) match this behavior.
- [x] I recomputed the guide's numbers by hand. All 25 cells of the co-occurrence matrix match the seven-user affinity table (guide 81–91), including the corrected (Item 1, Item 4) value of 4. The User 1 scores are [44, 34.5, 39, 33.5, 15.5]. The new-user scores are [2,1,1,2,3] and [8,9,7,6,5]. The interpretability sum is 20+6+7.5=33.5. The rankings in the text at lines 121, 146 and 167 are correct.
- [x] I confirmed the claims about filtering already-seen items. `SARModel.topCandidates` (SARModel.scala:296–309) ranks every item and takes the top `numRecommendations`. `RankingAdapter.scala:149` calls `recommendForAllUsers(getK)` and also does no filtering. The `recommendForUserSubset` doc comment (SARModel.scala:178) says unknown users produce no output. `SARSpec.scala:212` already requests `10 + usersProducts.length` candidates and then filters, which is the pattern guide line 123 recommends.
- [x] I checked the defaults the guide quotes against `setDefault` (SAR.scala:424) and `Constants` (`core/src/main/scala/org/apache/spark/ml/recommendation/RecommendationHelper.scala:221–223`).
- [x] I checked the AGENTS.md rules on public API and serialization. Only the Scaladoc and the help string of the `supportThreshold` `IntParam` changed. The param's name, type, default and all signatures are unchanged.
- [x] I compared the new test with existing website tests (`website/test/quantileRegressionScalaDocs.test.js`, `rSetupDocs.test.js`, `installDocs.test.js`). It follows their pattern: `node:test`, strict assert, and docs read by repository-relative path. The way it picks tables out of the page differs (Issue 1).
- [x] I checked how the site is versioned (`website/docusaurus.config.js`, `website/versions.json`) and what the 1.1.3 copy of this guide still says (Issue 3).
- [x] I reviewed the driver-supplied validation evidence (website npm test, Scala style/compile, SAR specs, codegen, Azure build 241817102). I did not re-run any of it.
- [x] I considered these points and decided not to raise them:
  - The lowercase `time` table header is coherent: only rating and time are detected automatically, and wrong user or item column names fail loudly.
  - The retained phrase "Assuming that a same item isn't recommended again" (guide:146) is a condition, not a contradiction.
  - The "New User" example is adequately covered by the caveat at line 167.
  - Hard-coded defaults are stable; changing them would require approval.
  - A mismatch between the Spark session time zone and the JVM time zone is negligible next to a half-life measured in days.
- [ ] Builds and tests not executed: this is a read-only review.

## Issues

### Issue 1: The doc tests pick tables by their position in the whole page
- **Severity**: Low
- **File**: website/test/sarDocs.test.js
- **Line(s)**: 13–14, 49, 51–52, 56–58, 69–70 (compare line 71)
- **Description**: `const [, similarityTable, affinityTable, ...examples] = markdown.match(...)` assigns tables by their order in the whole page. Every table after the affinity table is then treated as an (affinity, score) pair. Test 3 takes a different approach: it anchors on the `#### Interpretability` heading. No other website test locates content by position. The current page has exactly nine tables, so every assignment is correct today.
- **Risk**: Adding any table above the similarity table would break or shift these assignments. A parameter summary in the newly expanded Input section is a natural candidate; a table after Interpretability would do the same. The resulting failures would point at the wrong place, for example "Invalid numeric table cell: User 1" or an unexplained odd-count assertion at line 49. That ties unrelated doc edits to the arithmetic tests. The failures are loud, so nothing would pass silently.
- **Suggested Fix**: Limit extraction to the relevant section text, as test 3 already does. Find each table by its header label and pair them by name: `User 1 aff` with `User 1 rec`, `New User aff` with `New User rec`. Give each lookup an explicit "missing table" message.

### Issue 2: The generated API help still omits the input traps the guide now documents
- **Severity**: Low
- **File**: core/src/main/scala/com/microsoft/azure/synapse/ml/recommendation/SAR.scala
- **Line(s)**: 384–386, 391, 413–414 (compare 404–405)
- **Description**: For `supportThreshold`, the patch follows the repository's Scala-first convention: the Param doc is corrected so the generated PySpark, .NET and R help matches the guide. Three sibling parameters owned by SAR are left alone, even though the guide now flags their behavior as a silent pitfall:
  - `similarityFunction`: the help names "Lift", "Co-occurrence" and "Jaccard" as concepts. It never lists the accepted case-sensitive values `jaccard`, `lift` and `cooc`. Any other value becomes raw co-occurrence (line 271).
  - `activityTimeFormat`: the help shows the `h` default but not the 12:xx→00:xx caveat.
  - `timeCol`: the help does not say the column is optional, matched by exact name, and silently skipped when absent.

  This is not a contradiction. The help is incomplete, not wrong. But the contract now lives only on a website page.
- **Risk**: Users who rely on `help()` or `explainParams()` can pass "Jaccard" or "Lift", or keep the 12-hour default, and silently get different similarities or time decay. The trap already shows up in the repository: `SARSpec.scala:35` and `docs/Quick Examples/estimators/core/_Recommendation.md:291,360` pass `"jacccard"`, which yields raw co-occurrence. Both files predate this patch and are outside its scope.
- **Suggested Fix**: Add one sentence to each Param doc. Names, types and defaults stay the same, and the change goes through the same codegen path the patch already validated:
  - `similarityFunction`: "Supported values (case-sensitive): jaccard (default), lift, cooc; other values are treated as cooc."
  - `activityTimeFormat`: "The default 12-hour 'h' field reads 12:xx as 00:xx; use HH for 24-hour times."
  - `timeCol`: "Optional; matched by exact, case-sensitive name; time decay is skipped when absent."

  Adding a `ParamValidator` would change behavior and is out of scope. If maintainers want to keep this PR narrow, this can be deferred.

### Issue 3: The default docs URL still serves the uncorrected guide behind #767
- **Severity**: Low
- **File**: website/versioned_docs/version-1.1.3/Explore Algorithms/Other Algorithms/Smart Adaptive Recommendations.md (not changed by this patch)
- **Line(s)**: 23, 51, 72, 120, 181
- **Description**: Docusaurus serves the newest version in `website/versions.json` (1.1.3) at `/docs/`. The patched `docs/` page appears only under `/docs/next/`. The 1.1.3 copy still has the original errors:
  - Line 23 lists "catalog data" as an input.
  - Line 72 describes the cold-item feature learner as if SynapseML uses it.
  - Line 51 shows co-occurrence(Item 1, Item 4) = 3.
  - Lines 120 and 181 show the User 1 / Item 4 score as 28.5.

  The PR says leaving versioned docs unchanged is deliberate, so this is a scope and convention point, not a defect in the three changed files. The repository does have precedent for fixing the current release's versioned docs: `website/test/installDocs.test.js:84–85,175–176,190–191` and `website/test/rSetupDocs.test.js:18` test versioned copies.
- **Risk**: Until the next release is snapshotted, the default page keeps promising a catalog DataFrame and showing inconsistent arithmetic. The confusion behind #767 stays on the page most readers land on after merge.
- **Suggested Fix**: Choose one:
  - Apply only the version-independent corrections to the 1.1.3 copy: the catalog and cold-start disclaimers, both (1,4)/(4,1) matrix cells, and the 33.5 score and arithmetic. Do not copy master-only content such as native string identifiers. Then run `sarDocs.test.js` over both files, as `rSetupDocs.test.js` does.
  - Or record the deferral explicitly in the PR body.

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

## Driver dispositions

The original review and `ISSUES_FOUND` verdict above are preserved. No product
source changed after this review.

1. **Table extraction: nonblocking fixture-maintenance suggestion.**
   `website/test/sarDocs.test.js:13-14` deliberately reads the current page's
   ordered worked-example fixture. Lines 26, 49-63 and 68-83 validate numeric
   cells, pairing, vector sizes, scores and interpretation factors. The reviewer
   confirms the present assignments are correct and future inserted tables
   would fail loudly. Section-aware parsing could improve diagnostics for a
   future fixture restructure, but does not fix a current arithmetic or
   false-green defect. The published 37-test run and recorded mutation tests
   establish the affected behavior without adding a general Markdown parser.

2. **Sibling parameter help: optional reference expansion.**
   `core/src/main/scala/com/microsoft/azure/synapse/ml/recommendation/SAR.scala:384-391,413-414`
   contains unchanged, incomplete-but-accurate descriptions. The corrected
   `supportThreshold` help at lines 404-405 previously described the wrong
   quantity, unlike these sibling strings. The guide now documents their
   relevant input pitfalls. Expanding all parameter help is a useful separate
   improvement, not a contradiction introduced by this patch; the reviewer
   explicitly permits deferral.

3. **Released documentation: explicit scope deferral.**
   `website/versions.json:2` identifies the current 1.1.3 snapshot, and
   `website/docusaurus.config.js:45` links the default documentation route.
   The PR description now explicitly states that this correction publishes
   under `/docs/next/`, that the default 1.1.3 SAR page remains unchanged, and
   that version-independent backports are deferred. This adopts the reviewer's
   second suggested resolution without copying master-specific behavior into
   a released snapshot. The source scope and review fingerprint are unchanged.
