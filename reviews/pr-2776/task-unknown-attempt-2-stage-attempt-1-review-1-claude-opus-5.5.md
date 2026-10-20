---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 2
stage_attempt: 1
round: 1
timestamp: "2026-10-20T08:31:39Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "04542c964cf277aeca9ce100531f4c270c8bfac2995a63e08223bbd5b3570ef1"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2776/task-unknown-attempt-2-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Diff matches the working tree.** The supplied diff matches the three manifest files in the worktree: `core/src/main/scala/com/microsoft/azure/synapse/ml/recommendation/SAR.scala`, `docs/Explore Algorithms/Other Algorithms/Smart Adaptive Recommendations.md`, and `website/test/sarDocs.test.js` (55 lines).
- [x] **Co-occurrence counts distinct users.** In `SAR.scala`, `createInteractionMatrix` builds from `.distinct()` (user, item) pairs. `itemFeaturesVector` assigns `values(user) = 1.0` instead of adding to it, and `collectItemCounts` uses `countDistinct`. So both co-occurrence and the diagonal count distinct users, and repeated interactions add nothing. Doc lines 42, 52 and 61 are accurate.
- [x] **Threshold, defaults and formulas match.** At `SAR.scala` line 267, `cooccurrence >= getSupportThreshold` applies to every index, including the diagonal; pairs below it get `0.0f`. Line 424 sets defaults `supportThreshold -> 4` and `similarityFunction -> "jaccard"`. The Jaccard and lift formulas match doc lines 57 and 63. The new `IntParam` description (lines 404–405) correctly states a necessary condition for nonzero similarity.
- [x] **Affinity and time handling match.** In `calculateUserItemAffinities`:
  - Rating and time columns are used only when present; without a rating column the weight is `lit(1.0)`.
  - The decay `2^(-Δmin/(coeff·1440))` makes `timeDecayCoeff` a half-life in days.
  - If `startTime` is unset, the code parses `Calendar…toString` with `startTimeFormat`. That makes the doc's advice at line 22 ("leave both unset") correct.
  - Defaults match: `C.RatingCol = "rating"` (`core/src/main/scala/org/apache/spark/ml/recommendation/RecommendationHelper.scala` line 223), `timeCol -> "time"`, both time formats, and `timeDecayCoeff -> 30`.
- [x] **Scoring and filtering claims match `SARModel.scala`.** `topCandidates` ranks every destination item and excludes nothing previously seen. Scores are the fit-time user `flatList` times the item `itemAffinities` transpose. `transform` and the subset APIs inner-join stored factors, so unknown IDs are dropped (`SARIdentifierSpec`, "SAR drops unknown and null identifiers during scoring"). Doc lines 115 and 159 are therefore accurate. `SARSpec`'s TLC tests filter seen items on the caller side (`filterScore`) and select raw co-occurrence with `"cooc"`.
- [x] **Every number recomputed by hand.**
  - All 25 co-occurrence cells against the 7-user affinity table: only (1,4)/(4,1) changed, and 3→4 is correct.
  - User 1 scores: [44, 34.5, 39, 33.5, 15.5].
  - Single-interaction user scores: [2, 1, 1, 2, 3], then [8, 9, 7, 6, 5].
  - Interpretability: 4·5 + 2·3 + 3·2.5 + 0 + 0 = 33.5.
  - The lift example still holds (2/16 < 2/12), and the ranking prose at lines 115, 137–138 and 159 agrees.
- [x] **Test mechanics are sound.**
  - The `gm` regex handles CRLF line endings.
  - The tables are matched in order: input table, similarity, affinity, then three aff/rec pairs.
  - Blank cells become 0, and the extra empty affinity column is ignored.
  - All values are exact binary fractions, so `deepStrictEqual` is safe.
  - Table rows have no trailing whitespace or tabs, and failure modes are loud.
  - `website/package.json` runs `node --test` (CommonJS, Node ≥24).
  - `.github/workflows/website-deploy.yml` runs `npm test` on PRs that touch `docs/**`.
- [x] **Rendering path is unaffected.** `tools/docgen/docgen/channels.py` copies `.md` files verbatim into `website/docs/`. `website/legacyMarkdownPreprocessor.js` only rewrites table cells containing `{…}`. The new prose adds no MDX-sensitive or remark-math characters (`{`, `<`, `$`) outside code spans.
- [x] **`SAR.scala` change is documentation-only.** Only the class doc and the `IntParam` description change; the param name, type, default, setter and serialization shape are unchanged. The edited scaladoc line 38 is 118 characters (limit 120 in `scalastyle-config.xml`). No other current source or doc uses the old description string.
- [x] **No security concerns.** The PR adds no product code, network access, secrets or dependencies. The test reads one file from the repository.
- [x] **Driver-supplied validation reviewed as evidence only.** I did not run the npm, website-build, scalastyle/compile or SAR-suite commands myself.
- [ ] **External content not checked.** I did not fetch the imgur diagram or the linked original SAR description: this review is read-only with no remote calls. Claims about their content come from the PR context.
- [x] **Pre-existing problems found, not counted (outside this PR's scope):**
  - The default `activityTimeFormat` uses the 12-hour `h` with no AM/PM marker, so the unchanged example time `12:00:01` parses as 00:00:01.
  - When `startTime` is unset, the fallback parses `Date.toString()` with a default-locale `SimpleDateFormat`, which fails on non-English JVM locales.
  - `docs/Quick Examples/estimators/core/_Recommendation.md` and `SARSpec` pass the misspelling `"jacccard"`, which silently selects co-occurrence.
  - Rating and time column detection is an exact, case-sensitive match.

## Issues

### Issue 1: The page never says the similarity example comes from the affinity-table users
- **Severity**: Low
- **File**: docs/Explore Algorithms/Other Algorithms/Smart Adaptive Recommendations.md
- **Line(s)**: 42–52, 75
- **Description**: The correction of cooccur(Item 1, Item 4) from 3 to 4 (lines 46 and 49) is only valid because the similarity matrix is computed from the seven users in the affinity table (lines 77–85). The page never says this. The similarity example ("Let's look at the following example:", line 42) comes before the affinity table and is presented as a separate example. The new line 52 says "three distinct users interacted with both items 1 and 2", but those users are never identified. The only place the relationship is stated is the test name (`website/test/sarDocs.test.js` line 26).
- **Risk**: Readers cannot check the new distinct-user explanation or see why the corrected value is 4. Impact is low because the test prevents a silent revert.
- **Suggested Fix**: Add one sentence near line 52 or line 75, for example: "The example co-occurrence counts are computed from the seven users in the affinity matrix below, treating each nonblank cell as one interaction."

### Issue 2: The tests don't check the User 1 vector or the interpretability arithmetic this PR corrected
- **Severity**: Low
- **File**: website/test/sarDocs.test.js
- **Line(s)**: 41–55 (doc lines 97–103 and 171–172)
- **Description**: The scoring test checks each aff/rec table pair against the similarity table, but two hand-maintained derivations are unchecked:
  - (a) The "User 1 aff" vector (doc lines 97–103) is never compared with User 1's row in the affinity table (line 79).
  - (b) The interpretability expansion and total (lines 171–172) are not checked. This is the same arithmetic that was stale (28.5) and that this PR fixes.

  If someone later edits Item 4's similarity row or User 1's affinities, the test forces the score tables to be updated. These prose lines could still go stale while CI passes.
- **Risk**: The kind of doc drift this PR fixes could come back unnoticed. Impact is low because this is documentation only.
- **Suggested Fix**:
  - Assert that `numericRows(examples[0]).map((r) => r[0])` deep-equals `numericRows(affinityTable)[0].slice(0, similarity.length)`.
  - Parse the `a \* b` pairs on the CRLF-tolerant `_= …_` line and assert they equal `similarity[3][k]` and the User 1 affinity for each `k`.
  - Assert that the final bold total on the next line equals the Item 4 value in the "User 1 rec" table.

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

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "Repository six-lens requirement overrides the low-complexity estimate.",
    "models": "Installed review-code direct Medium contract, complementary to the OpenAI primary.",
    "stageBehavior": "Repository review-gauntlet.md requires six sequential themes unless parallel mode was requested.",
    "safety": "Trusted external-contributor guidance and the explicit maintainer follow-up request."
  },
  "changeSize": "Three source files: one algorithm page, two Scala documentation/help-text corrections, and two JavaScript documentation consistency tests.",
  "cyclomaticComplexity": "Low: prose, parameter help text, and deterministic table comparisons; no algorithm or service changes.",
  "rationale": "Use one complementary reviewer per lens for the mandatory six-lens policy. Require all six clean on the same frozen source patch.",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoningEffort": "max",
  "modelSource": "Current task-tool live catalog supports claude-opus-5.5 with max reasoning.",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md from independently trusted guidance at 5a3e29f1a3c0d3b9649a2168279ef4f3be06dcc4",
  "phase": "Pre-commit pass 2; remote validation follows publication."
}

## Driver disposition after review

Both findings were accepted and addressed in the pending source:

- Issue 1: the similarity introduction now explicitly connects the co-occurrence
  matrix to the seven users in the later affinity table. It describes a nonblank
  cell as evidence of an interaction, not as exactly one event.
- Issue 2: the tests now compare the User 1 vector with its affinity-table row
  and verify the Interpretability factors, individual products and total against
  the displayed matrices and Item 4 recommendation score.

The three focused SAR documentation tests and the full 37-test website suite
pass. Read-only, in-memory mutation checks demonstrate that changing only the
affinity-table weight fails the new vector consistency assertion, and restoring
the obsolete total of 28.5 fails the Interpretability assertion. The source
changes invalidate this pass; a complete new independent pass is required.

The tool-scope audit found only source-context reads. The driver's initial
scope guard rejected root-relative globs such as `website/test/**` because they
omitted a separate `paths` argument; those patterns were already anchored to
source directories and did not read prior review evidence.
