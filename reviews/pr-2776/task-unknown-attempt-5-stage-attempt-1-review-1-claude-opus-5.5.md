---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 5
stage_attempt: 1
round: 1
timestamp: "2026-10-20T09:48:48Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "e188d25e61b079c105cf4e1679a638b21baebceef5a56065bb61afc546d72059"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2776/task-unknown-attempt-5-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Input section matches the code.** I checked `docs/Explore Algorithms/Other Algorithms/Smart Adaptive Recommendations.md` lines 20–26 against `core/src/main/scala/com/microsoft/azure/synapse/ml/recommendation/SAR.scala`:
  - **Column detection:** The rating and time columns are found by exact, case-sensitive name (`dataset.columns.contains`, lines 178–179). The numeric check also uses an exact name match (line 75 → `validateNumericColumnIfPresent`).
  - **Weights:** A matching rating multiplies the time-decay weight. With no rating column the weight is `lit(1.0)` (lines 180–185).
  - **Time parsing:** Time values are cast to strings and parsed with `SimpleDateFormat(activityTimeFormat)` (lines 166, 181–182). `startTime` is parsed with `startTimeFormat`, falling back to the current time via `Date.toString` (line 162).
  - **Half-life and storage:** The decay is 2^(−days/`timeDecayCoeff`) (line 168), and affinities are stored as `Float` (lines 172–174).
  - **Defaults:** All documented defaults match lines 424–426.
  - **The 12-hour `h` pattern:** Java's lenient parsing turns hour 12 into 0 and keeps hours 13–23, so the "`12:xx` as `00:xx`" warning is exact.
- [x] **Similarity rules match the code.** Item pairs are counted from distinct (user, item) pairs (line 235), and per-item totals use distinct users (`countDistinct`, line 220). The `>=` threshold applies to every pair, including an item with itself (line 267). Function names are case-sensitive, and any other string falls back to raw co-occurrence (lines 269–271). The new `supportThreshold` help text (line 405) is accurate. Only the help text changed: the parameter name, type, default and saved values are the same.
- [x] **Worked example recomputed by hand from the seven-user affinity table:**
  - The co-occurrence matrix (doc lines 50–54) is correct, including the corrected Item 1 / Item 4 value of 4 (Users 2, 3, 4 and 5).
  - User 1's scores are 44, 34.5, 39, 33.5 and 15.5.
  - The two new-user score vectors are [2,1,1,2,3] and [8,9,7,6,5].
  - The interpretability sum is 4·5 + 2·3 + 3·2.5 = 33.5 (doc lines 179–180).
  - All three of User 1's top items are already seen, so five candidates are needed (doc line 123). The "Item 1, then 3, then 4" order holds (line 167).
- [x] **Recommendation behavior matches `SARModel.scala`.** `topCandidates` (lines 296–309) ranks every item and does not remove items the user has already seen. Scores are the user's affinities times the item-similarity matrix (line 327), which matches the sim(Item 4, Item k) · aff(User 1, Item k) breakdown. `recommendForUserSubset` drops users it has not seen before, which fits the statement that a fitted model does not pick up new interactions.
- [x] **New test file is sound.** In `website/test/sarDocs.test.js`:
  - The table regex picks up all nine tables in order and skips the first (interaction) table.
  - It handles Windows (CRLF) line endings.
  - Blank cells count as 0, and any non-numeric cell fails `Number.isFinite`.
  - A missing table or heading makes the test fail rather than pass silently.
  - All values are multiples of 0.5, so the exact (strict) equality checks are safe.
  - `node --test` in `website/package.json` discovers the file by default.
  - The test makes no network or process calls.
- [x] **The page builds cleanly.** It contains no `$`, `{`, `}`, JSX tags or trailing spaces, so remark-math, MDX and the table regex are safe. `website/legacyMarkdownPreprocessor.js` does not touch the new lines. No new external resources or secrets were added; the image link was already there.
- [x] **Existing tests agree with the new text.**
  - `SARSpec.scala`: the TLC fixtures cover cooc, lift and Jaccard. `SarTLCSpec` already requests `10 + seen` candidates and then filters, which is the caller-side pattern the page now documents.
  - `SARIdentifierSpec.scala`: covers string and numeric IDs, rejection of null IDs, and dropping unknown IDs.
  - `RankingTestBase.scala`: `setTimeCol("timestamp")` names a column that does not exist, so no time decay is applied — the silent behavior the page describes.
- [x] **Driver-supplied validation was treated as evidence only; I did not re-run it.** This covers `npm test`, the website build, the JDK 11 parsing probe, scalastyle and compile, the 29 SAR tests, and codegen.
- [ ] **Not counted as issues** (already present before this PR, or optional extra explanation):
  - `docs/Quick Examples/estimators/core/_Recommendation.md` lines 291 and 360, and `SARSpec.scala` line 35, pass `"jacccard"`. `SAR.scala` line 271 silently treats that as raw co-occurrence.
  - The unchanged `similarityFunction` help text does not list the lowercase accepted values.
  - Doc line 123 could optionally state how many extra candidates are enough: the desired count plus the most items any user has already seen.
  - Null rating/time values and parameters inherited from Spark's ALS are existing behavior and outside this PR's scope.

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
