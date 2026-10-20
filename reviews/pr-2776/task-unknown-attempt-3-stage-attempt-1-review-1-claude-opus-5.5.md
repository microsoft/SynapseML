---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 3
stage_attempt: 1
round: 1
timestamp: "2026-10-20T08:49:29Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "ff54ff02f3947437990dd734b9c3484bf8fdcf4ac3f15c40733ede66a860cefd"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2776/task-unknown-attempt-3-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] I traced the `SAR.fit` path in `core/src/main/scala/com/microsoft/azure/synapse/ml/recommendation/SAR.scala` against each new claim in the Input section:
  - **Identifiers:** `validateIdentifierColumn` accepts string and numeric identifiers.
  - **Optional columns:** the rating and time columns are used only when present (`hasTime`/`hasRating`, lines 178–184). With neither column, each row uses `lit(1.0)`.
  - **Time decay:** the weight is `2^(-Δminutes / (timeDecayCoeff·24·60))`, so `timeDecayCoeff` is a half-life in days.
  - **Defaults (lines 424–426):** time `time`, `activityTimeFormat` `yyyy/MM/dd'T'h:mm:ss`, `startTimeFormat` `EEE MMM dd HH:mm:ss Z yyyy`, decay 30, threshold 4 and similarity `jaccard`. The rating default is `rating`, from `core/src/main/scala/org/apache/spark/ml/recommendation/RecommendationHelper.scala:223`.
  - **Reference time:** when `startTime` is unset, the code parses `Date.toString` with `startTimeFormat`. That makes the advice "leave both unset" correct.
- [x] I confirmed that co-occurrence counts distinct users:
  - `createInteractionMatrix` uses `.distinct()` on user-item pairs.
  - `itemFeaturesVector` sets each user's entry to 1.0, so repeat rows do not add to the count.
  - `collectItemCounts` uses `countDistinct`.
  - `similarityFeature` applies `supportThreshold` to off-diagonal pairs and to the diagonal (`countI`).
  - The Jaccard and lift formulas match doc lines 59–63.
- [x] I recomputed the 5×5 co-occurrence matrix from the seven-user affinity table. All 25 cells match, including the corrected Item 1/Item 4 value of 4 (Users 2, 3, 4 and 5). The lift example on line 59 still holds: 2/16 for Item 2 versus 2/12 for Item 5, both against Item 4.
- [x] I recomputed every example score:
  - User 1: [44, 34.5, 39, 33.5, 15.5].
  - New user: [2, 1, 1, 2, 3], then [8, 9, 7, 6, 5].
  - Interpretability: 4·5 + 2·3 + 3·2.5 = 33.5.
  - The ranking text is correct: Item 4 ranks above Item 5, and Item 1 ranks above Item 3, which ranks above Item 4.
- [x] I checked `core/src/main/scala/com/microsoft/azure/synapse/ml/recommendation/SARModel.scala`:
  - `recommendForAll`, `rankScores` and `topCandidates` rank every destination by sim·aff and never remove seen items. This supports "SAR does not automatically exclude previously seen items."
  - `attachIdMapping` uses an inner join, so unknown users and items are dropped. `SARIdentifierSpec` covers this case and also covers string and numeric identifiers. This supports the statement that `SARModel` does not incorporate new interactions.
- [x] I confirmed that the `SAR.scala` change only edits the Scaladoc and the `supportThreshold` help text (lines 404–405):
  - The param name, type, default and setter are unchanged.
  - Spark saves param names and values, not help text, so saved models are unaffected.
  - The changed lines are within the 120-character limit.
  - A grep of `core/src`, `docs`, `website/src` and `website/test` finds no other copy of the old wording outside versioned docs.
- [x] I traced `website/test/sarDocs.test.js`:
  - The regex finds nine tables. The test skips the interaction table and uses the similarity table, the affinity table and three affinity/score pairs.
  - Blank cells become 0, CRLF line endings are handled, and the expansion and sum regexes match.
  - Exact equality is safe because all values are halves.
  - `node --test` (`website/package.json`) discovers the file. `.github/workflows/website-deploy.yml` (lines 11–16 and 56–61) runs it on pull requests that touch `docs/**` or `website/**`.
- [x] I found no security issues: no credentials, no new network calls, and the test only reads a repository file. Among unversioned docs, only the updated SAR page mentions a catalog.
- [x] These are older limitations outside this PR's scope, so I did not count them:
  - SAR inherits `coldStartStrategy` from `ALSParams` (`RecommendationHelper.scala:47`) but never reads it.
  - `SARSpec.scala:35` and `docs/Quick Examples/estimators/core/_Recommendation.md` contain the typo `"jacccard"`.
  - The default reference-time parsing depends on the system locale.
- [x] I considered the validation results you supplied (npm test, website build, scalastyle/compile, the three SAR test suites, codegen). I did not run any of them.
- [ ] I could not inspect the diagram content because it is an external image on i.imgur.com. For the claim about its catalog and cold-item paths, I relied on the PR context and the original algorithm description.

## Issues

### Issue 1: The newly documented default time format misreads noon-hour timestamps, including one in the page's own example
- **Severity**: Low
- **File**: docs/Explore Algorithms/Other Algorithms/Smart Adaptive Recommendations.md
- **Line(s)**: 22, 24, 31. Implementation: `core/src/main/scala/com/microsoft/azure/synapse/ml/recommendation/SAR.scala` lines 166, 181–182 and 424.
- **Description**: Line 22 now says time values are parsed with `activityTimeFormat` (default `yyyy/MM/dd'T'h:mm:ss`). Line 24 presents the table as timestamped input.
  - In `SimpleDateFormat`, `h` is the 1–12 clock hour. The pattern has no `a` (AM/PM) marker, so lenient parsing reads hour `12` as `0` AM.
  - As a result, line 31 (`2015/08/28T12:00:01`) parses as `2015-08-28 00:00:01`. That is about 11 hours before the previous row (`11:01:00`), not about 1 hour after it.
  - Hours 13–23 parse correctly only because lenient parsing lets the value overflow into later hours. `SARSpec.scala:154–155` relies on this with `19:39:37`.
  - Time values are converted to strings before parsing (`SAR.scala:181–182`). A Spark `TimestampType` column therefore becomes `yyyy-MM-dd HH:mm:ss` text, which the default pattern rejects.
- **Risk**: Readers who follow the new format guidance can get noon-hour events silently mis-dated. The decay weight changes by a factor of 2^(-0.5/timeDecayCoeff): about 1% at the default 30 days, but about 29% at a one-day half-life. Timestamp-typed time columns fail at fit time.
- **Suggested Fix**: Fix this in the docs only; changing the default would change behavior. After the `activityTimeFormat` default on line 22, add a sentence like: "The default pattern uses `SimpleDateFormat`'s 12-hour `h` field without an AM/PM marker, so `12:xx` is read as `00:xx`. Set `activityTimeFormat` to `yyyy/MM/dd'T'HH:mm:ss` for 24-hour times such as those below. Time values are converted to strings before parsing, so a Spark timestamp column needs a matching pattern such as `yyyy-MM-dd HH:mm:ss`."

### Issue 2: The page doesn't say which `similarityFunction` value the examples need, or what values are accepted
- **Severity**: Low
- **File**: docs/Explore Algorithms/Other Algorithms/Smart Adaptive Recommendations.md
- **Line(s)**: 53, 65 and 91. Implementation: `SAR.scala` lines 269–271, 384–386 and 425–426. Tests: `SARSpec.scala` lines 84, 93 and 102.
- **Description**: Line 91 lists the examples' non-default settings. It gives `supportThreshold=1` as a parameter value but describes the similarity only as "raw co-occurrence".
  - The page never mentions `similarityFunction` or its values, and the parameter's help text (lines 384–386) lists none either.
  - The code matches only the exact lowercase strings `jaccard` and `lift`. Any other string falls through to `case _` (line 271) and silently uses raw co-occurrence.
  - So `Jaccard`, capitalized as in the page's prose on lines 53 and 65, silently gives co-occurrence. The repository's regression tests use `cooc`.
  - The existing `jacccard` typo in `SARSpec` and the Quick Examples silently selects co-occurrence in the same way.
- **Risk**: Readers can't reproduce the examples from documented settings. Someone who picks a measure from the prose can get co-occurrence instead of Jaccard with no error.
- **Suggested Fix**: Near line 65, add: "Select the measure with `similarityFunction`: `jaccard` (default) or `lift`. Any other value, such as `cooc`, uses raw co-occurrence. Values are case-sensitive." On line 91, say the examples use `similarityFunction="cooc"` and `supportThreshold=1` instead of the defaults `jaccard` and 4. Listing the values in the help text on lines 384–386 is optional and changes no logic. Fixing the existing `jacccard` typos is out of scope.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending
- **How verified**: Pending

### Issue 2
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending
- **How verified**: Pending

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
  "phase": "Pre-commit pass 3; remote validation follows publication."
}

## Driver disposition after review

Both findings were accepted as documentation-only improvements:

- Issue 1: the input guidance now explains that time values are string-cast,
  warns about the legacy 12-hour default's noon behavior, and gives matching
  24-hour patterns for the displayed text and Spark timestamp columns. A JDK 11
  parsing probe reproduced the default's midnight result and verified that both
  documented 24-hour patterns preserve noon.
- Issue 2: the page now names `similarityFunction`, lists its case-sensitive
  values and fallback behavior, and explicitly sets `"cooc"` alongside
  `supportThreshold=1` for the worked examples.

No runtime defaults or parsing behavior were changed. The 37 website tests pass;
the existing Scala compilation, 29 SAR tests and code-generation evidence remain
applicable because their source is unchanged. The page changes invalidate this
pass and require a new complete independent pass.
