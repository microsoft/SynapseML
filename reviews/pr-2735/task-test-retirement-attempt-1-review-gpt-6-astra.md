# Test retirement review

Reviewed the agent's exact four-file patch and evidence report against
`714d365e71f6d2db5b7072094a4a3ad22485eb57`. No unresolved finding.

## Removal decisions

| Candidate | Decision and evidence |
| --- | --- |
| `tools/pytest/run_all_tests.py` | Remove. Its Scala 2.11 output path and `synapseml` test namespace do not match the current build. Tracked-text caller search found no caller. `CodegenPlugin.testPython` already invokes pytest/JUnit against `synapsemltest`; that replacement exists even at the available February 2023 history boundary. No current tests or dependency pins are removed. |
| Two `mmlStyle` loops in `VerifyValueIndexer.scala` | Remove only the loop wrappers. Neither body reads the loop variable, so both iterations have identical inputs and assertions. Both named tests, every assertion, inherited fuzzing, and the genuine metadata-style test in `TestCategoricals.scala` remain. |
| Azure Maps Spatial live tests | Already removed by microsoft/SynapseML#2485. Remove only orphaned imports now; retain address-geocoder suites and offline retired-stage contracts. Add scalar/column save-load coverage because the retired stage cannot use the ordinary live serialization fuzzer. |
| Form Recognizer v2.1, Text Analytics, legacy OCR, OpenAI deprecated aliases | Retain. Deprecated public APIs and compatibility stubs still need contract coverage. No already-passed retirement was established for these remaining live endpoints. |
| Ignored null/NaN, performance, slow service and model tests | Retain. Ignored, slow, or old does not establish redundancy or obsolescence. |

The audit covered 396 tracked test-source/helper files and examined the unused
runner separately. It found no nonempty whole-file duplicates. This is not a
claim of complete semantic deduplication.

## Review evidence

- Correctness: the categorical edit only removes duplicate execution. No named
  test or assertion is deleted; the old ignored null/NaN case remains.
- Architecture and compatibility: no production class, public signature,
  dependency, generated file, or active runner changes. Offline Maps coverage
  protects the retained reader and serialized scalar/column parameter shapes.
- Edge cases: persistence checks UID, endpoint, fake subscription key,
  coordinates/UDID, output/error schema, and the explicit retirement exception
  after load. Both scalar and column-bound instances are exercised.
- Resource safety: the added test uses the suite's managed temporary directory
  and cannot send HTTP because the retired transform throws immediately.
- Validation: baseline categorical tests passed before the edit. Final affected
  main/Test compilation and scalastyle passed under JDK 11. Twenty-seven
  distinct tests passed across six suites, with one pre-existing ignored case
  unchanged. The codegen suites validate the active generated-test path.
  Black 22.3.0 passed for 206 Python files; whitespace checks passed.
- Documentation: the existing Maps removal comment now points to the offline
  coverage. Detailed retained/rejected candidates and source evidence belong in
  the PR description rather than a new contributor rule.

## Limits

No live Azure tests were run. There is no claim of measured pipeline wall-clock
savings: the deleted runner was unused and the loop change removes two Spark
test passes. No newly retired service suite was found to delete.

Official lifecycle references checked by the audit:

- [Bing Search retirement](https://learn.microsoft.com/en-us/lifecycle/announcements/bing-search-api-retirement).
  Those tests were already removed.
- [Document Intelligence lifecycle table](https://learn.microsoft.com/en-us/azure/ai-services/document-intelligence/overview?view=doc-intel-4.0.0).
  The audit found v2.1 support ending September 15, 2027, not an already-retired endpoint.
- [Anomaly Detector overview](https://learn.microsoft.com/en-us/azure/ai-services/anomaly-detector/overview).
  Its October 1, 2026 retirement is still future at the September 21 audit date;
  its tests had already been removed in the repository.

The Azure Maps announcement redirected to an application shell, so its text
was not independently recovered. Existing source/history establishes why the
old live test is absent, not a new deletion decision.

This is a direct review, not a completed six-round, three-family gauntlet.
The advertised Gemini reviewers failed to launch with HTTP 400 during this
task. GPT and Opus reviews were available for the larger companion CI change.
Current-head Azure validation and GitHub review remain separate PR gates.
