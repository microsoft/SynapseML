# Task 5615496: attempt 2, round 1

## Original review

**ISSUES_FOUND: 2 findings.** No implementation changes were made. Full artifact content follows; no file was written.

## Review summary

- **Task / attempt / round:** 5615496 / 2 / 1
- **Theme:** Broad correctness, spec conformance, and security sweep
- **Mode:** Sequential
- **Model:** gpt-6-astra
- **HEAD:** `ee2bb4685e93ba3d0e620257254760ebcc7d61af`
- **Artifact:** `reviews/pr-2708/pr-2708-attempt-3-review-1-gpt-6-astra.md`
- **Issues found:** 2
- **Verdict:** ISSUES_FOUND

## Evidence checklist

- [x] Read the current ten-file source/documentation delta, including all three newly added Scala files, and relevant surrounding implementations.
- [x] Traced lazy cache initialization, reference-identity matching, recursive mutable-sequence rejection, and Java-list conversion to immutable lists.
- [x] Checked JSON escaping and numeric serialization against `AnyJsonFormat`; Responses caches the format fragment without freezing row-dependent verbosity.
- [x] Examined selector/schema compatibility cases, multirow loopback assertions, mutation, concurrency, serialization, replacement, copy, and persistence coverage.
- [x] Compared the previous and current request-builder dispatch paths.
- [ ] No build or runtime test was started. Validation already running elsewhere and the reported AI Functions probe results were not independently rerun. No Fabric-runtime result is claimed.

## Issues

### Issue 1: Misplaced braces prevent the cache test suite from compiling

- **Severity:** High
- **File:** `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIRequestBodySuite.scala`
- **Lines:** 22-41
- **Trigger:** Compile the cognitive test sources containing this new file.
- **Problem:** The brace at line 35 closes `CountingValues`, leaving its enclosing object open. Consequently, the class declared at line 37 is nested inside that object and immediately closes at line 39. All subsequent fixtures and test registrations belong to the enclosing object, which does not extend `TestBase`.
- **Evidence:** The current source places the first `test(...)` call at line 75 outside the `TestBase` subclass. Later calls to `spark`, `intercept`, and `fail` are likewise outside that subclass. These instance members are unavailable there; the intended top-level test class is also absent.
- **Impact:** Test compilation fails before the cache regression tests can run.
- **Suggested fix:** Close both `CountingValues` and its companion object before declaring the top-level suite. Keep the import, fixtures, and test registrations inside the class extending `TestBase`.

### Issue 2: Prepared requests bypass existing `getStringEntity` overrides

- **Severity:** Medium
- **Files and lines:**
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIChatCompletion.scala`: 132-137
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIResponses.scala`: 154-159
- **Trigger:** A subclass declared within the `openai` package overrides the existing two-argument `getStringEntity` method.
- **Problem:** Previously, both `prepareEntity` implementations invoked that overridable method. Chat now invokes the private `getStringEntityCollectionSeq` implementation directly; Responses invokes a new private three-argument overload. Neither call dispatches to the existing override.
- **Evidence:** The diff explicitly replaces both two-argument calls while retaining their non-final, package-visible method declarations. An override that rejects a request or returns a customized entity previously controlled prepared requests; the new path ignores it. This also occurs when `responseFormat` is unset.
- **Impact:** Existing custom request encoding or validation is silently skipped. Direct calls to the retained helper still dispatch correctly, so existing direct-helper tests do not detect the regression.
- **Suggested fix:** Preserve the existing virtual dispatch while introducing the prepared-builder cache. Add regression coverage that overrides the two-argument helper for both stages and exercises prepared requests.

## Reviewer follow-up on the revised cache wiring

**ISSUES_FOUND: one existing blocker remains; no new issue found in the final cache wiring.** The override-bypass finding is resolved.

## Review summary

- **Task / attempt / round:** 5615496 / 2 / 1
- **Theme:** Broad correctness, spec conformance, and security sweep
- **Mode / model:** Sequential / gpt-6-astra
- **Open issues:** 1
- **New issues in revised wiring:** 0
- **Verdict:** ISSUES_FOUND
- **Artifact:** `reviews/pr-2708/pr-2708-attempt-3-review-1-gpt-6-astra.md`

Artifact content is returned here; no files were modified.

## Evidence checklist

- [x] **Cache lifetime is per stage/executor instance, not per prepared builder.** Each Chat/Responses instance owns one transient, lazily initialized selector. Executor-side deserialization rebuilds its cache.
- [x] The selector publishes the parameter-reference/encoder pair together through a volatile tuple. Replacement is synchronized; each caller retains a consistent encoder snapshot.
- [x] Payload identity checking still prevents a cached fragment from replacing a different supplied format. Recursive immutability checks preserve mutable-sequence behavior.
- [x] Column-driven and unset formats retain uncached encoding. Responses caches only `text.format`, leaving verbosity dynamic.
- [x] Both original `prepareEntity` calls and two-argument `getStringEntity` virtual dispatch are restored.
- [x] Revised tests cover transient stage serialization, reuse across builders, bounded replacement, and helper overrides.
- [ ] No builds, Spark jobs, or compatibility replays were run. Their completion is not claimed.

## Issue 1: Test-suite scope error remains open

**Severity:** High
**File:** `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIRequestBodySuite.scala`
**Lines:** 22-41
**Trigger:** Compile the cognitive test sources.

The current file still leaves the companion object open after closing `CountingValues` at line 35. The `OpenAIRequestBodySuite` class at line 37 is consequently nested inside that object and closes at line 39. The fixtures and test registrations that follow belong to the object, not the class extending `TestBase`.

The first registration at line 75 and subsequent uses of `test`, `spark`, `intercept`, and `fail` therefore lack the required `TestBase` instance. This remains a compilation blocker.

**Suggested fix:** Close the helper class and companion object before declaring the top-level suite; keep its import, fixtures, and tests inside that suite.

## Resolution note: Original issue 2

The final wiring resolves the override bypass:

- `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIChatCompletion.scala:134-138`
- `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIResponses.scala:156-160`

Both now invoke the original overridable helper. Caching is confined to final payload encoding, and the new override regression test checks both dispatch paths. This resolution is source-verified; test execution remains blocked by issue 1.

## Driver resolution log

- Issue 1: Moved the companion-object closing brace before the top-level suite declaration and kept all tests inside the `TestBase` class. This restores the intended discovery and instance-member scope. Final cognitive test compilation and execution are pending.
- Issue 2: Replaced the prepared-builder wiring with a bounded, transient, thread-safe stage-local selector. The original virtual calls and helper signatures remain unchanged. Added override-dispatch tests for both APIs. Fresh Spark 4.0 and 4.1 three-way replays passed and retained port-specific sequence handling; final runtime tests are pending.

### Runtime verification

Both findings are fixed and verified. Core and cognitive main/test compilation and Scala style checks passed. The final targeted run passed 25 core tests and 49 cognitive tests, with no failures, cancellations, ignored tests, or pending tests. This includes all 18 cache tests, the warm/cold executor-copy cases, and both overridden helper paths. Round 2 independently confirmed the corrected test-class scope and restored dispatch. Code generation and current-head CI are separate remaining gates.
