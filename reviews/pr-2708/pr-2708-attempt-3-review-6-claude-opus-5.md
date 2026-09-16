# Attempt 2, Round 6 Review: Final Polish, Performance, Documentation

| Field | Value |
| --- | --- |
| **Task** | 5615496, attempt 2 |
| **Round** | 6 (final) |
| **Model** | claude-opus-5 (max) |
| **Mode** | sequential |
| **Theme** | Polish, performance, documentation |
| **Base** | frozen delta vs. `HEAD` `ee2bb4685e`, `master` `133c38a1f3`; source unchanged since Round 3 |
| **Issue count** | **0** |
| **Verdict** | **CLEAN** |

Read-only. No builds, Spark, edits, subagents, or external actions. Private AI Functions source not inspected.

## Findings

None.

## Verification detail

### Hot-path overhead

Per request the cache adds a single volatile read, two allocation-free `Option.orNull` calls, and one `eq` (`OpenAIRequestBody.scala`:86-88). On the unset-format path `get`/`getDefault` both return the `None` singleton, so `encode` reaches `OpenAIRequestBody.Uncached` and degrades to the original `payload.toJson.compactPrint` (:46-47) behind one `isEmpty` check. The fallback is genuinely free.

The one new per-request cost is `JsString(name).compactPrint` for path-node keys (:34), which allocates a small builder per key where spray-json's `printObject` would append into the shared one. It is bounded by root key count plus, for Responses, the `text` key count, under ten in practice, and the measured net is strongly negative in every case, so no action is warranted.

### Performance evidence

`schema-cache-benchmark.json` corroborates the reported allocation range at 2000 rows per trial:

| Properties | Chat | Responses |
| --- | --- | --- |
| 1 | 45.41% | 35.95% |
| 10 | 57.31% | 53.87% |
| 100 | 78.56% | 78.26% |

The artifact's own `scope` field states "steady-state local request JSON encoding including cache lookup; not LLM or network latency", which correctly bounds the claim. Time reductions are present in the data but, as instructed, are not claimed here; only the allocation range and the deterministic traversal counts asserted in `OpenAIRequestBodySuite` are load-bearing.

### Bounded memory

Each stage retains one `(value, encoder)` tuple. The encoder holds a reference the `ParamMap` already retains plus one `cachedJson` string, so incremental footprint is proportional to the format, never to row count or to the number of distinct formats observed. A -> B -> A recreates A in this one-entry cache rather than growing it, the intended bound, asserted as `creations.get() == 3` in `The bounded cache replaces changed formats and ignores column-dependent values`. `Uncached` is a stateless shared singleton, so unconfigured stages allocate nothing.

### Locking and non-eager failure

Lock acquisition occurs only on encoder replacement; hits are lock-free, and `encoder.encode` runs outside the monitor (:95). `cacheable` is evaluated last in the `&&` chain (:24) and `cachedJson` is a `lazy val` (:15-16), so `prepareEntity` serializes nothing, the basis for the suite's `counter.get() == 0` precondition. A throwing initializer is not memoized, so failures re-raise instead of yielding a stale body.

### Logging

`OpenAIRequestBody.scala` imports no logger and emits no diagnostics. This is correct: falling back to uncached encoding for a mutable sequence or a column-driven format is an expected optimization miss, and any per-row log would be noise at Spark scale. The condition is instead made observable through tests and documentation.

### Compatibility

`OpenAIRequestBody` and `OpenAIRequestBodyCache` are `private[openai]`, so there is no new public API, no codegen surface, and no Python wrapper change. No Spark params were added, leaving the serialized param shape and previously persisted stages loadable, exercised by the copy and save/load tests on both APIs. The two-argument `getStringEntity` signature is untouched and subclass dispatch is asserted. `OpenAIPrompt` delegates to fresh inner stages, each with its own cache, so the schema is serialized once per transform rather than once per row, with all three loopback rows asserted to carry it.

On `ServiceParam.toScalaAny` (`JsonEncodableParam.scala`:63), the prior `.toSeq.map(...)` already produced an independent copy, since `Buffer.map` builds a new buffer. The change is therefore one of **type**, mutable `Buffer` to immutable `List`, which is precisely what makes Python-supplied formats eligible under `isImmutable`. `VerifyJsonEncodableParam`'s new `isInstanceOf[immutable.Seq[_]]` assertion is the one that captures the new property; the post-conversion `javaList.add` assertion is a regression guard on behavior that already held.

### Documentation accuracy

The added Markdown at `docs/Explore Algorithms/OpenAI/OpenAI.ipynb`:452-460 states no numeric performance claim, so there is nothing to reconcile against the benchmark. It does not assert byte-identical output, which matches the actual contract: parsed-JSON equality plus preserved schema-property order, not arbitrary root-key byte order. The sentence "The format is still included in every HTTP request; caching does not cache model responses or reduce the number of requests" forecloses the most likely misreading. Every remaining claim, lazy once-per-stage serialization, replacement on change, non-persistence with per-executor rebuild, immutable copying of Python inputs, mutable Scala sequences staying uncached, column formats and Responses verbosity staying row-dependent, was traced to source in Round 3 and the source has not changed.

### Naming

`OpenAIRequestBody` / `OpenAIRequestBodyCache` / `Uncached` / `cacheable` / `cachedJson` / `formatPath` read unambiguously. `scalar` matches the existing `setScalarParam` / `setVectorParam` vocabulary for the `Left` branch of a `ServiceParam`. The `chat` and `responses` factories mirror their stages. No renaming warranted.

## Scope statement

Covers the frozen delta only. The regenerated Python rerun is reported as in progress and its outcome is not asserted; the earlier 20 tests / 116 subtests and eight notebook loopback cases are cited as prior evidence, not as final. No Databricks or Fabric runtime result is claimed. Sequential six-round review for attempt 2 complete.

## Driver precision notes

- Unconfigured stages still initialize one cache holder and its tuple on first use and pay a small lookup cost. Only the uncached encoder is shared. No zero-allocation or zero-overhead claim is made.
- Path-key work is bounded by the actual payload's root/text key counts, not a universal ten-key limit.
- Prompt amortization is per executor-side inner-stage instance, not one global serialization for an entire distributed transform. The schema still needs copying into, and UTF-8 encoding with, every request body.

## Final local runtime verification

After code generation, the final Python run passed 20 tests and 116 subtests with zero failures, errors, or skips. Provenance checks confirmed the worktree JVM classes and regenerated Python wrappers. All eight exact notebook loopback cases passed, covering default and custom non-strict settings across direct Chat, direct Responses, and both Prompt modes. The test-only model-loading opt-in was restored to its original setting. Together with the 74 Scala tests, this completes the local runtime gates; current-head CI remains a separate post-push gate.
