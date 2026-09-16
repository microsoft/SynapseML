# Attempt 2, Round 3 Review: Edge Cases & Reliability

| Field | Value |
| --- | --- |
| **Task** | 5615496, attempt 2 |
| **Round** | 3 |
| **Model** | claude-opus-5 (max) |
| **Mode** | sequential |
| **Theme** | Edge cases / reliability |
| **Base** | uncommitted delta vs. `HEAD` `ee2bb4685e93ba3d0e620257254760ebcc7d61af`, `master` `133c38a1f3` |
| **Issue count** | **0** |
| **Verdict** | **CLEAN** |

Read-only. No builds, no Spark, no edits, no commits, no external posts, no agents. Review Markdown under `reviews/` excluded.

## Files reviewed

| Path | Change |
| --- | --- |
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIRequestBody.scala` | added, 98 lines |
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIChatCompletion.scala` | +5/-2 |
| `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIResponses.scala` | +5/-2 |
| `core/src/main/scala/com/microsoft/azure/synapse/ml/param/JsonEncodableParam.scala` | 1 line |
| `cognitive/src/test/scala/.../OpenAIRequestBodySuite.scala` | added, 356 lines |
| `cognitive/src/test/scala/.../OpenAIResponseFormatCompatibilitySuite.scala` | added, 129 lines |
| `cognitive/src/test/scala/.../OpenAIResponseSchemaSuite.scala` | 1-row to 3-row loopback routes |
| `core/src/test/scala/.../VerifyJsonEncodableParam.scala` | +20 |
| `cognitive/src/test/python/.../test_OpenAIResponseSchema.py` | +36/-3 |
| `docs/Explore Algorithms/OpenAI/OpenAI.ipynb` | +11 Markdown |

## Findings

None. The areas below were examined against the source and no concrete defect was verified.

## Verification detail

### Byte-parity between cached and uncached encodings

`OpenAIRequestBody.write` (`OpenAIRequestBody.scala`:23-43) hand-rolls object emission only for nodes on `formatPath`; every other subtree goes through `anyFormat.write(...).compactPrint` (:40). Parity holds on all three axes:

- **Key order.** The uncached path is `anyFormat.write(map)` -> `JsObject(ListMap(v.toSeq.map(...)))`, and spray-json's `CompactPrinter.printObject` iterates that `ListMap` in insertion order. The cached path iterates the same map via `fields.foreach` (:30). `Map.toSeq` and `Map.foreach` both delegate to `iterator`, so the emitted order is identical.
- **Escaping.** Keys use `JsString(name).compactPrint` (:34), which routes into the same `JsonPrinter.printString` that `printObject` uses for members. `ContentType.APPLICATION_JSON` still supplies UTF-8, unchanged at `OpenAIChatCompletion.scala`:553 and `OpenAIResponses.scala`:559. Covered by "Cached schema fragments cannot inject fields or alter message escaping", which round-trips `"`, `}`, `{`, `,`, `\n` and `\u263a` through both the message body and the cached enum.
- **Structural fallback.** When a path node is not a `Map`, or the payload lacks the path key entirely, `(value, path)` falls to `case _` (:39) and the whole subtree is emitted by `anyFormat`. There is no code path that emits a partially-built object and then diverges.

### `isImmutable` is exactly bounded by what `AnyJsonFormat` can serialize

`OpenAIRequestBody.scala`:60-65 returns `false` only for `scala.collection.Seq` that is not `immutable.Seq`, and `true` for everything not a `Map`/`Seq`. I checked the over-approximation for reachable staleness:

- `scala.collection.mutable.Map` is not `Predef.Map` and not a `Seq`, so it reaches `case _ => true`. It is nevertheless **not a staleness risk**, because `AnyJsonFormat.write` (`UntypedArrayParam.scala`:35) also matches only `Predef.Map`, so a nested mutable map throws `IllegalArgumentException` on the cached and uncached paths alike.
- The same holds for `java.util.List`/`java.util.Map`, `Array`, and `mutable.Set`, all unserializable by `anyFormat` in both paths.
- The only mutable value `anyFormat` *can* serialize is a mutable `scala.collection.Seq` (the unqualified `Seq[_]` arm), and that is precisely the case `isImmutable` rejects at :63.

The guard is therefore complete rather than merely conservative, and it is written with explicit `scala.collection.immutable.Seq` / `scala.collection.Seq` qualifications, so it carries the same meaning on the 2.13 port branches. In-place mutation is covered end-to-end by the per-API `does not cache formats containing mutable sequences` tests, which mutate `values(0)` between requests and assert the new value reaches the wire.

### Concurrency and cache lifetime

`OpenAIRequestBodyCache` (:78-96) stores one `@volatile` immutable `Tuple2`. A single volatile read into `previous` (:86) yields a consistent `(value, encoder)` pair, so the lock-free hit cannot pair an encoder with a foreign value. The slow path re-checks inside `synchronized` (:90) and returns `current._2` from within the critical section, so the returned encoder is always the one built for a reference `eq` to the caller's. `encoder.encode(payload)` runs **outside** the monitor (:95), so the `cacheable`/`cachedJson` lazy-val initialization never occurs while the cache lock is held, with no lock ordering between the two monitors.

Exactly one `create` occurs per distinct value reference, which is what makes the exact traversal counts in the suite deterministic rather than flaky: "Concurrent requests initialize one cached format" asserts `counter.get() == 2` across 20 tasks on 4 threads, and that count is only reachable if `isImmutable` and `anyFormat.write` each run once.

### Initialization failure is not memoized as success

`cachedJson` (:16) is a `lazy val`; a throwing initializer is not cached in Scala, so a later access re-throws rather than yielding a stale body. `cacheable` is evaluated last in the `&&` chain at :24, so nothing is traversed until a cached leaf is actually reached, which is what lets the suite assert `counter.get() == 0` immediately after `prepareEntity`. The "Cached encoding preserves serialization failures rather than returning a stale successful body" test confirms a valid format followed by an unserializable one raises on both the existing and a freshly prepared builder.

### Parameter replacement, null, unset, and column-valued formats

- `scalar` (:67-68) filters `Left(null)`, so `Left(null)` degrades to `Uncached`.
- `Right(column)` never produces a `cachedValue`, so column-driven formats stay row-dependent, asserted by both `Row-dependent ... remain row-dependent` tests and by `creations.get() == 4` in the bounded-cache test.
- `OpenAIRequestBody.responses` (:73-76) additionally requires a `Map`-typed `format` member, falling back to `Uncached` otherwise.
- `matches` (:18-21) uses a typed pattern, which does not match `null`, so a null payload value falls through to `anyFormat` and prints `null` as before.
- Replacement works because `setScalarParam` allocates a fresh `Left`, changing the reference the cache compares. `Changing a scalar response format does not reuse an earlier cached value` asserts the old name is absent, not merely that the new one is present.
- Identity is checked only at the leaf, not at intermediate path nodes, which is why the per-row merged `text` map (carrying row-level `verbosity` alongside the static `format`) still hits the cache, as shown by `Responses caches its format without freezing row-specific verbosity`.

### Serialization, copy, persistence, dispatch

`@transient private lazy val requestBody` (`OpenAIChatCompletion.scala`:109, `OpenAIResponses.scala`:133) marks both the value field and its bitmap transient, so a deserialized stage rebuilds its own cache; `OpenAIRequestBodyCache` is never serialized despite not being `Serializable`. The executor-copy test exercises warm and cold stages across both APIs and asserts `4` vs. `2` traversals, which is only well-defined because the shared `AtomicInteger` is serialized inside the same object graph as the stage. `copy(ParamMap)` and save/load are covered for both APIs, including `loaded.getResponseFormat == copied.getResponseFormat`. The two-argument `getStringEntity` signature and `prepareEntity` are unchanged, and `Prepared requests preserve Chat and Responses request-helper overrides` asserts subclass dispatch still reaches the override exactly once per API.

### `ServiceParam.toScalaAny`

`JsonEncodableParam.scala`:63 changes `l.asScala.toSeq.map(toScalaAny)` to `l.asScala.iterator.map(toScalaAny).toList`. On 2.12 the former returned a mutable `Buffer` that was also a live view over the Java list; the latter snapshots into an immutable `List`. This is a strict improvement for both caching eligibility and portability, since `List` satisfies `Seq`, `scala.collection.Seq`, and `immutable.Seq` alike. `VerifyJsonEncodableParam` now asserts the immutable type, that a post-conversion `javaList.add` is not observed, and that nested lists are deep-copied (`inner.set(0, ...)` plus `lists.clear()`). Note that `ServiceParam.toSeq` (:56) is unchanged and still yields a mutable buffer on 2.12; that only forfeits caching for any caller using it, and is fail-safe rather than incorrect.

### Resource safety in new tests

The fixed thread pool is released in `finally executor.shutdownNow()`; both temporary directories are removed in `finally FileUtils.deleteDirectory(...)`; entity streams are read through `StreamUtilities.using`.

### Documentation accuracy

Every claim in the new Markdown at `docs/Explore Algorithms/OpenAI/OpenAI.ipynb`:452-460 is supported by the source: lazy once-per-stage serialization, replacement on format change, non-persistence and per-executor rebuild, Python deep-copy into immutable JVM collections, mutable Scala sequences staying uncached, column-driven formats staying row-dependent, and Responses verbosity remaining row-varying. The sentence "The format is still included in every HTTP request; caching does not cache model responses or reduce the number of requests" correctly scopes the optimization. The routing table matches `ResponseFormatUtils.normalize`: `text`/`json_object` return a bare `Map("type" -> t)`, and `flattenToFlatJsonSchema` omits `strict` when the caller omitted it, which is exactly the "without implicitly enabling strict mode" claim asserted by `Legacy inner-schema configuration does not silently enable strict mode` and its Python counterpart.

## Notes, not findings

- The Python helper at `test_OpenAIResponseSchema.py`:48 now resolves the envelope key by fallback rather than by stage type. This is required by the new `text`/`json_object` selector test, whose payload has no envelope key, and it means that helper no longer discriminates a Chat envelope from a Responses one. That property is still covered, more strongly, at wire level by `OpenAIRequestBodySuite` and by `OpenAIResponseSchemaSuite`'s `assert(!payload.fields.contains("response_format"))`. No coverage hole results. `normalized` in `OpenAIResponseFormatCompatibilitySuite`:45-48 uses the same deliberate abstraction for a parity-focused suite.
- The `case _ => throw new IllegalArgumentException("JSON object keys must be strings")` arm at `OpenAIRequestBody.scala`:36-37 is unreachable for a `Map[String, Any]` payload and carries different message text than `AnyJsonFormat`'s `Cannot serialize ...`. Non-String keys in nested maps still produce the original message, since those subtrees go through `anyFormat`.

## Scope statement

This review covers the core serializer delta, the mutable-buffer regression guard, and the notebook case matrix and diagnostics, as read from the working tree. The final Scala case, codegen, and style run is reported as in progress and its outcome is not asserted here. No Databricks or Fabric runtime result is claimed; live CI for the new inference cases runs only after push, and Fabric is outside the fork pipeline.

## Driver precision notes

- The bounded cache creates one encoder while a selected reference remains current. Switching A to B and back to A creates another encoder, as the replacement test explicitly verifies.
- The previous Java-list conversion already called `map(toScalaAny)`, producing a converted buffer rather than retaining the original Java-list view. This change makes that copied result immutable; it is not the first introduction of copying.
- Required wire parity is parsed JSON equality plus preservation of supplied schema property ordering. No new contract promises byte-for-byte order of unrelated top-level request keys.
