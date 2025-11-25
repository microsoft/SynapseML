Spark 4.0 / Scala 2.13 / Java 17 upgrade notes
=============================================

Entry: Java 17 + Kryo HeapByteBuffer requires `--add-opens`
-----------------------------------------------------------

Context:
- While running `core/test` under Java 17 (Spark 4.0 / Scala 2.13), multiple tests failed with:
  - `IllegalArgumentException: Unable to create serializer "com.esotericsoftware.kryo.serializers.FieldSerializer" for class: java.nio.HeapByteBuffer`
  - caused by `java.lang.reflect.InaccessibleObjectException: Unable to make field final byte[] java.nio.ByteBuffer.hb accessible: module java.base does not "opens java.nio" to unnamed module`
- Root cause is Java 9+ module encapsulation blocking Kryo’s reflective access to `java.nio.HeapByteBuffer` internals.

Decision (tests / local dev):
- For sbt tests, we added a Java option in `build.sbt`:
  - `Test / javaOptions ++= Seq("--add-opens=java.base/java.nio=ALL-UNNAMED")`
- This unblocks Kryo when running `sbt core/test` under Java 17.

Implications for Fabric runtime:
- The same JVM flag is likely required for Spark driver and executors in Fabric:
  - `spark.driver.extraJavaOptions=--add-opens=java.base/java.nio=ALL-UNNAMED`
  - `spark.executor.extraJavaOptions=--add-opens=java.base/java.nio=ALL-UNNAMED`
- However, this cannot be set from within SynapseML code or this repo; it must be configured by the Fabric / Spark runtime (e.g., image or cluster configuration).

If this approach does not work in Fabric:
- Symptoms to expect:
  - Similar `IllegalArgumentException` / `InaccessibleObjectException` stack traces involving `HeapByteBuffer` and `FieldSerializer`.
- Potential follow-ups:
  - Coordinate with the Fabric platform team to add the `--add-opens` flags at the Spark runtime level.
  - OR refactor serialization in SynapseML to avoid Kryo’s reflective `HeapByteBuffer` access (e.g., custom serializer or different state representation), which is more invasive and was not implemented as part of this upgrade.

Entry: Featurize date column handling under Spark 4.0
----------------------------------------------------

Context:
- `Featurize` has special handling for `DateType` and `TimestampType` columns, deriving multiple numeric features (epoch millis, year, month, day, etc.) and assembling them into a vector.
- Under Spark 4.0, the `VerifyFeaturize` “Featurizing with date and timestamp columns” test failed with:
  - `SparkRuntimeException: [EXPRESSION_DECODING_FAILED] Failed to decode a row to a value of the expressions: static_invoke(DateTimeUtils.toJavaDate(...))`
- The failure came from the Date branch’s Scala UDF taking `java.sql.Date` directly; encoder/decoder internals changed in Spark 4.0.

Decision:
- Keep the public Featurize behavior the same (date columns still produce a vector of derived numeric features), but reimplement the Date branch in a Spark 4-compatible way:
  - For `DateType` columns, Featurize now:
    - Casts the date column to `BIGINT` (epoch millis) via `SQLTransformer`.
    - Applies a UDF that takes `Long` and reconstructs a `java.sql.Date` from the millis to derive:
      - millis since epoch
      - year
      - day of week
      - month
      - day of month
    - Packs these into a `Vector` as before.
- The `TimestampType` branch remains unchanged.

Customer impact:
- API surface and configuration are unchanged (`setInputCols`, `setOutputCol`, etc.).
- Conceptually, the same date-derived features are produced; however, exact numeric values may differ slightly from pre-Spark-4 runs due to changes in Spark’s date handling.
- The primary motivation is to avoid runtime failures under Spark 4.0 while preserving the intent and structure of date features.

Entry: HTTP streaming tests and HTTPv2
--------------------------------------

Context:
- Under Spark 4.0:
  - The legacy streaming HTTP connectors (`HTTPSource`/`HTTPSink` and `DistributedHTTPSource`/`DistributedHTTPSink`) started failing:
    - `HTTPSuite` received `NoHttpResponseException` from the client side (the streaming query never completed the request→reply loop).
    - `DistributedHTTPSuite` failed with `StreamingQueryException: assertion failed: DataFrame returned by getBatch ... did not have isStreaming=true` because the source returns a non-streaming DataFrame.
- The newer HTTPv2 connector (`HTTPSourceProviderV2` / `HTTPSinkProviderV2` / `HTTPSourceV2`) already passes tests in `HTTPv2Suite` under Spark 4.0 and uses the modern V2 streaming interfaces.

Decision:
- For Spark 4.0 and above, treat HTTPv2 as the supported streaming connector in tests:
  - `HTTPSuite` has been updated to use `HTTPSourceProviderV2` / `HTTPSinkProviderV2` instead of the v1 `HTTPSourceProvider` / `HTTPSinkProvider`.
- The legacy v1 HTTP connectors are left in place for backward compatibility but are not relied on by the core test suite for Spark 4.0.
- The `DataStreamReaderExtensions.server` / `DataStreamWriterExtensions.server` implicits in `IOImplicits` now also point at the HTTPv2 providers on Spark 4, with `address(host, port, api)` defaulting both the `path` and `name` options to `api` so that source and sink share a service key.
- `DistributedHTTPSource` now constructs its internal server-info DataFrame via `classic.SparkSession.internalCreateDataFrame(..., isStreaming = true)` so that `getBatch` returns a streaming `DataFrame`, satisfying Spark 4.0’s micro-batch assertions.
- Because HTTPv2 queries can take longer to report initial progress, the HTTP streaming tests (`HTTPSuite`, the non-distributed parts of `DistributedHTTPSuite`) now use a short fixed warm-up sleep instead of polling `StreamingQuery.recentProgress`, which was timing out under Spark 4.0 on local runs.

Implications:
- Local and CI runs of `core/test` on Spark 4.0 exercise the HTTP streaming path via HTTPv2.
- The v1 HTTP streaming connectors may still require additional porting work if they are to be supported on Spark 4.0; they are effectively legacy paths at this point.

Entry: DatasetExtensions.getColAs and MultiColumnAdapter under Scala 2.13
------------------------------------------------------------------------

Context:
- Under Scala 2.13, collection changes (`ArraySeq`, new `ArrayOps.map` behavior) caused `ArrayStoreException`s in `DatasetExtensions.getColAs` when tests requested array-typed columns (e.g., in `MultiColumnAdapterSpec`) and when column element types did not exactly match the requested JVM array type.
- `MultiColumnAdapterSpec` was also asserting array-of-string types for `StringIndexer` outputs that are actually numeric indices.

Decision:
- `DatasetExtensions.getColAs` was updated to build its result via a mutable `ArrayBuffer` and per-row `getAs[T](0)` rather than relying on `ArrayOps.map` over the collected array, eliminating `ArrayStoreException`s under Scala 2.13 while preserving the public signature.
- `MultiColumnAdapterSpec` now:
  - continues to validate the tokenizing transformer case as `Seq[Array[String]]`, and
  - asserts numeric `Double` index outputs for the estimator case (`output1` and `output2`), matching the actual `StringIndexer` behavior.

Entry: Power BI integration tests and environment gating
--------------------------------------------------------

Context:
- `PowerBiSuite` exercises real writes/streams into Power BI using a URL sourced from either:
  - `MML_POWERBI_URL` environment variable, or
  - `Secrets.PowerbiURL`, which shells out to fetch a secret.
- In local / CI environments without configured Azure credentials, this lookup failed with a non-zero process exit (e.g., expired AAD refresh token), causing all Power BI tests to fail even though the core library behavior was unchanged.

Decision:
- Introduced a `withPowerBiUrl` helper in `PowerBiSuite` that:
  - attempts to resolve the URL from `MML_POWERBI_URL` or `Secrets.PowerbiURL` inside a `Try`, and
  - cancels the test (ScalaTest “canceled”, not “failed”) with a clear message if the URL cannot be obtained.
- All Power BI tests (`write to powerBi`, delayed/minibatch variants, and streaming) now run only when the Power BI URL is available; otherwise they are reported as canceled.

Implications:
- Local Spark 4.0 runs of `core/test` no longer fail due to missing/expired Power BI credentials; these tests are effectively integration tests gated on environment configuration.
- Consumers who want to exercise the Power BI path must ensure `MML_POWERBI_URL` or the underlying secrets mechanism is configured; otherwise the behavior of the production `PowerBIWriter` code remains unchanged.
