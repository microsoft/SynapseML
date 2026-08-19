## Why

Azure Pipelines intermittently loses a matrix job during `Setup repo` before tests run:

```
[error] sbt.librarymanagement.ResolveException: unresolved dependency:
        com.globalmentor#hadoop-bare-naked-local-fs;0.1.0: not found
```

Build **231667649** showed 39 of 40 UnitTests shards resolving the module offline from the byte-identical restored cache while one shard failed ten consecutive times. Exact-head build **231783740** then captured the missing evidence:

- the local Ivy entry contained the POM metadata and the 9,106-byte JAR;
- the Maven Central probe returned **HTTP 429** from `repo1.maven.org`;
- retrying the same hostname for ten attempts still failed.

The failure is therefore a combination of unusable restored state and host-specific Maven Central throttling, not a missing artifact.

## What changes

1. **Safe cache recovery**
   - Streams sbt output through `tee` while preserving sbt's exit status via `PIPESTATUS[0]`.
   - Parses only complete, path-safe `org#name;revision` coordinates.
   - Evicts the named Ivy module and exact Coursier revision under both official Maven Central host paths before retrying; unrelated failures evict nothing.
   - Requires absolute, non-root cache paths without lexical traversal. Without `HOME` or a safe override, eviction is disabled.
   - Logs the existing entry and uses option-safe `rm -rf --`.
2. **Attributed, bounded diagnostics**
   - Probes `repo1.maven.org` and `repo.maven.apache.org` at most once each per coordinate per wrapper invocation.
   - Caps each host probe at 15 seconds, preserving a 30-second total diagnostic bound per coordinate.
   - Probe failure never changes the sbt result.
3. **Durable HTTP 429 recovery**
   - Keeps Maven Central's canonical `https://repo.maven.apache.org/maven2` endpoint after sbt's default `repo1` resolver.
   - Ivy falls through to the same official artifact repository when the first hostname returns 429.
4. **Portable failure handling**
   - Uses a GNU/BSD-compatible `mktemp` template and exits explicitly if the attempt log cannot be created.

No pipeline topology, dependency version, Spark runtime code, or published artifact changes.

## Behavior and performance contract

- **Cache hit / successful setup:** no eviction or probe. The fallback resolver is not contacted after a successful earlier resolution.
- **Unrelated failure:** existing retry/backoff behavior is unchanged.
- **Unresolved dependency:** deletion is bounded to one validated module under validated cache roots; diagnostics are bounded to two requests and 30 seconds per coordinate.
- **Exhaustion:** remains a visible non-zero failure; no success-shaped fallback.

Successful-path benchmark (40 alternating runs):

| Workload | Before median | After median | Delta |
|---|---:|---:|---:|
| 20 ms quiet command | 41.80 ms | 41.90 ms | **+0.10 ms** |
| 500 log lines | 22.41 ms | 22.69 ms | **+0.28 ms** |

## Validation

- `bash -n tools/ci/sbt_retry.sh`
- **101/101** `tools/ci/tests` passed; **28/28** wrapper tests passed.
- The original five safety regressions fail on their prior implementation for the intended reasons.
- Three dual-host regressions fail on rebased pre-fix head `e78c9fd2d5`: fallback-host Coursier state survives eviction and only `repo1` is probed.
- The SIGPIPE regression fails on prior head `268579b7b6`: truncating a successful `find` listing spuriously invokes the `ls` fallback under `pipefail`.
- Pinned `black==22.3.0`: **194 files unchanged**.
- JDK 11 `scalastyle test:scalastyle`: every main/test module, **0 errors**.
- Safe real-path exercise: the probe returned HTTP 200 and the retry recovered on attempt 2.
- Synthetic Ivy test: a local resolver returned only HTTP 429; Ivy fell through to `repo.maven.apache.org` and downloaded every dependency, including `hadoop-bare-naked-local-fs`, successfully.
- Resolver chain verified in this project: `local` -> `public (repo1)` -> `Maven Central fallback`.
- Exact-head Azure build **231820780** completed for commit `19171a7ee840250d6a896fdfaaaf2119e36f3192`: **65/68 jobs succeeded**.
- Exact-head rerun **231832869** reproduced each of the three external failures below; its redundant remainder was canceled after reproduction because build 231820780 had already completed every other job successfully.

## Current CI blockers

- **Fabric E2E:** certificate authentication returns `AADSTS9002313`; the suite aborts with **0 tests run**. The latest `master` build **231812737** failed identically.
- **Internal compatibility (Python):** the internal environment's vendored `openai` imports unavailable `aiohttp.SocketTimeoutError`; both attempts fail before test results are produced. Prior exact-head build **231789810** has the same failure.
- **Internal compatibility (Scala):** **137 tests pass**, then the `ebm` and `predict` suites abort while acquiring the same certificate token (`AADSTS9002313`). The rerun reproduced both aborts.

All three failures are outside the five files changed by this PR. The sbt prewarm, style, Spark 4.1 compatibility, publish, Docker, Databricks CPU/GPU, Fabric-independent Python/R tests, website samples, and every OSS unit-test shard succeeded in build 231820780.

## Follow-up

After merge, port the same helper and resolver fallback to `spark4.1` and `spark4.0`.
