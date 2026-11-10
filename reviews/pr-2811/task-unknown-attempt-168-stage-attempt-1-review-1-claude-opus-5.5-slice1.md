---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 168
stage_attempt: 1
round: 1
timestamp: "2026-11-06T12:02:39Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "6d6b45048e764405770f769d8e031301b4fb20f3e49eb8b909ad280c2af3769d"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2811/task-unknown-attempt-168-stage-attempt-1-review-1-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Coverage of slice 1/24 (Cancellation lifecycle and regression coverage).** I read the assigned frozen diff in seven bounded views: lines 1–150, 151–300, 301–450, 451–600, 601–750, 751–900 and 901–938. Every line from 1 to 938 came back without truncation. The total matches the file contents: 4 files × 6 header lines, plus 252 + 10 + 621 + 31 added lines, equals 938.
- [x] **Before and after content.** All four assigned files are new (`new file mode 100644`, `--- /dev/null`, hunks of the form `@@ -0,0 +1,N @@`). There is no "before" version, and I reviewed each file's complete "after" content:
  - `core/src/main/scala/com/microsoft/azure/synapse/ml/io/http/CancellableRequests.scala` (252 lines)
  - `core/src/main/scala/org/apache/spark/injections/TaskContextUtils.scala` (10 lines)
  - `core/src/test/scala/com/microsoft/azure/synapse/ml/io/split1/VerifyCancellableRequests.scala` (621 lines)
  - `core/src/test/scala/org/apache/spark/TaskContextTestSupport.scala` (31 lines)
- [x] **Contracts with other files (read-only).**
  - `core/src/main/scala/com/microsoft/azure/synapse/ml/io/http/HTTPClients.scala`:
    - lines 62–90: the cancellable client and `sendCancellableRequestsWithContext`, including the comment that callers outside a Spark task must close the iterator.
    - line 120: the cancellation check in `retryPause`.
    - lines 329, 335 and 353: the checks in `sendWithRetryPolicy`.
    - lines 618, 624 and 630: `boundedWithFabricAuth`, which calls `withRequest` and `check`.
  - `HTTPTransformer.scala` lines 107–152: `transformCancellable` and `concurrentWait`.
  - `Clients.scala`: `RequestWithContext` and the ordering used by the older `AsyncClient`.
  - `VerifyBoundedRequests.scala` lines 34–51: the `client` and `response` test fixtures.
  - `cognitive/.../openai/OpenAIAttachmentUtils.scala` lines 37–48: this caller of `TaskContextUtils` only runs when `TaskContext` is non-null.
  - `build.sbt`: Spark 3.5.0, Scala 2.12.17, `Test / parallelExecution := false`.
- [x] **Concurrency and lifecycle in `CancellableRequests.scala`.**
  - **Lock ordering:** `observeCancellation` (lines 80–88) and `cancel` (137–162) take the scope lock. `close` (164–203) takes `cleanupLock` and holds the scope lock only briefly inside `cancel`. The bounded wait for workers to stop never holds the scope lock, so one scope that is shutting down cannot stall the shared monitor for other scopes.
  - **Listener vs. watch race:** if the task-completion listener runs immediately on an already-completed task, the `closed` flag prevents the watch from being scheduled (lines 209–210).
  - **Request registration vs. abort snapshot:** `withRequest` (53–60) registers the request and then checks for cancellation. `cancel` sets `cancelled` before calling `shutdownNow` and taking the snapshot (140–143). Under the memory model, every in-flight request is either in the snapshot or fails its own check.
  - **Queued work that never starts:** the `checkTask` poll every 100 ms in `await` (119–135) stops the wait on futures that `shutdownNow` drained.
  - **Bounded cleanup:** abort and worker shutdown share one 5-second budget.
  - **Interrupt flag:** `close` clears the caller's interrupt flag at line 168 and restores it at line 200.
- [x] **Kill reason and rejected submissions.**
  - `checkTask` (90–100) checks in this order:
    - a thread interrupt raises `InterruptedException`, which Spark maps back to the recorded kill reason;
    - a task kill raises `TaskKilledException` carrying `getKillReason`, with the generic message only as a fallback;
    - a scope cancellation raises `CancellationException`.
  - `await` repeats this check before rethrowing the cause of an `ExecutionException` (127–129).
  - The `RejectedExecutionException` path in `submit` (102–117) is the race the latest change targets. I traced it through `ThreadPoolExecutor.execute` and `addWorker`. Worker construction happens before `mainLock` is taken, so a concurrent `shutdownNow` leads to `addWorkerFailed` and then `reject`. The path then rechecks cancellation and otherwise rethrows the original rejection, as the spec requires.
- [x] **Regression tests.** I traced all 12 tests (10 `test` blocks, two of them parameterized ×2) against the production code. The count matches the supplied 12/12 claim.
  - **Submission race (lines 78–129):** `InheritableThreadLocal.childValue` runs inside `Executors.defaultThreadFactory().newThread`. Because the monitor is single-threaded, the observer watch is guaranteed to run after `cancel()` finishes.
  - **Abandoned iterator (lines 150–200):** a fixed pool always adds a new worker while below its core size, so `workers.size() == 2` is deterministic. The bounded join at line 186 runs before the liveness assertions.
  - **Uncooperative workers:** a cleanup failure surfaces through the message of `TaskCompletionListenerException`.
  - **Thread cleanup:** every worker, abort and consumer thread is joined or shut down in a `finally` block.
- [x] **Spark API use in the test support and accessor.**
  - `TaskContextTestSupport.scala` uses the 11-argument Spark 3.5 `TaskContextImpl` constructor and passes `cpus = 1` explicitly, so `SparkEnv` is not needed. It also uses `markInterrupted`, `markTaskCompleted` and `setTaskContext`; these are Spark-internal and are only called from code in the `org.apache.spark` package.
  - `TaskContextUtils.scala` line 9 follows the existing injection objects (`UDFUtils`, `SConf`, `RegressionUtils`, `BlockManagerUtils`). `HTTPSinkV2.scala` already calls `getKillReason()` in the same way.
- [x] **Security.** Abort failures log only the exception's class name (no URL, headers or body). All threads are daemon threads. The change adds no credentials, endpoints or RDD use. The only new public JVM API is the `TaskContextUtils` accessor.
- [x] **Considered and dismissed, with no reproducible defect:**
  - **(a) Errors from workers skip cleanup in `guarded`:** throwables that are not `Exception`s bypass it, but inside a task the completion listener still closes the scope, and the driver close requirement is documented in `HTTPClients.scala`.
  - **(b) Mismatched exception types:** a failed worker shutdown raises `IllegalArgumentException` (from `require`), while a failed abort raises `IllegalStateException`. The former only appears suppressed or wrapped on cleanup paths.
  - **(c) Permanent monitor thread:** the shared monitor is created lazily and lives as a permanent daemon thread on the library classloader. This is the intended design, and it does not retain the task classloader.
  - **(d) Overflow in `await`:** the arithmetic only overflows for timeouts near −2^63 ns, which the validated callers cannot pass.
- [ ] **Not applicable — no execution evidence from this review.** Under the read-only contract I ran no build, test or scalastyle command. I cross-checked the supplied Java 8 and Spark port results against the source (for example, that 12 tests exist) but did not rerun them.
- [x] **Independence.** I did not access `reviews/`, session state or other reviewers' output. All searches were limited to `core/src` and `cognitive/src`, plus targeted reads of `build.sbt`. The only reads outside the worktree were bounded views of the assigned frozen diff.