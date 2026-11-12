---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 167
stage_attempt: 1
round: 1
timestamp: "2026-11-06T11:10:27Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "66e57cc13e47ca37afb67ac2efaf3ddf1f0b301ae1ad2b978cd71ef675deb5d1"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2811/task-unknown-attempt-167-stage-attempt-1-review-1-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Diff coverage:** I read slice 1/24 of the assigned frozen diff (881 lines) in six bounded views: lines 1–150, 151–300, 301–450, 451–600, 601–750 and 751–881. No view was truncated.
- [x] **Assigned files:** All four files are `new file mode 100644`, so the before side is `/dev/null` (empty). I read every added line on the after side:
  - `core/src/main/scala/com/microsoft/azure/synapse/ml/io/http/CancellableRequests.scala` (+248)
  - `core/src/main/scala/org/apache/spark/injections/TaskContextUtils.scala` (+10)
  - `core/src/test/scala/com/microsoft/azure/synapse/ml/io/split1/VerifyCancellableRequests.scala` (+568)
  - `core/src/test/scala/org/apache/spark/TaskContextTestSupport.scala` (+31)
  
  The diff has 857 added lines plus 4 × 6 header lines, which equals 881.
- [x] **Worktree check:** The current `CancellableRequests.scala` matches the diff, where the file line number is the diff line number minus 6. I spot-checked lines 60–100 and 155–248. The grep hits at lines 31, 94 and 153 also line up.
- [x] **Callers and dependencies checked:**
  - `HTTPClients.scala`:
    - 84–90: `sendCancellableRequestsWithContext` calls `CancellableRequests.map`.
    - 112–127: bounded `retryPause` calls `check()`.
    - 329 and 335: bounded checks.
    - 607–637: `bounded` and `boundedWithFabricAuth` use `withRequest`; an `IOException` leads to `check()`.
  - `HTTPTransformer.scala` 125–153: the only caller, inside executor `mapPartitions`.
  - `AIPrompt.scala` 113–123 and 543: the default bounded handler is what routes to `transformCancellable`.
  - `AIRequestAdapter.scala` 228–232: requires concurrency > 0 and a finite `concurrentTimeout` of at least 1e-9, so `await` never gets a non-finite or negative-infinite duration.
  - `OpenAIAttachmentUtils.scala` 42–44: checks for a null `TaskContext` before calling `getKillReason`.
  - `HTTPSchema.scala` 165–188: `HTTPRequestData` is not final, `toHTTPCore` can be overridden, and the `this(HttpRequestBase)` constructor exists.
  - `VerifyBoundedRequests.scala` 34–51: the `private[split1]` `client` and `response` helpers.
- [x] **Concurrency and lifecycle:**
  - **Registration vs. abort race:** A request is added to the set before `check()`, and `cancelled` is set before the abort snapshot is taken. A request registered after the snapshot therefore sees the cancellation and throws.
  - **Lock order:** `close` holds `cleanupLock` and then briefly takes the scope lock. The monitor takes only the scope lock, and aborts run on their own thread, so there is no inversion.
  - **Monitor watch:** `observeCancellation` respects `closed`; `cancel` cancels the watch, and remove-on-cancel drops it from the queue.
  - **`close()`:** It clears and later restores the thread's interrupt flag. The abort wait and worker drain share one 5-second budget.
  - **Suppressed exceptions:** Nested `guarded` calls add exactly one suppressed cleanup error, and an exception is never suppressed into itself.
  - **`await` timing:** It always makes one final zero-wait poll before reporting a timeout.
  - **Unstarted work:** Futures dropped by `shutdownNow` never complete, but `checkTask()` throws before anyone waits on them.
- [x] **Behavior matches the stated scope:**
  - The task-kill reason is kept, with `"HTTP request task canceled"` used only when no reason is recorded (line 94).
  - The monitor thread (line 31) and abort threads (line 153) use the library classloader. Request workers still inherit the caller's classloader.
  - `checkTask()` still checks thread interruption first, so that behavior is unchanged.
- [x] **Version compatibility:**
  - `build.sbt` uses Scala 2.12.17 and Spark 3.5.0. `scala.util.Using` already appears in existing code (`FabricCertificateBasedAuthenticator.scala:31`), so a backport is on the classpath.
  - `build.sbt:282` sets `Test / parallelExecution := false`, so the test hooks on the JVM-wide monitor cannot affect other suites.
  - The 11-argument `TaskContextImpl` constructor passes `cpus` explicitly, avoiding any need for `SparkEnv`.
  - `getKillReason` is already used at `HTTPSinkV2.scala:142`.
- [x] **Tests:**
  - There are 11 cases (1+1+1+2+1+2+1+1+1), which matches the claimed 11/11.
  - Timing margins hold: the drain is at most 5 s against 8 s assertions, and the `eventually` windows of 2 s and 5 s sit inside the drain budget.
  - Every blocked thread is released in a `finally` block, and threads are joined before their liveness is checked.
  - The tests are not vacuous: the abort- and monitor-classloader assertions would fail without the explicit `setContextClassLoader`, and the second-scope cancellation would miss its 2 s limit if the monitor did the abort or drain itself.
- [x] **Security:** The abort-failure log records only the exception's simple class name. Timeout and cancellation messages contain no URLs, headers or credentials, and nothing secret was added.
- [x] **Concerns considered and rejected:**
  - A `RejectedExecutionException` race between `checkTask` and `submit`: Spark still reports the task as killed using its recorded reason.
  - `require` raising `IllegalArgumentException` for the drain failure: this only affects the message wording.
  - Aborts running one at a time: HttpClient forces an immediate (RST) close on abort, so a single abort does not block for long.
  - The monitor thread never exits: this would only matter with per-session classloaders, which SynapseML's py4j-based deployments don't use.
- [ ] **Not run:** I executed no builds or tests in this read-only review. I did not re-run the supplied Java 8 and port compile/test/style evidence.