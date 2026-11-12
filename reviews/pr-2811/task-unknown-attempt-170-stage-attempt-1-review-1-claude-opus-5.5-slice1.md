---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 170
stage_attempt: 1
round: 1
timestamp: "2026-11-06T13:14:44Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "dba01f2e02b1f3a5cab351d86eb04801a957be44a6e55ff799acc51352ce2530"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2811/task-unknown-attempt-170-stage-attempt-1-review-1-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Slice identity.** Slice 1/24 (Cancellation lifecycle and regression coverage), source HEAD `5250eb2874612851f9f2b77dbc70dc558cd19b2a`, merge base `d3f3af6bd3e4b7d191f51c9eb29f9aeffe788193`, slice SHA-256 `f7ffba2e…`, 4 files.
- [x] **Full diff coverage.** I read all 1,001 lines of the assigned frozen diff with bounded `view` ranges: 1-150, 151-300, 301-450, 451-600, 601-750, 751-900 and 901-1001. I re-read the test portion (274-423, 424-573, 574-723, 724-873, 874-1001). No range was truncated or inaccessible.
- [x] **Before and after content.** All four files are new (`new file mode 100644`, `--- /dev/null`), so the before side is empty and the full after side is in the diff:
  - `core/src/main/scala/com/microsoft/azure/synapse/ml/io/http/CancellableRequests.scala` (+254)
  - `core/src/main/scala/org/apache/spark/injections/TaskContextUtils.scala` (+10)
  - `core/src/test/scala/com/microsoft/azure/synapse/ml/io/split1/VerifyCancellableRequests.scala` (+682)
  - `core/src/test/scala/org/apache/spark/TaskContextTestSupport.scala` (+31)

  The worktree copies of both production files match the diff exactly.
- [x] **Guidance read by exact path.** `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/synapseml-branches/references/branch-spark3p5.md`, `.github/skills/code-review/SKILL.md`, and `build.sbt` (Spark 3.5.0, Scala 2.12.17). I used only the `view` tool and did not read `reviews/`, `.validation`, session state or any other reviewer's output.
- [x] **Request registration versus cancellation, `CancellableRequests.scala`.** `withRequest` (53-60) registers the request before calling `check()`. `cancel()` (139-164) sets the volatile `cancelled` flag before taking a snapshot of `requests` (a `ConcurrentHashMap` key set). So every request is either rejected before it runs or included in the abort snapshot.
- [x] **`cancel()` and `close()`.**
  - `cancel()` runs once under the Scope lock and never blocks. It calls `shutdownNow`, takes the snapshot, and starts a daemon abort thread that uses the library classloader.
  - `close()` (166-205) runs under a separate `cleanupLock`, so the shared monitor never waits on a drain. The abort wait and the pool drain share one 5-second budget.
  - The first failure is kept and later ones are added as suppressed. The interrupt flag is cleared during the drain and restored afterwards.
- [x] **`submit`, `await` and `map`.**
  - `submit` (102-117) rechecks after a `RejectedExecutionException`. Because `cancelled` is set before `shutdownNow`, a rejection caused by shutdown always reports the recorded cancellation.
  - `await` (119-137) calls `checkTask()` before both the expired-timeout path and the `ExecutionException` path. An already-buffered result is still returned when the wait is zero.
  - `map` (208-253) handles an already-completed task context because the listener runs first and the `closed` check stops a leaked watch. Nested `guarded` calls drain only once.
- [x] **Kill reason.** `checkTask()` (90-100) throws `TaskKilledException(getKillReason.getOrElse(fallback))` only when `task.isInterrupted()`. `TaskContextUtils` (8-10) wraps Spark's `private[spark] getKillReason()` following the existing `org.apache.spark.injections` pattern. The thread-interrupt path is unchanged.
- [x] **Contracts in `core/src/main/scala/com/microsoft/azure/synapse/ml/io/http/HTTPClients.scala`.**
  - Lines 84-90: `sendCancellableRequestsWithContext` delegates to `CancellableRequests.map`.
  - Lines 113-128: `retryPause` calls `check()` on every pause.
  - Lines 320-336 and 449-454: retries check before and after each execute/buffer step, and an IOException goes to `keepTrying`.
  - Lines 614-636: `boundedWithFabricAuth` wraps all attempts in one `withRequest`, so the same request is never registered twice. It calls `check()` again before returning null after retries are exhausted.
- [x] **Timeout contract in `core/src/main/scala/com/microsoft/azure/synapse/ml/io/http/HTTPTransformer.scala`.** In lines 107-108 and 136-146, `concurrentWait` yields either a finite duration or `Duration.Inf`. NaN becomes 0, +∞ becomes a finite `Long.MaxValue` nanoseconds, and −∞ is rejected by `Duration.fromNanos`. So `await` never receives `MinusInf` or `Undefined`.
- [x] **Other contracts.** `CognitiveServicesBaseNoHandler.getInternalTransformer` in `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/CognitiveServiceBase.scala` routes through the same transformer path. I also checked `RequestWithContext` in `core/src/main/scala/com/microsoft/azure/synapse/ml/io/http/Clients.scala` and the `client`/`response` helpers in `core/src/test/scala/com/microsoft/azure/synapse/ml/io/split1/VerifyBoundedRequests.scala`.
- [x] **Test soundness.** I traced all 11 test definitions (13 cases, counting the two `Seq(false, true)` parameterizations) against:
  - JDK 8/17 `ThreadPoolExecutor.addWorker`, which builds the worker thread before taking `mainLock`. This makes the `InheritableThreadLocal.childValue` hook valid: `shutdownNow` runs, then the rejection, then `TaskKilledException`.
  - `FutureTask` and `ScheduledFutureTask` semantics.
  - The Spark 3.5 `TaskContextImpl` constructor, which takes 11 arguments plus a default `resources`.
  - `markInterrupted`, `markTaskCompleted` and `setTaskContext`, which are `private[spark]`. That is why the test helper lives in the `org.apache.spark` test package.

  Every test releases its latches and semaphores in `finally` and joins its threads with bounded waits.
- [x] **Security and compatibility.**
  - The abort-failure log (line 151) records only the exception class's simple name, never URIs, headers or credentials.
  - The slice adds no RDD usage and changes no existing public signature.
  - The new surfaces are the `private[http]` `CancellableRequests`, a public `TaskContextUtils` injection consistent with existing injections, and test-only support.
- [x] **Concerns checked and not reported, because they are not concrete defects:**
  - The first test's monitor-classloader check only proves the fix when that test is the first in the JVM to create the shared monitor. Production sets the monitor's classloader explicitly either way.
  - Inheritable thread-locals and the classloader pinned by the long-lived monitor; the PR explicitly disclaims clearing inheritable thread-locals.
  - The abort/`setCancellable` window inside Apache HttpClient, which the library re-checks itself before sending.
  - Non-finite timeouts, which production callers cannot pass.
- [ ] **Tests not executed.** This was a read-only review, so I ran no build or test commands. I took the PR's validation evidence into account but did not reproduce it.