---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 169
stage_attempt: 1
round: 1
timestamp: "2026-11-06T12:39:08Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "82c91d9290e2942b75aa8d16cf9d8afa1304f4d4f7896d28fb2c1ff969db8f13"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5 (reasoning: max)
- **Artifact**: reviews/pr-2811/task-unknown-attempt-169-stage-attempt-1-review-1-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Diff coverage, slice 1/24.** I read the assigned frozen diff (1,001 lines) with seven bounded views: 1–150, 151–300, 301–450, 451–600, 601–750, 751–900 and 901–1001. No range was truncated. All four files are new (`/dev/null` before-side), so the before-state is empty and every added line was reviewed:
  - `core/src/main/scala/com/microsoft/azure/synapse/ml/io/http/CancellableRequests.scala` (+254, diff lines 1–260)
  - `core/src/main/scala/org/apache/spark/injections/TaskContextUtils.scala` (+10, diff lines 261–276)
  - `core/src/test/scala/com/microsoft/azure/synapse/ml/io/split1/VerifyCancellableRequests.scala` (+682, diff lines 277–964)
  - `core/src/test/scala/org/apache/spark/TaskContextTestSupport.scala` (+31, diff lines 965–1001)
- [x] **Locking and concurrency in `CancellableRequests.scala`:**
  - `cancel()` and `observeCancellation()` use the scope monitor (L80–88, L139–164). `close()` uses a separate `cleanupLock` (L166). No path takes the two locks in opposite order, so they cannot deadlock.
  - The shared monitor never waits on a drain, and request aborts run on a dedicated thread (L156–160).
  - A request registered in `withRequest` (L55–57) is always either aborted or rejected. Registration is followed by a volatile check of `cancelled`, while `cancel()` sets `cancelled` before taking its request snapshot (L142–145). At least one side must see the other's write.
- [x] **Cancellation and timeout paths:**
  - Futures dropped by `shutdownNow` cannot hang `await`, because `checkTask()` runs on every poll of at most 100 ms (L121, L124–135).
  - A rejected submission rechecks cancellation before rethrowing (L112–115). An expired wait also rechecks before reporting a timeout (L125–127).
  - Task kills keep Spark's recorded reason and fall back to the generic message only when none exists (L93–94).
  - `close()` clears and restores the caller's interrupt flag (L170, L202). The abort wait and worker drain share one five-second budget (L172). After a failed abort wait, the abort pass is interrupted and the workers are still drained (L185–198).
  - `guarded` closes the scope only once, and cleanup failures are attached as suppressed exceptions (L214–220).
- [x] **Callers and inputs (contracts outside the slice):**
  - `HTTPClients.scala` L84–90 documents the close contract. L113–127, L329–353 and L612–637 show bounded retries checking cancellation and registering requests through `withRequest`.
  - `HTTPTransformer.scala` L110–118 rejects concurrency below 1 before a scope or thread pool is created. L125–155 contains the cancellable `mapPartitions` path.
  - `AIPrompt.scala` L113–115 and L543 send only the default bounded handler down the cancellable path.
  - `OpenAIAttachmentUtils.scala` L42–44 calls `TaskContextUtils` only with a context already checked for null.
- [x] **`TaskContextUtils` and the test helper.**
  - `TaskContextUtils` delegates to Spark's `private[spark] getKillReason()`. This follows the existing pattern in the `org.apache.spark.injections` package and in `HTTPSinkV2`.
  - `TaskContextTestSupport` calls the `TaskContextImpl` constructor with the correct arguments for Spark 3.5.0 (`build.sbt` L31). It passes `cpus` explicitly so no `SparkEnv` is needed.
- [x] **All 13 test cases traced.**
  - Submission-time kill: creating the worker thread triggers the kill. The pool is shut down while the thread is being added, the submission is rejected, and the recorded kill reason is reported. `addWorker` creates the thread outside `mainLock`, so this cannot deadlock.
  - The expired-wait race test uses stack-frame gating on `CancellableRequests$Scope.await`.
  - Both shared-monitor isolation tests depend only on the monitor being single-threaded, so they are deterministic.
  - Assertions are not vacuous, and the timing bounds (5 s drain against 8 s and 2 s limits) are consistent.
- [x] **Spec and build checks.** The behaviour matches `docs/Explore Algorithms/OpenAI/Unified AI prompting.md` L437–458 (timeout semantics, five-second cleanup budget, logged non-fatal abort errors) and L781–786 (reason preservation, `interruptOnCancel=false`, both rechecks). The build uses Scala 2.12.17 (`build.sbt` L34). `scala.util.Using` is already used in pre-existing main code (`OpenAIPrompt.scala`). `Test / parallelExecution := false` (L282).
- [x] **Considered and not raised:**
  - The worker-drain failure uses `require` (L197–198), so it surfaces as `IllegalArgumentException`, while the abort timeout throws `IllegalStateException`. No caller treats the type differently: the `IllegalArgumentException` catches in `AIPrompt` L140 and L442 run during driver-side plan building and per-row request preparation, not around the HTTP iterator.
  - Abandoned driver-side iterators that are never closed leave idle pool threads; the `HTTPClients.scala` L84 comment already documents that callers must close them.
  - Inherited thread-local values on the monitor thread are explicitly out of scope in the stated design.
- [ ] **Tests not executed.** This was a read-only review; no commands were run. The pass counts in the plan were not reproduced by this reviewer.
- [x] **Independence.** I did not open, list or search `reviews/`, sibling feedback, run directories or `.validation`. The only read outside the product worktree was the assigned frozen diff.