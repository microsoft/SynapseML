# Review 6 — final polish and hardening (Java 8 codegen-test repair)

- Task `task-java8-codegen-20260917`, attempt 1, round 6, model `claude-opus-5`
- Round scope: clarity, change scope, error reporting, cleanup, doc concision,
  claim accuracy. Static review plus re-reading the driver's completed log; no
  build, test, network, agent, code fix, or commit in this turn.
- Surface: working-tree delta vs `HEAD` (`1d12c2b8`) is exactly two files —
  `core/src/test/scala/com/microsoft/azure/synapse/ml/codegen/CodegenDiscoverySuite.scala`
  (+3/-1) and
  `.github/skills/synapseml-branches/references/branch-spark3p5.md` (+11).
  The branch's wider PR surface is out of this round's scope.

**Verdict: APPROVE. No blocking defect. Three optional nits (N1-N3), one
prior-artifact correction (N4). Merge remains gated on G7-G9 below.**

## Gates

| Gate | State | Evidence |
| --- | --- | --- |
| G1 Java 8 API compatibility | PASS | `git grep getPlatformClassLoader` over tracked sources returns nothing in the working tree; the only remaining hits are prose in `branch-spark3p5.md:38` and `reviews/`. |
| G2 Temurin 8u504 local gates | PASS | `master-java8-final-gates.log`: `openjdk version "1.8.0_504"` / `Temurin ... 8u504-b01` (l.6-7), sbt 1.10.11 on that JDK (l.11), `compile` 517 s (l.221), `Test/compile` 295 s (l.254), 3 suites / 28 tests / 0 failed (l.296-299), `scalastyle` 14 s (l.349), `Test/scalastyle` 12 s (l.399), `codegen` 172 s (l.1406). Zero `[error]` lines. |
| G3 Gate set matches the claim | PASS | Recorded invocation (l.9) is `compile Test/compile core/testOnly <CodegenDiscoverySuite, PyCodegenSuite, VerifyJarLoadingUtils> scalastyle Test/scalastyle codegen` — unscoped tasks aggregate all modules; style covered 364 main and 331 test files across 7 modules; codegen output reaches `services.vision` wrappers, so it is not core-only. |
| G4 Scalastyle width | PASS | New lines are 107, 85, and 101 chars against `maxLineLength` 120. |
| G5 Scope discipline | PASS | No reflection, no `-D`/JVM flag, no pipeline, template, `build.sbt`, or `AGENTS.md` edit; no custom-classloader support added or implied. Fix is one expression plus one invariant. |
| G6 Error reporting | PASS | `CodegenDiscoverySuite.scala:30` fails loudly in the launcher; `redirectErrorStream(true)` (l.229) folds it into `probe.log` and `assert(process.exitValue() == 0, text)` (l.240) attaches the whole log, so the message reaches the suite failure unwrapped. |
| G7 Fix is committed | **OPEN — blocking** | `HEAD:core/.../CodegenDiscoverySuite.scala:23` still reads `ClassLoader.getPlatformClassLoader`. The repair is working-tree only, so CI 236308415 stays red until it is committed and pushed. Commit both files before requesting validation. |
| G8 Remaining runtimes | OPEN | JDK 11 targeted 28 (driver running), JDK 17 port merges plus their validation, and full current-head Azure CI are all pending, as the task states. |
| G9 Build-log attribution | OPEN | `templates/java_setup.yml` does pin JDK 11 (`versionSpec: '11'`) and `pipeline.yaml` includes it at exactly one place (l.1509, commented at l.1499), so the doc's structural claim is verified in-tree. The narrower claim that setup and publication jobs of build 236308415 ran the agent's Java 8 default is not verifiable offline; confirm from that build's SBT startup lines before treating it as settled. |

## Cleanup

- Suite cleanup is correct and unchanged: `loader.close()` runs after context
  restoration (`CodegenDiscoverySuite.scala:33-34`), and the temp tree is removed
  by `FileUtils.deleteDirectory(root)` in `finally` (l.243) after the child is
  reaped, so Windows handle-locking is not reintroduced.
- Working tree carries no stray artifacts: only the two modified files and the
  round 1-5 reports are untracked. `reviews/` is tracked on `upstream/master`
  (53 files), so adding this report follows existing convention rather than
  polluting the PR.

## Nits (non-blocking, no change requested)

- **N1 — diagnostic text.** `require(probe.getClassLoader eq loader, ...)` names
  the violated invariant but not the loader actually observed. Interpolating
  `${probe.getClassLoader}` would push the line past the 120-char limit and force
  a wrap, and `probe.log` already carries the surrounding context, so leaving it
  is reasonable. Recording the trade-off, not asking for the edit.
- **N2 — why no null guard is needed.** If a future JDK ever parents the
  application loader directly to bootstrap, `getParent` returns `null` and
  `new URLClassLoader(urls, null)` delegates to bootstrap: isolation gets
  stricter, not weaker, and the new `require` still holds. A defensive branch
  would be speculative; none should be added.
- **N3 — doc concision.** `branch-spark3p5.md:37` is 84 chars where the file
  otherwise wraps near 79 (only one other line, the pre-existing l.42, exceeds
  it), and the new bullet runs nine lines carrying three rules: per-job JDK,
  read each job's SBT startup log, avoid the Java 9-only API. Rewrapping l.37 and
  splitting the bullet in two would improve scannability. Content is accurate as
  written; the durable rule survives even after build 236308415 ages out.
- **N4 — prior-artifact correction.** Round 5 reports "322 files" for aggregate
  `scalastyle`; the log's per-module counts (0, 7, 55, 3, 43, 214, 42) total 364.
  Its 331 test-file figure is right. It also states the run "confirm[ed]"
  `PUBLISHED_TEST_JAR_CODEGEN_OK`; that marker is asserted inside the suite and
  printed to the child's `probe.log`, not to the sbt log, so a green suite
  implies it rather than showing it. Neither point affects the verdict.

## Evidence limits

1. Static review plus log reading only — no compile, test, scalastyle, codegen,
   or network call was made this round. Round 6 does not establish completion.
2. Java 8 `AppClassLoader -> ExtClassLoader` and Java 9+
   `AppClassLoader -> PlatformClassLoader` parentage is taken from documented JDK
   structure; the 8u504 leg in G2 is the confirming run.
3. Build 236308415 job logs were not fetched (no network), hence G9.
4. Findings were derived independently; rounds 1-5 were consulted only to check
   claim accuracy (N4) and to confirm M1 landed in the doc.

## Driver disposition

- N1/N2 remain explicit trade-offs. The invariant error reaches the failure
  log, and no speculative null-parent or custom-loader branch is needed.
- N3 fixed by splitting the per-job JDK and portable-helper guidance.
- N4 corrected in the round 5 report's appended driver note without erasing
  the original review.
- G9 is verified by retained build 236308415 logs 948 and 1566. Their SBT
  startup identifies Temurin Java 1.8.0_504 in publication and setup; the
  existing Internal compatibility template selects JDK 11. No pipeline
  configuration was changed.
- Publication, JDK 11/17 validation, and final-head CI remain separate gates.

The subsequent JDK 11 run also passed all 28 targeted tests, with zero
failures, canceled, ignored, or pending cases. The shared branch reference
now records two documentation-only lessons from the final audit: historical
review reports retain dispositions rather than acting as a current gate
ledger, and a minimal Python environment cannot prove compatibility with
optional dependencies installed by CI. The driver checked these additions
against the observed review and environment failures; no private source,
resource identifiers, or credentials appear in the public guide.
