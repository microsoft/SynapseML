# Review 3 — edge cases and robustness (Java 8 codegen-test repair)

- Task: `task-java8-codegen-20260917`, attempt 1, round 3
- Reviewer model: `claude-opus-5`
- Scope of this round: edge-case and robustness only (parent delegation across
  Java generations, class/resource identity, exception and resource/context
  restoration, documentation accuracy of the merge-order gates)
- Worktree: `.worktrees/branch-context-20260916`, branch
  `docs/spark4-branch-context-20260916`
- Tracked change surface reviewed (only these two files are modified):
  - `core/src/test/scala/com/microsoft/azure/synapse/ml/codegen/CodegenDiscoverySuite.scala` (+3/-1)
  - `.github/skills/synapseml-branches/references/branch-spark3p5.md` (+11)

**Verdict: approve the code change; one minor documentation correction (M1)
and two document-only limitations (L1, L2). No blocking defect found.**

---

## 1. Parent delegation across Java generations — correct on both

`CodegenDiscoverySuite.scala:24` now reads:

```scala
val loader = new URLClassLoader(urls, ClassLoader.getSystemClassLoader.getParent)
```

- **Java 9+**: `getSystemClassLoader()` is `ClassLoaders$AppClassLoader`, whose
  parent is `ClassLoaders$PlatformClassLoader` — the same object
  `ClassLoader.getPlatformClassLoader()` returned. The new expression is a
  literal identity with the removed API on every JDK where that API existed, so
  the previously passing local JDK 11 behavior is unchanged.
- **Java 8**: `getSystemClassLoader()` is `Launcher$AppClassLoader`, whose
  parent is `Launcher$ExtClassLoader` (itself parented to the bootstrap
  loader). This is the structural analogue of the platform loader.

The isolation property the design depends on holds on both: the chosen parent
never exposes `java.class.path`, so every application class resolves from
`loader`'s own URLs, which `CodegenDiscoveryLauncher` builds from
`java.class.path` at `CodegenDiscoverySuite.scala:21`. The added comment at
`CodegenDiscoverySuite.scala:23` states this accurately.

`urls` is not narrower than the app loader's view: `URLClassPath` honors jar
manifest `Class-Path` entries on both generations, and no module path is passed
(`CodegenDiscoverySuite.scala:226-229` passes only `-cp`).

## 2. The new assertion closes a real blind spot — this is not a cosmetic guard

`CodegenDiscoverySuite.scala:30`:

```scala
require(probe.getClassLoader eq loader, "Probe must load through the isolated URL classloader")
```

The comment at `CodegenDiscoverySuite.scala:22` records that Spark 3's shaded
Guava cannot scan the JDK application loader; that Guava entry point is
`ClassPath.from(getClass.getClassLoader)` at
`core/src/main/scala/com/microsoft/azure/synapse/ml/core/utils/JarLoadingUtils.scala:100`
(import at line 7).

Before this change, nothing verified the premise. If the parent had resolved to
the application loader, parent-first delegation would have loaded the probe
there, and **every pre-existing probe assertion would still have passed**: the
application loader also has the test JAR on `-cp`, so the fixture resource check
at `CodegenDiscoverySuite.scala:45-48` would still see `protocol == "jar"` and
still match `args(1)`. The suite would have gone green while silently exercising
the non-`URLClassLoader` path that `getAllClasses` is documented not to support.
The new `require` is what converts that into a failure.

Escape paths the guard actually catches:

- `-Djava.system.class.loader=X` → `getSystemClassLoader` is the custom loader
  and `getParent` is the built-in application loader → parent-first resolves
  the probe there → `ne loader` → fails loudly.
- probe classes on `-Xbootclasspath/a:` → `getClassLoader` returns `null` →
  `eq` is `false` → fails loudly.

No false-positive path exists: `com.microsoft.azure.synapse.ml.codegen.CodegenDiscoveryProbe`
cannot be visible to the extension or platform loader in a stock JDK image.
`eq` is correct here — `ClassLoader` does not override `equals`, and the `null`
case is handled by `eq` returning `false` rather than throwing.

The stated launcher assumption is verified in-tree: a grep over the worktree
finds no `java.system.class.loader` anywhere, and the only remaining
`getPlatformClassLoader` occurrences are prose in
`.github/skills/synapseml-branches/references/branch-spark3p5.md:36` and the two
prior review files.

## 3. Class and resource identity — no split identity

Because the probe is proven to come from `loader`, parent-first delegation
means `JarLoadingUtils$`, `PyCodegen`, `Wrappable`, and `PyCodegenFixtures` all
resolve there too (the extension/platform parent misses each of them).
`JarLoadingUtils.getAllClasses` resolves names with `Class.forName(cn)` at
`JarLoadingUtils.scala:106`, whose caller loader is that same `loader`, so the
`Class` objects compared inside the probe —
`testClasses.contains(classOf[PyCodegenFixtures.TypedPythonEstimator])` and the
`allClasses` checks — are drawn from one loader. Identity comparisons are sound
on both generations.

The launcher deliberately shares no application type with the probe: it crosses
the boundary only by reflection. `probe.getMethod("main", classOf[Array[String]])`
resolves because `String[]` is a bootstrap type and therefore identical in both
loaders — which requires the parent chain to reach the bootstrap loader, and it
does on Java 8 (ext → bootstrap) and Java 9+ (platform → bootstrap).

Resource identity is unaffected: the fixture lookup at
`CodegenDiscoverySuite.scala:45` delegates parent-first, misses at
ext/platform and bootstrap, and resolves from the test JAR in `loader`'s URLs,
yielding the `jar:` protocol the assertion requires.

## 4. Exceptions, resource release, and context restoration

`CodegenDiscoverySuite.scala:32-35`:

```scala
} finally {
  thread.setContextClassLoader(original)
  loader.close()
}
```

- Ordering is correct. Context restoration runs **before** `loader.close()`, so
  an `IOException` from closing the jar handles cannot skip it. Restoration
  matters because the discovery fallback reads the context loader at
  `JarLoadingUtils.scala:127`.
- `original` is captured at `CodegenDiscoverySuite.scala:26`, before the `try`,
  so a failure inside `setContextClassLoader(loader)` still restores.
- Residual, both benign: a throwing `loader.close()` would mask a primary probe
  failure, and neither the launcher nor the probe unwraps the
  `InvocationTargetException` produced by the reflective `main` invocation at
  `CodegenDiscoverySuite.scala:31`. Diagnosis survives anyway — the JVM prints
  the full `Caused by:` chain, `redirectErrorStream(true)`
  (`CodegenDiscoverySuite.scala:229`) folds it into `probe.log`, and
  `assert(process.exitValue() == 0, text)`
  (`CodegenDiscoverySuite.scala:240`) attaches the whole log to the failure.
  The new `require` at line 30 throws directly in the launcher and is therefore
  reported unwrapped. No change recommended.
- The subprocess holds the jar handles, and the parent only deletes the temp
  tree after `waitFor` / `destroyForcibly` + `waitFor`
  (`CodegenDiscoverySuite.scala:231-236`), so the Windows delete path is not
  affected by the loader lifetime. `completed` is asserted before
  `exitValue()`, so `IllegalThreadStateException` is not reachable.

## 5. Java 8 completeness of the surrounding suite

Every other API this suite uses predates Java 9: `URLClassLoader.close` (7),
`Process.waitFor(long, TimeUnit)` / `isAlive` / `destroyForcibly` (8),
`Files.walk` (8), `Deflater.NO_COMPRESSION`, `getProtectionDomain.getCodeSource`.
A grep across `core/` for `getPlatformClassLoader`, `ProcessHandle`,
`Files.readString`, and `List/Map/Set.of` returns no Java 9+ usage; the
`Files.readAllBytes` hits are Java 7. So `getPlatformClassLoader` was the only
Java 9+ dependency and this single edit completes the repair.

One property makes the single-expression fix sufficient rather than needing a
runtime branch: the probe JVM is launched from
`new File(System.getProperty("java.home"), "bin/java")`
(`CodegenDiscoverySuite.scala:224`), so the child always runs the same JDK as
the suite. Parent and child cannot diverge in Java generation, and inherited
environment such as `JAVA_TOOL_OPTIONS` cannot reach the child with a flag the
parent did not already accept.

## 6. Style and line endings

New lines measure 107, 85, and 101 characters against `maxLineLength` 120 in
both `scalastyle-config.xml:7` and `scalastyle-test-config.xml:7`. Despite
git's CRLF advisory on the working copy, the tracked diff is +3/-1 with no
whole-file line-ending flip, so the change stays reviewable on a shared branch.

---

## Findings to act on

### M1 — minor, documentation accuracy

`.github/skills/synapseml-branches/references/branch-spark3p5.md:32-33` asserts
"Azure validation still uses Java 8 at this baseline." That is stated more
broadly than the repository supports, and a reader will find the apparent
contradiction:

- `templates/java_setup.yml` pins JDK 11 (`displayName: 'Use Java 11'`,
  `versionSpec: '11'`).
- `pipeline.yaml:1499` describes that template as "this repo's per-branch JDK
  pin, carrying 11 here and 17 on spark4.0/spark4.1."
- But the template is included by exactly **one** job, `pipeline.yaml:1509` (the
  compatibility-replay job). Every other leg inherits the hosted agent's default
  JDK, which is what produced the Temurin 8u504 failure.

The real rule is per-job, not per-branch-single-JDK. Suggested rewording:

> Azure validation is not single-JDK. `templates/java_setup.yml` pins JDK 11 but
> is included only by the compatibility-replay job (`pipeline.yaml:1509`); the
> remaining legs inherit the agent default, which is Java 8 at this baseline.

This correction *strengthens* the guidance rather than weakening it: shared test
helpers must compile on both 8 and 11, which `getSystemClassLoader.getParent`
satisfies and `getPlatformClassLoader` did not. The bullet's existing hedge,
"Confirm the actual CI JDK from its SBT startup log," already points a reader
the right way, so this is wording, not a blocker.

### L1 — low, document-only

Java 8's extension loader is a `URLClassLoader` over `java.ext.dirs`, so it is
extensible in a way the Java 9+ platform loader is not. Using the application
loader's parent therefore inherits one Java 8-only exposure the removed API did
not have: a non-stock JDK image or `-Djava.ext.dirs` could place classes ahead
of `loader` in the delegation order. The new `require` covers the probe itself
but not a *partial* shadow (for example a stale `synapseml-core` jar in
`lib/ext`). That case still fails rather than silently passing — an ext-loaded
`JarLoadingUtils` cannot see the test JAR, so `Class.forName` drops the fixture
and `"Explicit test discovery missed fixture"` fires — but the message would
misattribute the cause. No code change is warranted for a stock CI image; worth
one sentence if the M1 bullet is reworded anyway.

### L2 — informational, pre-existing helper interaction

`runtimeClasspath` (`CodegenDiscoverySuite.scala:172-175`) collects URLs by
walking loader parents and keeping `case loader: URLClassLoader`. On Java 9+
neither the application nor the platform loader is a `URLClassLoader`, so only
`java.class.path` contributes. On Java 8 both `AppClassLoader` and
`ExtClassLoader` are, so `$JAVA_HOME/lib/ext/*.jar` enters the forked `-cp` at
`CodegenDiscoverySuite.scala:221-225`.

This is benign under the new parent: parent-first means the ext loader wins, so
no duplicate class identity arises; Guava's classpath entries are keyed by
`File` with the parent taking precedence; and the
`startsWith("com.microsoft.azure.synapse")` filter at
`JarLoadingUtils.scala:103` excludes extension content from `AllClasses`. The
observable effect is a slightly longer child command line and marginal extra
scan cost, on the Java 8 leg only. Useful context when reading the driver's
8u504 timings; not worth changing.

## Second guide bullet — compatibility replay as a merge-order gate

`.github/skills/synapseml-branches/references/branch-spark3p5.md:44-47` is
consistent with the repository's own model. `pipeline.yaml:1490-1513` shows the
replay job checking out `self` plus `SynapseML-Internal` and selecting the
matching Internal branch — that is, validating against an existing release
target rather than an unmerged sync PR, exactly as the bullet claims. "Retain
the conflict as a merge-order gate" agrees with `AGENTS.md`'s port-branch rules
(merge `master` into shared port branches, never rebase or force-push them) and
with its warning that reachability is not proof a sync preserved content. No
inaccuracy found.

## Placement compliance

Both added blocks land in
`.github/skills/synapseml-branches/references/branch-spark3p5.md`, which
`AGENTS.md` designates for branch-only facts. `AGENTS.md` itself is unmodified,
so its constraint that it must not name a Spark, Scala, Java, or Python version
is not violated by naming Java 8 and JDK 11 in the branch reference.

---

## Evidence limits

These bound what this review proves. It is a static review only.

1. **No execution.** Per the round-3 instruction I ran no compile, no tests, no
   sbt, no scalastyle, no codegen, and no network calls. I did not run
   `CodegenDiscoverySuite`. The driver's aggregate `compile` / `Test/compile`,
   28 targeted tests, aggregate Scala style, and codegen on exact Temurin 8u504
   remain the authority. **This review does not establish completion.**
2. **JDK internals are reasoned, not observed here.** The
   `AppClassLoader → ExtClassLoader` (Java 8) and
   `AppClassLoader → PlatformClassLoader` (Java 9+) parent relationships are
   asserted from documented JDK structure, not from a run on 8u504 in this
   session. The driver's 8u504 leg is the confirming evidence.
3. **Shaded Guava not decompiled.** Statements about `ClassPath.from` walking
   parents, special-casing `URLClassLoader`, and de-duplicating entries are
   reasoned from the `org.sparkproject.guava.reflect.ClassPath` API used at
   `JarLoadingUtils.scala:7,100`. I did not decompile the shaded jar to pin the
   exact Guava version's system-loader fallback, so the *degraded* behavior
   described in section 2 is directionally certain but not version-pinned.
4. **Build 236308415 logs not inspected** (no network). The Temurin 8u504
   attribution is taken from the task statement; it is consistent with the code
   and with the pipeline evidence in M1, but is not independently verified here.
5. **Agent default JDK not observed.** M1 establishes that only one job pins a
   JDK; it does not independently confirm which default the hosted image
   supplies. That is precisely why the reworded bullet should keep the
   "confirm from the SBT startup log" instruction.
6. **Prior rounds.** R1 (`gpt-6-astra`) and R2 (`gemini-3.8-flash`) are present
   in `reviews/`. I saw only their `getPlatformClassLoader` reference lines
   incidentally through a tree-wide grep and did not otherwise read them;
   findings above were derived independently, so some overlap is possible.
7. **Tree state.** All citations are line numbers in the working tree of
   `.worktrees/branch-context-20260916` at branch
   `docs/spark4-branch-context-20260916`, with only the two files above
   modified and the two prior review files untracked.

## Driver disposition

- M1 fixed in the branch reference. The guide now distinguishes the explicit
  JDK 11 setup in the Internal compatibility job from the observed Java 8
  setup/publication jobs. It directs readers to each job's SBT startup log,
  rather than treating a template pin as pipeline-wide configuration.
- L1 and L2 remain documented limits, not hidden failures. Validation uses a
  stock, checksum-verified Temurin 8u504 SDK. The child uses the parent's
  `java.home`, and the probe requires isolated-loader ownership. Supporting
  modified extension directories or optimizing their small classpath scan
  would expand this test-only compatibility repair unnecessarily.
