# Round 4 detailed correctness review

**Verdict: CLEAN. Issues found: 0.** Static review only, not an execution pass.

Task `java8-codegen-20260917`, attempt 1, round 4, sequential, model `gpt-6-astra`.
Target context is `master`; HEAD is `1d12c2b8bc557ceca1a96a977649cbbd29054413`.
Read `AGENTS.md` and the exact two-file tracked diff. No production change.
Reviewed content hashes are `82c5036f14d213c3802fe5acdbb10996e17800c2` for the
Scala suite and `dd21d8109b5d61f6ce6d87b6bef83b037c031150` for the branch guide.

## Evidence checklist

- [x] Java 8 API availability. `ClassLoader.getSystemClassLoader()` and
  `getParent()` date to Java 1.2. The `URLClassLoader` constructor,
  `Class.getClassLoader` and reflection calls also exist on Java 8.
  `URLClassLoader.close` requires Java 7; `Files.walk`, timed `Process.waitFor`,
  `isAlive` and `destroyForcibly` require Java 8. Replacing the Java 9-only
  `getPlatformClassLoader` removes that compile-time dependency without a runtime branch.
  `core\src\test\scala\com\microsoft\azure\synapse\ml\codegen\CodegenDiscoverySuite.scala:21-35,184-203,224-236`.

- [x] Delegation and defining-loader identity. On stock Java 8, the system
  application's parent is the extension loader; on stock Java 9+, it is the
  platform loader. Both retain bootstrap access without delegating project
  classes to the application loader. `probe.getClassLoader eq loader` checks
  reference identity, not `equals` or merely the initiating loader. A bootstrap
  definition yields null and fails this comparison safely.
  `core\src\test\scala\com\microsoft\azure\synapse\ml\codegen\CodegenDiscoverySuite.scala:21-31`.

- [x] Discovery retains one application-type identity. The probe, fixtures and
  production classes resolve through the isolated loader under the stock-JDK
  assumption. `JarLoadingUtils` scans its defining loader, and its
  `Class.forName` calls use that same caller loader. The probe's `Class` comparisons
  therefore compare definitions from one loader, not parent/child duplicates.
  `core\src\main\scala\com\microsoft\azure\synapse\ml\core\utils\JarLoadingUtils.scala:98-113`;
  `core\src\test\scala\com\microsoft\azure\synapse\ml\codegen\CodegenDiscoverySuite.scala:44-69`.

- [x] Reflection boundary is type-safe. The existing Scala static `main(String[])`
  forwarder accepts the bootstrap-defined `String[]` type on either side.
  `invoke(probe, args)` passes the array as one argument, without varargs expansion;
  reflection ignores the receiver for a static method. No Scala/Spark instance
  crosses the loader boundary through a cast.
  `build.sbt:34`; `core\src\test\scala\com\microsoft\azure\synapse\ml\codegen\CodegenDiscoverySuite.scala:20,29-31,40-43`.

- [x] Failure and cleanup paths remain failures. Loading, ownership-check and
  invocation exceptions enter the existing `finally`, which restores the context
  loader before closing the URL loader. Reflected target failures escape as
  `InvocationTargetException`; a close failure can mask an earlier exception but
  cannot create success. The parent waits or kills/reaps the child, requires
  completion plus exit zero plus the success marker, then deletes temporary files.
  `core\src\test\scala\com\microsoft\azure\synapse\ml\codegen\CodegenDiscoverySuite.scala:27-35,226-243`.

- [x] Child Java selection follows the running JVM's `java.home`, not PATH or
  `JAVA_HOME`. Stock Java 8's nested JRE supplies its own `bin\java`; newer JDKs
  supply it directly below `java.home`. Thus an actual 8u504 parent selects that
  installation for the child. Separate `ProcessBuilder` arguments preserve spaces.
  `core\src\test\scala\com\microsoft\azure\synapse\ml\codegen\CodegenDiscoverySuite.scala:208,221-229`.

- [x] R3 M1 is corrected. The guide separates Internal compatibility's explicit
  JDK 11 from the supplied observation of Java 8 setup/publication jobs. The public
  template and its job-local inclusion support that distinction. The separate
  release replay fetches an existing release tip and fails on patch conflicts,
  consistent with the guide's merge-order gate rather than a validation bypass.
  `.github\skills\synapseml-branches\references\branch-spark3p5.md:32-49`;
  `templates\java_setup.yml:1-7`; `pipeline.yaml:1068-1071,1349-1393,1418-1419,1509`.

## Evidence limits

R1/R2 were not rerun. R3 L1/L2 remain recorded stock-JDK limitations, not new
findings; adversarial extension-directory configurations are outside this review.
The `masterCI236308415` Temurin 8u504 failure is supplied evidence, not a log
retrieved here. No private trees, network, agents, code fixes, commits or test runs.
The driver owns exact Temurin 8u504 aggregate compile, Test/compile, 28 targeted
tests, aggregate Scala style and codegen. Those results remain pending here.
