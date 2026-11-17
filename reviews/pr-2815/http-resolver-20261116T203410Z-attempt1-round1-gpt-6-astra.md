# HTTP internal-column resolver: focused fix review

## Provenance and scope

- PR: microsoft/SynapseML#2815.
- Finding: https://github.com/microsoft/SynapseML/pull/2815#discussion_r4590904723
- Base HEAD: `cd6994a3893e8c1fe7250cbe6e8f31d09d5e02a1`.
- Reviewed source diff SHA-256:
  `f7076b637186e74be8d1d78b43f82f645a537df89be9368f85d09a77c9234cd3`.
- Reviewer: `gpt-6-astra`, focused direct review; no independent harness or
  multi-model signoff claimed.
- Scope: `SimpleHTTPTransformer.scala` and `SimpleHTTPTransformerSuite.scala`.
- Complexity estimate: Low; one internal naming policy and its two call sites,
  with no changes to request handling, parameter shapes, or existing signatures.

## Finding and resolution

The custom `equalsIgnoreCase` lambda misses the reference-identity check for
Spark's canonical case-insensitive resolver in `findUnusedColumnName`.
Consequently, each candidate temporary column name rescans the occupied names.

The fix uses `caseInsensitiveResolution`. A package-internal helper centralizes
the policy for both temporary columns and permits a deterministic visit-count
regression without timing assertions or exposing a new public Scala API.
Internal naming remains case-insensitive even in case-sensitive sessions,
preserving safe schema-only planning across session contexts.

The new public transformer test checks both resolver modes, matching
`transformSchema` to runtime output and preserving all input columns whose
names collide case-insensitively with both internal prefixes.

Direct review found no remaining actionable defects in this bounded change.
Existing Unicode/custom-resolver, error-schema, serialization, and HTTP
behavior remain covered by the selected suites.

## Regression evidence

After extracting the original naming policy without changing its lambda,
this command reproduced the performance failure:

```text
sbt 'core/testOnly com.microsoft.azure.synapse.ml.io.split1.SimpleHTTPTransformerSuite -- -z "internal column names"'
```

Result: **one failed, two passed**. The efficiency assertion observed
**2,003,000 occupied-name visits** for 2,000 collisions, exceeding the bound of
4,000. The public behavior checks passed, isolating the performance defect.

After switching the helper to Spark's singleton, the following completed
successfully under WSL with the repository-selected JDK 11:

```text
sbt core/scalastyle core/Test/scalastyle core/compile core/Test/compile 'core/testOnly com.microsoft.azure.synapse.ml.core.schema.VerifyDatasetExtensions com.microsoft.azure.synapse.ml.io.split1.SimpleHTTPTransformerSuite'
```

Both style checks reported zero errors and warnings; production and test
compilation passed; **25 tests passed, with no failures, cancellations,
ignored tests, or pending tests**. `git diff --check` also passed.

The source snapshot is retained in the session's `files/http-resolver-reviewed.diff`.
These are local results. No commit, push, new CI run, or GitHub thread
resolution is included in this fix step.
