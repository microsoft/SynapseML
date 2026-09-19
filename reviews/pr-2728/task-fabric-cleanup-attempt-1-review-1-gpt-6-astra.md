## Review summary

- **Round:** 1
- **Theme:** Broad Sweep
- **Mode:** sequential
- **Model:** gpt-6-astra
- **Base:** `master` at `cd45147c7025f483e86fc028069d72b070e73a55`
- **Issues Found:** 2
- **Verdict:** ISSUES_FOUND

Paths below are repository-relative; machine-specific prefixes were removed.

## Evidence checklist

- [x] Read `AGENTS.md`, applicable branch/review guidance, and runtime declarations.
- [x] Reviewed all four tracked uncommitted diffs and the complete untracked cleanup implementation.
- [x] Traced ownership, UTC expiry, dependency checks, activity checks, pagination, rechecks, deletion ordering, and confirmation through the implementation and supplied tests.
- [x] Checked surrounding artifact creation, connection, HTTP, and cleanup code.
- [x] Verified locally that differently cased GUID strings identify the same UUID but fail ordinal string equality.
- [x] Performed no edits, nested-agent calls, network requests, deletion commands, or builds. No internal repository code or private service data was used.
- [ ] Scala tests were not executed under the review restrictions. Findings below distinguish static traces from executed evidence.
- [ ] Live inventory completeness and soft-delete visibility were not verified. The local mocks remove inventory rows; that does not establish the service contract. No finding assumes undocumented service behavior.

## Findings

### 1. Mixed-case UUID references can hide a foreign dependency

**Severity:** High

**File:** `core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/FabricArtifactCleanup.scala:130-131`

**Reason:** The parser accepts uppercase and lowercase UUIDs but preserves their spelling. The dependency graph then uses case-sensitive string membership and map lookup. A valid reference can therefore disappear from the reverse-dependency check, allowing deletion of a store used by an unrelated item.

**Evidence:** Consider an expired, owned store with ID `01234567-89ab-cdef-0123-456789abcdef` and no outgoing references. An unowned notebook references `01234567-89AB-CDEF-0123-456789ABCDEF`. Both forms pass the parser's GUID validation. The local UUID comparison confirmed that they identify the same UUID while ordinal string equality returns false.

Following the code, `neighbors(store.id, current)` returns an empty set because `_.references(id)` does not match. The store branch at lines 176-178 accepts that empty set, and execution reaches `client.delete` despite the foreign consumer. This is a static trace, not an executed Scala test.

**Fix:** Canonicalize every artifact and reference UUID before indexing, duplicate detection, self-reference removal, and graph comparisons. Apply this to relation metadata, parent IDs, and default-store IDs. Add a regression where an unowned consumer references an owned store using different UUID casing and assert that the store is retained.

### 2. Cascading lakehouse deletion bypasses endpoint age and unknown-metadata protection

**Severity:** High

**File:** `core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/FabricArtifactCleanup.scala:141-143`

**Reason:** `managedEndpoint` accepts an endpoint based only on its type, relationship, and provisioning state. It does not check creation or update timestamps. The store-deletion branch explicitly permits these endpoints, so an expired lakehouse can be deleted even when its endpoint was just updated or has unknown age. Delegating endpoint removal to lakehouse deletion does not preserve the recently changed or unknown dependent item.

**Evidence:** In the supplied exclusive-endpoint fixture, change only the endpoint's `updated` value to `Some(cleanupNow)`. Alternatively, set its `created` value to `None`. In either case, `managedEndpoint` remains true and the endpoint's neighbors still equal `Set(candidate.id)`. The lakehouse therefore passes lines 176-178 and reaches deletion. The expiry filter at line 164 covers job/store candidates, not the endpoints whose continued operation depends on the store. This is a static trace.

**Fix:** Require known, strictly expired creation and update timestamps for every managed endpoint accepted for cascading deletion. Retain the parent when an endpoint is recent, at the cutoff, or has unknown timestamps, and evaluate those conditions from the refreshed inventory. Extend the endpoint regression cases to cover each condition.

## Resolution

- Canonicalized artifact IDs, relation IDs, parent IDs, and default-store IDs through `UUID`.
- Applied the same strictly older-than-24-hours rule to managed endpoints.
- Added mixed-case foreign-consumer and endpoint-age regression tests to the already scheduled `FabricTestArtifactTrackerSuite`.
- Targeted suite and Scala style results are recorded in the PR validation evidence.
