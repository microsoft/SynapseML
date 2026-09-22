## Review summary

- Round: 1, bounded broad sweep only
- Theme: Correctness, security, logic, and polling-contract conformance
- Mode: sequential
- Model: gpt-6-astra
- Target: master cleanup polling follow-up
- Base HEAD: `81ccc5490fb48acd1c746a56993561d36fa50557`
- Scope: Four-file uncommitted delta, relevant cleanup context, and test wiring
- Artifact: `reviews\pr-2732\task-cleanup-polling-30s-attempt-1-review-1-gpt-6-astra.md`
- Issues found: 0
- Verdict: CLEAN

## Reviewed contract and snapshot

The authorized 30-second interval supersedes the earlier 60-second request. Each item receives an immediate confirmation read and at most ten additional reads, with ten requested waits totaling 300,000 ms. Request time is additional; this is not a strict wall-clock deadline.

| Repository-relative path | Git blob hash |
| --- | --- |
| `core\src\test\scala\com\microsoft\azure\synapse\ml\nbtest\FabricArtifactCleanup.scala` | `e8d971a5958fe78bcf01bfa88104ceb3a3a3f31e` |
| `core\src\test\scala\com\microsoft\azure\synapse\ml\nbtest\FabricTestArtifactTrackerSuite.scala` | `65292e9cfaf1b44168779cc9e1bfa2a7f375b7cb` |
| `core\src\test\scala\com\microsoft\azure\synapse\ml\nbtest\FabricTestArtifactTrackerFailureTests.scala` | `7099ed8f1913e21e46d9585ebf00bc2cb5fb2e8a` |
| `docs\Reference\Developer Setup.md` | `2cdfe0cd7aa0805c8da5b40591633b2b3f51cb3b` |

## Evidence checklist

- [x] Traced the loop boundary: `check(11)` reads immediately, waits only when the item remains and another attempt exists, and fails on a still-visible eleventh read without an extra sleep.
- [x] Verified `TimeUnit.SECONDS.toMillis(30)` reaches the injected `Long => Unit` callback; the default calls `Thread.sleep(millis)`. A fresh local counter starts for each item.
- [x] Inspected both direct callers in core: `FabricOperations` uses defaults, while the private `CleanupClient` forwards the adapted millisecond callback. No production public API or network destination changes are introduced.
- [x] Verified confirmation contains no DELETE. Confirmed absence alone advances `deleted`; timeout, invalid inventory, and nonfatal read errors still leave the candidate loop immediately with prior errors preserved.
- [x] Verified interruption escapes unchanged without another read or DELETE. The existing NonFatal boundaries and self-suppression protection are unchanged.
- [x] Checked immediate success asserts zero waits, five total inventory reads for child and parent, and exactly one DELETE per item.
- [x] Checked delayed success after 1, 5, and 10 waits for both child and parent: exact 30,000-ms requests, exact read counts, no repeated DELETE, and independent full budgets.
- [x] Checked timeout and not-found-but-visible cases assert ten waits and 13 total inventory reads. The two-job timeout also asserts 300,000-ms total waits and no next-job or parent DELETE.
- [x] Checked first/later confirmation read failures, conflicting IDs, interrupt identity, and a foreign consumer arriving during confirmation. Parent eligibility is reread before deletion and the new consumer keeps the parent.
- [x] Compared all five moved test bodies against HEAD; they are unchanged. The existing mixed-in trait keeps them registered on the concrete suite selected by `pipeline.yaml:968`. There are 47 unique tracker tests; the main suite is 768 lines.
- [x] Inspected `master-polling-30s-red.log`: 44 tests passed and six failed under the old timing, including actual 2,000-ms versus 30,000-ms callback values and 31 versus 11 confirmation reads.
- [x] Inspected completed `master-polling-30s-green.log`: JDK 11 core compile and Test/compile succeeded, both style checks reported zero errors and warnings, and all 50 tests passed across two suites with no failures, canceled, ignored, pending, or aborted cases.
- [x] Documentation states the requested per-item waiting budget plus request time. The focused diff passes whitespace checking; no scoped changes were staged by this reviewer.

## Conclusion and boundary

No concrete defect found in this bounded delta. The implementation satisfies the superseding polling contract and retains the existing fail-closed deletion and exception behavior.

Validation claims come from inspected parent-produced logs, not reviewer-run builds or live service execution. Only this report was written; no source edits, agents, staging, commits, pushes, or cloud calls were performed. No new port validation is claimed.

The strict Gemini-family gate remains unavailable following confirmed backend 400 failures. This is a Round 1 result only, not a full gauntlet pass or readiness declaration.
