# Follow-up round 1: confirmation-read safety

**Result:** CLEAN for the bounded correction.
**Reviewer:** GPT-6 Astra, 2026-09-21.
**Scope:** current-head finding in microsoft/SynapseML#2732,
`discussion_r4066179872`; cleanup test infrastructure and its documentation.

The previous implementation caught an inventory exception from `confirmAbsent`
as though DELETE had failed, then attempted more deletions. The new 16-case
regression failed before the fix: three DELETE attempts occurred where one was
expected (`master-confirmation-read-red.log`, retained locally).

`tryDeleteItem` now classifies only actual DELETE failures. Concurrent not-found
responses still require absence confirmation. A single outer nonfatal handler
protects the candidate loop, including all safety reads and confirmation reads.
It rethrows the same exception with prior deletion errors attached, excluding
self-suppression. No later DELETE occurs after such an abort. Interrupts and
fatal errors retain their prior propagation.

Unconfirmed deletion now also ends the run; the two-job timeout regression
asserts only the first DELETE and exactly 30 pauses. Ordinary DELETE errors
still allow independent jobs to be attempted and prevent store deletion.

Final master evidence: zero test-style errors, 43 tests through the exact
CI-selected tracker suite, and 46 tests when the unchanged naming suite is
included. Both moved error tests appear in that existing suite's output.
Logs: `master-confirmation-read-green-v3.log` and
`master-confirmation-read-green-v4.log`, retained locally.

No public SparkML API or production implementation changes. Ports must receive
this commit and run their JDK 17 checks before their follow-up is complete.
This is not a completed three-family gauntlet; Gemini remains unavailable.
