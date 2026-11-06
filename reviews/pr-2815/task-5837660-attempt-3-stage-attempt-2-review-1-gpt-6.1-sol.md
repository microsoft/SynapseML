---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 3
stage_attempt: 2
round: 1
timestamp: "2026-11-06T18:41:44Z"
outcome: "issues_found"
reviewer_model: "gpt-6.1-sol"
input_sha256: "2996863fcb8581381dd5a23a58c40bf5bffbb403be3b076b1b4aa9600210ee10"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: gpt-6.1-sol
- **Artifact**: C:\Users\boraelkin\SynapseML-cu-native\reviews\pr-2815\task-5837660-attempt-3-stage-attempt-2-review-1-gpt-6.1-sol.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Reviewed the complete supplied 37-path diff, including the frozen source/test correction, against the supplied master baseline.
- [x] Traced submission, polling, management, deadlines, DNS admission, and handle retention through `ContentUnderstandingProtocol.scala`, `ContentUnderstandingDnsResolver.scala`, and the CU Scala suites.
- [x] Checked authentication provenance, URL/header validation, and secret handling in `ContentUnderstanding.scala`, `ContentUnderstandingHeaderValidation.scala`, and `CognitiveServiceBase.scala`.
- [x] Inspected Python parameter lifecycle and persistence in `ContentUnderstandingParams.scala`, `Utils.py`, `Wrappable.scala`, `ComplexParamsSerializer.scala`, and both offline Python test files.
- [x] Independently traced asynchronous execution through `core\src\main\scala\com\microsoft\azure\synapse\ml\io\http\Clients.scala:57–61`, `HTTPTransformer.scala:129–139`, and `AsyncUtils.scala:21–34`; scoped searches found no task-cancellation hooks in the HTTP execution package or native CU implementation.
- [x] Reviewed supplied validation provenance, distinguishing current compilation/codegen/Python results from earlier broader Scala results.
- [ ] No commands or tests were executed during this read-only review. Live CU, packaged-runtime, and Azure CI validation remain outstanding as disclosed.

## Issues

### Issue 1: Spark cancellation does not stop asynchronous CU workers
- **Severity**: Medium
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala
- **Line(s)**: 289–292, 418–429, 668–678
- **Description**: With `concurrency > 1`, the CU handler runs in global-execution-context Futures (`Clients.scala:57–61`), while the Spark task awaits their results. `checkInterrupted()` checks only the Future worker's thread, not the owning Spark task. Cancelling the job interrupts the awaiting task but neither cancels those Futures nor closes their independently created CU clients. Consequently, queued workers can start submissions after cancellation, and active workers continue polling until their own limits expire. A loopback reproduction is to run concurrent analysis against a fixture returning `Running`, cancel an interruptible Spark job group after polling starts, and inspect subsequent GET requests. Existing interruption tests exercise driver management, not this asynchronous path.
- **Risk**: Cancelled tasks leave unowned local work consuming HTTP/DNS capacity. Queued submissions can still create billable operations whose handles cannot reach the cancelled action. Repeated cancellation/restart can accumulate workers beyond the pressure implied by currently active Spark partitions.
- **Suggested Fix**: Propagate task-scoped cancellation into CU workers before dispatching Futures. Check that cancellation state before submissions, polls, and waits, and register best-effort cleanup for active requests when the owning task ends. Preserve already-received handles where an output remains deliverable; do not replay submissions or cancel siblings merely because of a shared service failure. Add a focused asynchronous job-cancellation regression asserting that no further local requests begin after cancellation.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending.
- **Why**: CU asynchronous workers currently have no connection to their owning Spark task's cancellation state.
- **How verified**: Static source tracing only; no cancellation test was executed during this review.

## Driver reproduction and scope assessment

**Confirmed for continued polling, still open.** An offline probe using the
current generated Python wrapper and compiled JVM classes ran a single
document against a loopback server returning Running. It cancelled an
interruptible Spark job group after two GETs, waited for the collect action
to report cancellation, allowed a 750 ms grace period, then observed requests
for 1.25 seconds:

| Per-partition concurrency | Additional GETs after cancellation and grace |
| --- | --- |
| 1 | 0 |
| 2 | 6 |

Both Spark actions were confirmed cancelled. The server was then changed to
Succeeded so the surviving worker could finish; the fixture and Spark session
were closed. No live CU service was contacted. This reproduces local polling
after cancellation, not remote operation cancellation or the separate claim
that queued POSTs can start after cancellation.

The first probe mistakenly configured a 30 ms budget and therefore did not
reach the intended cancellation scenario. That failed log is preserved. The
corrected 30000 ms run is recorded in session artifacts
`cu-native-cancellation-probe-r2.log` and
`cu-native-cancellation-observations.json`; the session-only probe is
`cu_cancel_probe.py`. Repository source was unchanged.

`Clients.scala`, `HTTPTransformer.scala` and `AsyncUtils.scala` are unchanged
against master d3f3af6b. Their shared Future execution lacks owning-task
cancellation; CU's polling exposes that limitation. The finding is not caused
by the two-file setParams correction and is not rejected merely because the
underlying mechanism predates CU. A correction requires a deliberate
task-to-worker cancellation design, rather than a blanket exception catch or
an assumption that a Spark task interrupt reaches a global Future worker.

Input/provenance audit: the full frozen snapshot is in the actual diff section,
the exact context and hash match, runtime is gpt-6.1-sol, and all 38 tool calls
have results with no feedback access. The overall six-stage review is not
complete or clean.

## Driver stage advancement

The human selected: "Finish the remaining High review stages first; keep
cancellation open." The driver therefore proceeds to ordered stages 2-6 on
the same frozen snapshot, retaining this confirmed finding and the other
reported risks. This is advancement with unresolved findings, not clean
signoff and not permission to hide them in later summaries.

Effective effort remains human-selected High for the large, high-complexity
37-file contribution. The continuing human reasoning override is
gpt-6.1-sol/high, grok-4.7/low and claude-opus-5.5/high, each independently
invoked through the skill-owned runner. Primary model: gpt-6-astra.
Current catalog availability was checked. The existing stage and outer safety
bounds remain; no permission/trust or evidence guard was relaxed.

The original failed Opus result is retained. The human-authorized Opus-only
replacement completed with findings; Grok returned clean, with failed
path-format lookups recorded separately. Mixed-slot source/input/runtime
provenance was audited without reclassifying the failed invocation as valid.
No new production change, commit or push accompanies this advancement.

### Stage 2: architecture and patterns

Both normal High attempts completed with all three required reports.
The second attempt returned issues_found from GPT, Grok and Opus. Actual
inputs, runtime identities, receipts and source-only tool access passed the
audit; five Grok malformed path lookups failed without broader fallback.
Stage 2 advances **with unresolved findings**, not clean, under the bounded
High gate. Stage 3 begins on unchanged snapshot 3. The human subsequently
requested concern analysis only after the remaining reviews finish, so no
further finding adjudication or implementation accompanies this transition.

### Stage 3: first-pass audit note

GPT and Grok returned clean; Opus returned issues_found. The transport audit
retains an unscoped-search flag for Opus call `toolu_01GVBJ1111eP7s7hfmJCL4zk`.
Manual inspection of its actual tool result confirms a double-escaped path
argument failed with `Search paths do not exist`, returning no file content
and performing no fallback search. This is retained as a tool-format warning,
not silently removed from the original audit. Other provenance checks passed.
The second and final normal High pass proceeds on unchanged snapshot 3;
concern analysis remains deferred.

### Stage 3: second-pass Grok scope recovery

The second-pass reports all returned issues_found. GPT and Opus passed their
provenance audits. Grok successfully searched the broader `cognitive` module
directory instead of the required `cognitive\src` root. Its actual result
contained only `CognitiveServiceBase.scala` and no review feedback, but the
broader execution is a genuine scope violation, unlike a failed malformed
lookup. That invocation is retained and does not count as compliant signoff.

Only Grok is replaced, with invocation stage-attempt 3 on the same frozen
snapshot; the valid GPT/Opus slots remain. This is invalid-slot recovery, not
a third complete stage attempt. Original reports and the flagged audit remain
unchanged. No review finding was supplied to the replacement, and no runner,
permission, trust or evidence safeguards were relaxed.

The replacement Grok report completed with issues_found and passed the
mixed-slot provenance audit. Three malformed path lookups failed without
fallback and remain format warnings. Stage 3 is now complete under the bounded
High gate and advances **with unresolved findings**. Stage 4 (detailed
correctness) begins on unchanged snapshot 3. Earlier clean reports do not erase
findings from other passes; all concern analysis remains deferred.

### Stage 4: detailed correctness

Both normal High attempts completed. The final pass returned issues_found
from GPT and clean from Grok and Opus. The complete batch passed input,
runtime, receipt and source-scope auditing; six failed malformed Grok lookups
remain format warnings. Stage 4 advances **with unresolved findings**, not
clean. Stage 5 (testing and coverage) begins on unchanged snapshot 3. No
concern analysis, implementation, commit or push accompanies this transition.

### Stage 5: testing and coverage

Both normal High attempts completed with issues_found from all three models.
The final batch passed the provenance/source-scope audit; seven failed
malformed Grok lookups remain format warnings. Stage 5 advances **with
unresolved findings**. Stage 6 (polish and hardening) begins on unchanged
snapshot 3, with concern analysis and implementation still deferred.

### Stage 6: polish and hardening; collection complete

Both normal attempts completed. The final pass returned clean from GPT and
Grok, and issues_found from Opus. The final audit preserved four failed malformed
Grok lookups as warnings. One exact read of
`docs/Get Started/Install SynapseML.md`, lines 1-5, returned only frontmatter and
the first heading. This was manually accepted as documentation-convention
evidence outside the auditor's static known-file list. The original flag and
actual tool result remain preserved; no guard or original outcome was changed.

All six ordered stages are now complete **with unresolved findings**, not
clean or merge-ready. The 38 retained artifacts include invalid/replaced
invocations and must not be described as 38 valid independent signoffs.
Later clean reports do not erase earlier findings.

## Driver consolidated assessment

The 50 reported findings map exactly once to 33 topics in the session artifact
`cu-native-review-dispositions.json`. Every report containing findings receives
an appended per-issue disposition without changing its original model verdict.
Coverage and architecture suggestions remain distinct from demonstrated bugs.

### Reproduced defects still open

| Topic | Observed behavior |
|---|---|
| Owning-task cancellation | After Spark cancellation and a grace period, concurrency 2 made six additional polling GETs; concurrency 1 made zero. Queued POSTs were not reproduced. The shared async implementation predates CU. |
| Persisted ordinary defaults | Save/load preserved a 1000 ms JVM default, but Python reported 600000 ms and transferred that value back over the saved default. |
| Generic sequence projection | TextSentiment's typed getter returned a Python list, while getOrDefault/extractParamMap returned a JavaObject for the same value. |
| Ambiguous terminal submission | HTTP 203/206 with a Succeeded body and valid location produced Unknown without a retained handle, response diagnostic, or errorCol; one POST and no GET. |
| Creation lookup failure | Accepted PUT followed by management GET 404/410 produced Failed with a retained handle but no getAnalyzer recovery guidance; exactly PUT plus GET. |

These are diagnostic reproductions against unchanged current artifacts, not
permanent regression tests or fixes. Evidence is retained in
`cu-native-cancellation-probe-r2.log` and `cu-native-review-probes.log`.
The latter intentionally tested claimed failures: four reproduced; its fifth
assertion failed because the claimed explanation KeyError did not occur.

### False positives and evidence limits

- PySpark clear returns None, not self; no fluent-return regression exists.
- JVM floating-to-integer conversion saturates. JDK 11 produced 2147483647 for
  both `(int)2147483648.0` and the computed accepted maximum timeout.
- Installed PySpark explainParam guards dictionary access with membership, not
  hasDefault. A separate positive control proved both explainParam and
  explainParams work with a JVM-only default and a pending Python value.
  `cu-native-review-explanation-probe.log` passed.
- The inherited SimpleHTTPTransformer parseErrors Lambda does enter the
  SparkContext/emptyRDD schema fallback. That dependency is source-confirmed;
  actual failure in an RDD-restricted runtime was not reproduced.
- The polling 404/410 documentation needs qualification for unreadable or
  oversized bodies, where the implementation preserves prior/Unknown state.
- Scalar fallback-auth partition stopping is explicitly documented policy.
  Credential visibility through generic logging is a known, documented risk,
  not eliminated by documentation; extractParamMap-only redaction would not
  stop the verified MLflow getOrDefault path.
- No concrete production trigger was established for the generic
  RuntimeException or deadline handle-loss claims. A JDK 11 probe showed
  cancel(false) can return true while its task is already running, so the
  suggested deadline patch does not establish correct ownership.
- Test coverage improvements and duplicate-validation performance concerns
  remain recorded separately; absence of a test is not a reproduced defect,
  and performance impact has not been measured.

### Scope and next decision

Recommend focused Python/protocol corrections with regressions, explicit schema
planning for the shared HTTP helper, and a deliberate owning-task cancellation
design. Review the changed paths and their dependent lifecycle behavior rather
than automatically restarting all six stages. Defer cosmetic architecture
refactors unless needed by a correction.

No production changes were made during this triage. The two-file setParams fix
remains local, uncommitted and unpushed; the PR description was not changed.
Snapshot 3 still contains 37 paths and 441538 patch bytes with SHA256
`e5b3150bbae9ee013ea0c08a94a194bbcf11701419ff548a19588d7e8ff5dbcd`.
Review artifacts also remain local and require publication with the reviewed
changes. Azure/live/package validation remains outstanding, and the PR stays
draft.

## Driver consolidated disposition (2026-11-06)

Post-review driver assessment; this does not rewrite the model verdict,
clear unresolved findings, or validate an invocation rejected by its audit.
Duplicate reports are not independent confirmations.

### s1t2-gpt-6.1-sol-i1: Concurrent polling survives Spark cancellation

- Disposition: **confirmed**.
- Evidence: Loopback reproduction:0 late GETs at concurrency1,6 at concurrency2 after action cancellation and grace. Shared async implementation predates CU; queued POST claim not reproduced.
- Consolidated topic: `cancellation`.

## Driver focused correction follow-up (2026-11-06)

This section records subsequent work; it does not rewrite the original findings,
verdicts, or rejected invocation history above. The human authorized focused fixes,
regressions and targeted review, then confirmed one three-model pass rather than
another six-stage cycle.

### Effective follow-up decision

| Field | Effective value and source |
|---|---|
| Tier | High; primary estimate based on concurrency, deadlines, state transitions and Python/JVM lifecycle interactions. |
| Change size | 13 tracked paths; 46,596-byte correction delta against HEAD, not the entire PR. |
| Cyclomatic complexity | Qualitatively substantial: task ownership and interruption, DNS admission/wait accounting, HTTP state recovery, schema planning and parameter ownership. |
| Primary model | `gpt-6-astra`. |
| Reviewers | Human-confirmed `gpt-6.1-sol/high`, `grok-4.7/low`, `claude-opus-5.5/high`; all exact IDs present in the current catalog. |
| Stage behavior | Human-confirmed single concurrent detailed-correctness pass; reruns only for concrete findings. This overrides default six-stage advancement and maximum-reasoning defaults. |
| Safety | Existing permission, trust, time, credit and cycle controls remain unchanged. Independent read-only invocations exclude prior/sibling feedback. |

Review attempt 4, round 4, stage attempt 1 uses frozen correction snapshot 1:
SHA256 `ae26d95e63760cbfbbf2fbda2330cf5bfa9b8359c43b6ca76a73818ae23764ed`.
HEAD remains `085928a9b1ee73473ea339c5a3b28bc2d5e5b1e0`; integrated target is
`d3f3af6bd3e4b7d191f51c9eb29f9aeffe788193`.
All three actual reviewer inputs contain the exact context and complete 13-file
patch under the diff section. Startup events identify the assigned models without
substitution. Final transport audits and verdict adjudication are still pending.

### Locally corrected behavior

The earlier setParams correction remains included. Owning-task cancellation now
propagates into queued async requests, CU polling, DNS waits and best-effort abort
of blocked HTTP. Worker association is finally-restored; DNS remains bounded and
its waits retain elapsed limits even when a budget provider returns a constant.
Remote operations and native DNS delegates cannot be rolled back.

Ordinary persisted JVM defaults are restored to loaded Python wrappers, and generic
sequence inspection returns Python lists. Nonstandard terminal submissions explain
discarded/unsafe handles. Creation lookup 404/410 preserves the accepted state and
provides recovery guidance instead of inventing a provisioning failure; sanitized
exception messages recognize the connector-owned lookup diagnostic.

Shared HTTP schema planning has an explicit callback rather than the inherited
RDD/session inference path. Polling documentation now qualifies unreadable or
oversized-body cases. These corrections do not establish general Spark Connect
support, eliminate credential-logging risk, or implement optional architecture
refactors.

### Regression provenance

- Before fixes, two Python default/sequence tests, schema-only planning, and both
  protocol regressions failed as expected. The earlier cancellation reproduction
  observed six late GETs at concurrency 2 and zero at concurrency 1.
- All four affected Scala style scopes, core/cognitive main and test compilation,
  and both code-generation tasks passed. Final internal DNS and diagnostic edits
  were recompiled and checked with their affected suites.
- Nine shared HTTP tests and all 167 offline CU Scala tests passed on corrected
  code. The final two management/exception-message tests also passed.
- The full 59-method Python run passed 58 methods. Its cancellation fixture
  initially missed TCP-reset disconnects; after recognizing actual reset/abort or
  EOF (not timeouts), the cancellation method passed all four combinations of
  blocked/polling HTTP and parent-thread interruption enabled/disabled, including
  fresh subsequent jobs. This is not a claim of 59 passes in one full run.
- Full pinned Black left 219 files unchanged; final whitespace checking passed.
  Earlier failed style attempts, the DNS elapsed-limit correction, and the fixture
  failure remain preserved in session evidence.

At this checkpoint the corrections are **local, uncommitted and unpushed**.
The targeted reviews are running, not clean yet. Live CU, Azure pipeline execution,
packaged-install validation and performance evidence remain outstanding. The PR
stays draft and its description is unchanged.

### Targeted attempt 1 collection and attempt 2 dispatch

The human subsequently requested iteration through the targeted review/fix loop.
The effective gate is therefore to resolve verified findings and repeat this
targeted stage until the required reviews pass or an existing safeguard/concrete
blocker stops work. It is not a six-stage restart and does not permit unresolved
confirmed defects to be cleared by a later clean result.

Attempt 1 returned ISSUES_FOUND from all three models. GPT's two findings and
Opus's four findings passed provenance and scoped-tool audits. Grok's one finding
overlapped Opus; its invocation is scope-invalid because two searches omitted
explicit paths. The original report, violation details and failed encoded-path
lookups are retained, and that invocation is not counted as independent signoff.

The seven reports of issues consolidate into five concerns, all reproduced:
case-only error-column replacement, invalid/coerced error-column types,
unsuccessful-response diagnostic loss, ordinary None mutation in setParams, and
FIFO DNS overtaking. The ANSI-mode type discrepancy was reproduced alongside
the reported incompatible-type case. The first protocol/DNS regression attempt
had fixture problems; the corrected run reproduced the actual failures.

Attempt 2 corrections preserve Spark's name-resolution/coercion behavior without
schema-time session/RDD creation; validate service arguments before skipping
ordinary None values; preserve unsuccessful-response service diagnostics; and
retain FIFO admission tokens across cancellation checks, removing them on every
exit path.

The initial post-fix run passed all 12 shared HTTP tests and 169 of 170 CU tests.
The one failure required an unsafe-handle warning on a throttled response. Its
existing assertion was kept, and diagnostics were combined rather than choosing
one: the service error remains primary, with unsafe-location detail appended.
Earlier innererror/details and nonobject service errors remain available.

Final local validation for attempt 2:

- All four affected Scala style scopes, core/cognitive main and test compilation,
  and both code-generation tasks passed.
- All 12 shared HTTP tests passed, including resolver modes, ANSI coercion,
  incompatible/ambiguous inputs, nullability and no-session planning.
- After the final internal diagnostic correction, affected style/compilation
  passed again and **all 171 offline CU Scala tests passed**.
- **All 60 Python methods passed in one full run**, including CU and existing
  ServiceParam bridge regressions, cancellation, lifecycle and None handling.
- The affected Python file passed pinned Black; the prior full 219-file check
  remains retained. Final whitespace checking passed.

Attempt 2 uses 14 tracked paths and 59,411 patch bytes with SHA256
`0e991d08239469809b50f93aced5da301131b73be92dc872b2aad6aa5cbcb451`.
HEAD/base remain as recorded above. The primary remains `gpt-6-astra`; High tier
is still justified by concurrency, state, schema and Python/JVM interactions.
The exact human-selected reviewers remain `gpt-6.1-sol/high`, `grok-4.7/low` and
`claude-opus-5.5/high`, launched independently through the skill-owned runner with
fresh external run directories. Source is frozen; final collection and audit are
pending. No permission, trust, time, credit or cycle safeguard was changed.

These are local results against rebuilt classes and generated wrappers, not
packaged-install, live-service, performance or Azure CI evidence. The changes
remain uncommitted and unpushed while the targeted loop continues.

### Targeted attempt 2 collection and attempt 3 dispatch

All three attempt-2 invocations passed the final provenance and scoped-tool audits
with no warnings or violations. GPT reported one issue, Opus two and Grok two.
Their reports cover three actionable diagnostic concerns: nonobject creation
lookup errors, terminal 203/206 service errors replaced by handle warnings, and
specific creation body/transport failures relabeled by the lookup wrapper.
Grok's additional nullable-coalesce claim is contradicted by the existing passing
nonnullable preparation-error regression; its proposed always-nullable change was
not applied. Original findings and per-report dispositions remain preserved.

The three new diagnostic regressions failed before correction. The implementation
now preserves serialized nonobject lookup errors, reuses the bounded diagnostic
merge for terminal submissions, and reserves the lookup wrapper for readable
responses without a transport/body diagnostic. The public creation matrix covers
objects with and without a code, strings, arrays, numbers and booleans. Existing
state, safe-handle and no-replay assertions remain intact.

Final attempt-3 validation passed affected main/test style and incremental
compilation, **all 172 CU Scala tests**, and **three affected Python public paths**.
The preceding complete 60-method Python and 12-test shared HTTP passes remain
evidence for the unchanged Python/wrapper, shared HTTP and cancellation surfaces;
they were not rerun or relabeled as final-snapshot full-suite results. The final
edits are internal diagnostic handling and Scala regressions, not generated APIs.
Whitespace checking passed.

Attempt 3 uses 14 paths and 61,783 patch bytes with SHA256
`30d98a1e6c7fbd2f2aa0586245ba9cb8191d5dba77903f6dd61a3845717734c5`.
The effective High tier, qualitative complexity rationale, primary
`gpt-6-astra`, human-selected `gpt-6.1-sol/high`, `grok-4.7/low` and
`claude-opus-5.5/high`, and targeted iterative gate remain unchanged. The three
skill-owned independent invocations use fresh external run directories with
frozen source. Collection and final audit are pending; no six-stage restart or
safeguard change occurred. Nothing has been committed or pushed.

### Targeted attempt 3 collection and attempt 4 dispatch

All three attempt-3 invocations passed the finalized receipt, actual input,
runtime identity and scoped-tool independence audits, with no violations or
warnings. GPT and Grok returned CLEAN. Opus returned ISSUES_FOUND with one
low-severity creation-submission diagnostic gap. Original verdicts are retained.

The public createAnalyzer regression reproduced the issue before correction:
HTTP 203 with a management handle threw an exception whose response claimed
Succeeded. The rejected-handle branch already existed in `085928a9b1`; the prior
analysis-submission correction had not covered this separate management path.
The correction uses creation status names and the existing bounded diagnostic
merger to retain service/HTTP errors, report Unknown, validate against the intended
analyzer and provide getAnalyzer recovery guidance. No handle is followed and no
creation is repeated.

Affected main/test style and incremental compilation passed, followed by all
173 offline CU Scala tests and three affected Python public-path tests. The new
matrix covers 203/206/500, Succeeded/creating and object/string errors, with valid,
cross-origin and wrong-analyzer handles. Red and green logs are retained as
`cu-focused-t4-red.log` and `cu-focused-t4-green.log`. Earlier full Python and shared
HTTP evidence remains correctly attributed to its original snapshot.

Attempt 4 uses 14 tracked paths and 65,538 patch bytes with SHA256
`936e994e736e5a6d59e7ba5227488f642adb142da5b01fa358864d7e31180c37`.
HEAD and integrated base remain unchanged. The primary is `gpt-6-astra`.
The effective High tier is justified by the aggregate state, concurrency, schema
and Python/JVM corrections. Human-selected `gpt-6.1-sol/high`, `grok-4.7/low` and
`claude-opus-5.5/high` remain live catalog IDs and override default reasoning.
The human-authorized targeted iterative gate remains in effect; there is no
six-stage restart and no change to safety bounds or permissions. Three independent
skill-owned invocations use fresh external run directories and frozen source.
Collection and final audit are pending. Nothing has been committed or pushed;
live-service, Azure CI, package and performance gates remain outstanding.

### Targeted attempt 4 collection and attempt 5 dispatch

All three attempt-4 invocations passed the finalized receipt, actual input,
runtime identity and scoped-tool independence audits with no violations or
warnings. GPT and Grok returned CLEAN; Opus returned ISSUES_FOUND with two issues.
The human reaffirmed the iterative fix/review loop and emphasized avoiding new
defects. Original feedback and verdicts are preserved.

Both findings were reproduced before correction (`cu-focused-t5-red.log`):
accepted creation with an unsafe handle reported Failed rather than Unknown,
and interruption immediately after DNS admission escaped as InterruptedException
instead of InterruptedIOException. The fixes reuse the existing creation
diagnostic helper and add the missing interrupt-conversion catch, respectively.
They introduce no new public API or retry behavior and retain permit ownership.

The expanded creation matrix verifies accepted/nonaccepted statuses, unsafe and
wrong-analyzer handles, uncertainty, original service details and recovery with
PUT-only traffic. A deterministic DNS test injects interruption at the vulnerable
check, repeats past pool capacity, preserves cause/flag, confirms no native lookup
and verifies a subsequent lookup succeeds. Affected main/test style and incremental
compilation passed; all 174 offline CU Scala tests and four affected Python paths,
including job cancellation, passed (`cu-focused-t5-green.log`). Adjacent successful
creation, definitive operation failure, body/transport diagnostic and existing DNS
FIFO/expiry regressions remain intact and passing. Earlier full shared HTTP and
Python evidence retains its original snapshot attribution.

Attempt 5 uses 14 paths and 69,438 patch bytes with SHA256
`b0640b9e490dccf25d2345ac44668583f9155c40a4f0c83d4400b58f7642f347`.
The primary remains `gpt-6-astra`; High tier remains justified by aggregate
concurrency, state, schema and Python/JVM interactions. Human-selected reviewers
are unchanged: `gpt-6.1-sol/high`, `grok-4.7/low`, `claude-opus-5.5/high`, all
present in the live catalog. The human-authorized targeted iterative gate remains
in effect. There is no six-stage restart, model substitution or safeguard change.
Fresh independent skill-owned runs use frozen source; collection and final audit
are pending. No commit, push or external-validation readiness claim is made.

### Targeted attempt 5 final collection and loop closure

The final targeted pass is CLEAN from all three required runtime reviewers:
`gpt-6.1-sol`, `grok-4.7`, and `claude-opus-5.5`. Every finalized receipt,
actual-input hash, embedded context/full diff, runtime model identity and scoped
read-only tool trace passed the final audit with no violations or warnings.
The source still matches snapshot 5 and patch SHA256
`b0640b9e490dccf25d2345ac44668583f9155c40a4f0c83d4400b58f7642f347`.
The human-authorized targeted review/fix loop is complete, with no unresolved
confirmed findings in the correction scope.

This closes the initial focused corrections and the verified follow-up findings:
Spark resolver/type planning, ordinary-None compatibility, FIFO DNS admission,
service/body/lookup diagnostic retention, discarded creation handles and DNS
post-admission interruption. Original findings, false-positive adjudications,
failed fixtures and the invalid attempt-1 Grok invocation remain in the history;
none is rewritten as an originally clean review.

Final-code local evidence is all 174 offline CU Scala tests and four affected
Python public paths, including cancellation, after affected style/compilation.
Earlier complete shared HTTP (12 tests), Python (60 methods), code-generation and
pinned formatting results remain attributed to their original snapshots. The
final internal changes did not alter public/generated APIs, Python or shared HTTP.
Each confirmed latest defect has failing-before and passing-after evidence.

The live PR still targets master and remains draft. Immediately before publication,
GitHub master was `d3f3af6bd3e4b7d191f51c9eb29f9aeffe788193` and the remote PR head was
`085928a9b1ee73473ea339c5a3b28bc2d5e5b1e0`, matching the reviewed baseline. These
review artifacts are preserved with the correction commit. Clean targeted review
does not establish merge readiness: live CU, Azure CI, packaged-install and
representative performance evidence remain outstanding. Previously documented
credential-logging sensitivity, deliberate scalar-auth partition stopping and
optional/unproven broader concerns retain their original dispositions.

Publication formatting normalizes Markdown line endings, whitespace-only blank
lines and redundant EOF blank lines. All report text, verdicts and provenance
fields are unchanged; the original runner responses remain in the audit records.
