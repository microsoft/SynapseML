# Exact CI evidence

Use helpers from the [verified guidance source](trusted-guidance.md).
These collectors are read-only by default and do not grant CI, workflow-approval,
retry, or merge permission. Keep raw evidence local; inspect it before sharing.

## GitHub snapshot

Use PowerShell 7.2 or newer to run `Get-PrReadiness.ps1 -PullRequest <number>`
and retain the JSON. It collects
the exact head and live target-tip SHAs, compares those immutable revisions, then
re-reads the PR and target to detect movement during collection. `prBaseRefSha`
is informational: GitHub's PR `baseRefOid` can lag the branch tip until the next
head push. Missing or malformed revision rechecks block evidence without
claiming that a revision moved. Being behind is an integration gate, not invalid
evidence or a ban on validation-only CI of the current merge. For a cleared pair,
also pass `-ExpectedHeadSha <sha> -ExpectedBaseSha <sha>`.
For head repository identity, the helper accepts `headRepository.nameWithOwner`
or combines `headRepositoryOwner.login` and `headRepository.name` when older
GitHub CLI JSON omits the full name. Missing identity still blocks evidence.

The snapshot includes paginated workflow runs, including `action_required`
and waiting runs that have no visible job checks yet. It retains the newest run
per workflow/event on the head, scoped to this PR before selecting the newest
PR-triggered run. Fork runs often have an empty `pull_requests` array; the
collector verifies an entry's base repository URL and head SHA before using
its PR number. Missing or foreign entries use the same fallback as an empty
array: matching head repository/branch and a paginated lookup
proving this is the only PR from that branch, including closed PRs even when
their recorded head has moved. Reused fork branches with multiple PRs require
manual run association when GitHub omits it; current head SHAs alone do not
prove historical ownership. There is no helper flag for supplying that manual
assessment, so missing or ambiguous identities leave collection and the guarded
trigger blocked. GitHub describes these entries as matching open PRs, not necessarily
the triggering PR; assess trigger provenance separately when a branch or
revision is shared.
`run_attempt` identifies retries. Do not approve
these workflows just to clear the helper. Each approval needs its own applicable
authorization and safety review.

Effective rulesets and enabled classic branch protection supply exact required
check names and approval requirements. The SynapseML Azure aggregate must match
`microsoft.SynapseML` exactly, including case; a successful matrix leg does not
substitute for it. Rule-derived check names are also case-sensitive.
`-RequiredCheck` adds case-insensitive check-name prefixes; it cannot remove
the Azure aggregate requirement. Every matching check must succeed, not merely
appear in the rollup.
REST evidence failures and empty responses produce `collectionErrors`, not an
empty policy. Comparison and final PR-recheck failures also leave a blocked
snapshot. The initial PR read and paginated review queries must succeed to
construct a snapshot; failures there terminate collection.
Classic protection inspection requires repository Administration read permission.
If unavailable, human approval and complete readiness remain unknown, and the
guarded `-RunPipeline` trigger stays blocked. Use an authorized identity rather
than bypassing the policy read or assuming the branch summary contains all rules.
Required skipped/neutral checks remain unproven execution, even if GitHub accepts
them for merging. Check issuer identities remain a manual gate.
Copilot auto-review configuration is covered by the current-head automated-review gate.
Other effective rule types, such as required workflows, code scanning,
deployments, or a merge queue, appear in `unevaluatedRules` and
`readiness.unassessedGates` and block the automatic engineering-clear signal.
Only deletion and non-fast-forward restrictions are ignored as inapplicable
to an ordinary PR merge; unknown future rule types require assessment too.
Enabled classic protection beyond checks, reviews, and conversation resolution
also blocks that signal and appears in `readiness.unassessedGates`, including
signature requirements, locked branches, linear history, and push restrictions.
Classic metadata, admin enforcement of existing rules, force-push/deletion
permissions, branch creation, and fork-sync permissions add no separate merge gate.
With no review decision, zero required approvals mean `humanApproval=not_required`
only when all collected review rules explicitly require zero and have no
code-owner, last-push, or named-reviewer requirement. Otherwise it remains unknown.

| Output | Meaning |
| --- | --- |
| `readiness.evidenceComplete` | Queried sources are readable and not truncated; the recorded revisions stayed consistent |
| `readiness.remoteEngineeringGatesClear` | Observable integration, reviews, checks, and workflow gates are clear |
| `readiness.engineering` | `blocked`, or `requires_manual_evidence` for behavior, local validation, complete review, and actual CI tests |
| `readiness.humanApproval` | `approved`, `pending`, `not_required`, or `unknown`; independent of engineering |
| `readiness.merge` | `blocked`, or `requires_engineering_evidence`; never an automated merge authorization |
| `completeness.pipelineRunComment` | Posted trigger receipt: `id`, `html_url`, and `created_at` for the watcher's kickoff; otherwise `null` |
| `completeness.complete` | Compatibility alias for remote engineering gates only, not full readiness or human approval |

`-WaitForReview` only waits for automated review and required checks to appear.
The explicit `-RequestReview` fallback accepts exactly one PR, which must be open
with stable revision evidence; unrelated policy/workflow collection errors do
not suppress it. Multi-PR requests fail before any request is posted; read-only
snapshots still support multiple PRs.
Invalid revision evidence stops a not-yet-attempted request with an error.
After the one permitted attempt, later polls remain read-only and report any
revision errors in the snapshot instead of aborting or requesting again.
Requesting a review
does not clear those other gates or authorize CI.

### Comment-trigger gate

Apply the [Azure scope exception](../SKILL.md#7-run-and-triage-full-ci) before
requesting a run. A missing aggregate alone does not make an exempt diff require
Azure validation.

A separately authorized `-RunPipeline`
requires one PR and both expected SHAs, refuses `-WaitForReview` and
`-RequestReview`, and only requests an absent Azure aggregate (not a missing
CLA or unrelated rule check).
Unlike a review notification, CI executes PR code and may access protected
resources. The helper deliberately requires complete collection before triggering,
not only stable revisions; known failing checks do not make collection incomplete.
The helper's error lists the evidence gaps. Run without `-RunPipeline` to inspect
the snapshot.
`pipelineRunRequested` records the POST's exit status, independently of receipt
parsing; it does not prove that a build queued. If the receipt is missing or the
POST fails, inspect existing comments and builds before retrying: the server may
have accepted the comment even without a usable response.

For an authorized requeue the helper cannot request
because the aggregate already exists, a hand-posted `/azp run` requires a fresh
read-only snapshot with both expected SHAs matching the cleared head/base pair
and `readiness.evidenceComplete=true`. Recheck existing builds first and post
only once under the same authorization and protected-resource contract.

With incomplete evidence on either comment route, obtain the missing evidence
or, after independent assessment of those gaps, use a separately authorized
pinned-queue route under the
[CI delegation contract](../../synapseml-external-contributor-review/references/ci-delegation.md).
If evidence or permission cannot be established, leave CI blocked; do not bypass
the helper with an unguarded trigger comment.
This stale-request guard does **not** eliminate the comment-trigger race or
waive the [protected-resource contract](../../synapseml-external-contributor-review/references/ci-delegation.md).
Use the Azure watcher for completion.

## Azure merge and named tests

Use [collect_ci_evidence.py](../scripts/collect_ci_evidence.py) with an explicit
build ID and expected head, base, and synthetic merge SHAs. For current-head CI,
take the build ID from the details URL of that head's `microsoft.SynapseML` check.
Without that check, find the Azure run for definition `17563` and
`refs/pull/<number>/merge`. Use the merge SHA recorded for the cleared revision
pair, or the run's `sourceVersion`; the collector verifies its ordered base/head
parents. Do not substitute the live GitHub merge ref, which is recomputed when
the target or head moves. For historical inspection, recover the base and head
from that recorded pair or the merge commit's first and second parents,
respectively; historical evidence is not proof of current-head readiness.

```text
python -E -s -B <collector> --pull-request <number> --build-id <id> --head-sha <sha> --base-sha <sha> --merge-sha <sha> --expected-test "<exact-run-name>::<exact-test-name>"
```

Repeat `--expected-suite` and `--expected-test` as needed. Suite means the exact
Azure test-run name, not a job label or a substring of a Scala/Python class.
Find it in the published results rather than assuming the publisher uses a
class name or a unique run title.
`tests.availableRunNames` lists the fetched run names, including nonmatching
ones, to distinguish a selector typo from missing publication without rerunning CI.
`--expected-test` checks only the named test;
add `--expected-suite` to check every result in that run name. Name the new regression explicitly. An
increased count alone cannot prove that regression was discovered.

The collector verifies the GitHub merge's ordered target/head parents and the
Azure pipeline definition, repository, build ID, source branch, source version,
and queue reason. Only the SynapseML pipeline definition (`17563`) is accepted;
`pipelineDefinition` records its ID and current name. Another pipeline's jobs
cannot substitute for this pipeline's validation, even on the same merge SHA.
`pullRequest` proves trigger-based queueing; `manual` is disclosed, not rewritten
as PR CI. Neither a manual build nor this collector repairs a missing required
GitHub status.
The scope is the explicitly supplied revision pair, so historical builds remain
inspectable. Match that pair to the live readiness snapshot before using it as
current-head evidence. The collector rechecks the build/timeline after reading
tests; movement leaves `collectionStable` false and cannot pass validation.

It reads the build timeline and paginated test runs, then fetches detailed
results under the requested run names. Run/build/result IDs and job-attempt metadata
must agree with a real timeline Job, even on the first attempt. Historical
results remain visible but do not satisfy a current
retried job; successful jobs still on attempt 1 retain their evidence.
For each explicit `--expected-suite`, every job that published it must publish current
results. Named expected tests must remain present in each job that previously
published them, without requiring unrelated shards to run them. Another job's success cannot mask a
missing retry publication. `expectedPublications` and `expectedTestPublications`
list these inferred job/suite and job/test pairs; audit the pipeline for jobs
that never published any run.
Historical and unrelated `publicationErrors` remain diagnostic. Completeness
failures block the test gate for current runs selected by `--expected-suite`
or containing a named expected test. All inspected results remain visible in
`runs`; only selected results contribute to current counts and outcomes.
Missing, empty, incomplete, failed, or unexplained skipped selected results block success.

Use `--allow-skipped "Suite::Test"` only for an exact, documented intentional skip
whose reason is recorded in the checkpoint. It cannot exempt a named
`--expected-test`. The JSON keeps all outcomes and exemptions visible.
The scope is `expected_suites_and_tests_only`: unrelated tests sharing a run
name do not become required unless that run name is an explicit expected suite.
Collection has a ten-minute total deadline; expiry leaves evidence unresolved.
Each blocking HTTP response read uses the remaining budget, so streaming bytes
do not reset the deadline. Expiry closes the response and its socket reader.
The original deadline is rechecked after the final collection; finishing at or
after it is an error even when all collected jobs and tests passed.
HTTP failures also recheck that deadline: expiry is reported as a deadline
error, while a socket timeout with budget remaining is reported as a query
timeout. Diagnostics never include the underlying response or error text.
On Windows, timed-out CLI batch launchers are stopped with their process tree;
cleanup can take up to fifteen additional seconds. A cleanup failure reports the
process ID for inspection before retrying. CLI output, including access tokens,
stays in memory.
Jobs must succeed or have an explicitly documented conditional skip for
`jobGatesPassed` and `validationPassed`. For example, verify fork/path conditions
in the effective pipeline, then pass `--allow-skipped-job <exact-timeline-identifier>`
for each intentionally omitted job. `skippedJobs`, `allowedSkippedJobs`, and
`unresolvedJobs` remain visible; an unexplained skip still blocks. This cannot
replace missing expected suite/test evidence. Timeline warnings, errors, and
untyped issues block the automatic job gate even when Azure reports success.
`issueCounts` preserves types and counts without messages, including counts
reported without issue details. Each job includes issues from its timeline
ancestors; shared Stage/Phase issues therefore affect each descendant job.
Inspect tasks/results and record an explicit
disposition under [CI triage](ci-triage.md); the collector does not automatically
waive these issues or label that manual disposition a passing collection.
Azure may represent an unstarted conditional job only as a Phase or Stage.
Leaf containers are included in the same gates with their `timelineType` and
exact identifier; they cannot provide test-run attempt evidence.
Before a build completes, a missing or empty timeline yields `timelinePending`
with no jobs and nonpassing job gates. Completed builds still require job
evidence. Authentication errors and malformed timeline responses remain errors.
Nonzero exit means unresolved evidence, not necessarily a product defect.
Malformed nested Azure fields produce sanitized error evidence rather than an
uncaught traceback; omitted nullable timeline labels/issues remain supported.

Timeline failure-stage hints separate setup before tests, test execution,
publication after a successful test step, and hosted-agent loss. They are
diagnostic hints, not proof of an infrastructure cause or of passing tests.
Setup steps such as dependency installation and test-credential retrieval
are not test execution merely because their display names contain "test".
Correlate actual published tests and inspect the failed task's logs locally.
Publication failure still blocks its required gate even when tests passed.

Authentication uses the existing Azure CLI login. Tokens stay in memory, requests
use only the fixed SynapseML Azure project, and redirects are refused. CLI stderr,
API error bodies, and timeline issue messages are not echoed. Do not add tokens
to arguments, evidence files, comments, or fixtures.
