# Exact CI evidence

Use helpers from the [verified guidance source](trusted-guidance.md).
These collectors are read-only by default and do not grant CI, workflow-approval,
retry, or merge permission. Keep raw evidence local; inspect it before sharing.

## GitHub snapshot

Run `Get-PrReadiness.ps1 -PullRequest <number>` and retain the JSON. It collects
the exact head and live target-tip SHAs, compares those immutable revisions, then
re-reads the PR and target to detect movement during collection. `prBaseRefSha`
is informational: GitHub's PR `baseRefOid` can lag the branch tip until the next
head push. Being behind is an integration gate, not invalid evidence or a ban
on validation-only CI of the current merge. For evidence tied to a cleared pair,
also pass `-ExpectedHeadSha <sha> -ExpectedBaseSha <sha>`.

The snapshot includes paginated workflow runs, including `action_required`
and waiting runs that have no visible job checks yet. It retains the newest run
per workflow/event on the head; `run_attempt` identifies retries. Do not approve
these workflows just to clear the helper. Each approval needs its own applicable
authorization and safety review.

Effective rulesets and enabled classic branch protection supply exact required
check names and approval requirements. The SynapseML Azure aggregate must match
`microsoft.SynapseML` exactly; a successful matrix leg does not substitute for it.
API/permission failures produce `collectionErrors`, not an empty policy.
Required skipped/neutral checks remain unproven execution, even if GitHub accepts
them for merging. Check issuer identities remain a manual gate.

| Output | Meaning |
| --- | --- |
| `readiness.evidenceComplete` | Queried sources are readable and not truncated; the recorded revisions stayed consistent |
| `readiness.remoteEngineeringGatesClear` | Observable integration, reviews, checks, and workflow gates are clear |
| `readiness.engineering` | `blocked`, or `requires_manual_evidence` for behavior, local validation, complete review, and actual CI tests |
| `readiness.humanApproval` | `approved`, `pending`, `not_required`, or `unknown`; independent of engineering |
| `readiness.merge` | `blocked`, or `requires_engineering_evidence`; never an automated merge authorization |
| `completeness.complete` | Compatibility alias for remote engineering gates only, not full readiness or human approval |

`-WaitForReview` only waits for automated review and required checks to appear.
Use the Azure watcher for completion. A separately authorized `-RunPipeline`
requires one PR and both expected SHAs, refuses `-WaitForReview`, and only
requests an absent Azure aggregate (not a missing CLA or unrelated rule check).
This stale-request guard does **not** eliminate the comment-trigger race or
waive the [protected-resource contract](../../synapseml-external-contributor-review/references/ci-delegation.md).

## Azure merge and named tests

Use [collect_ci_evidence.py](../scripts/collect_ci_evidence.py) with an explicit
build ID and expected head, base, and synthetic merge SHAs:

```text
python -B <collector> --pull-request <number> --build-id <id> --head-sha <sha> --base-sha <sha> --merge-sha <sha> --expected-suite <exact-run-name> --expected-test "<exact-run-name>::<exact-test-name>"
```

Repeat `--expected-suite` and `--expected-test` as needed. Suite means the exact
Azure test-run name, not a job label or a substring of a Scala/Python class.
Find it in the published results; name the new regression explicitly. An
increased count alone cannot prove that regression was discovered.

The collector verifies the GitHub merge's ordered target/head parents and the
Azure repository, build ID, source branch, source version, and queue reason.
`pullRequest` proves trigger-based queueing; `manual` is disclosed, not rewritten
as PR CI. Neither a manual build nor this collector repairs a missing required
GitHub status.
The scope is the explicitly supplied revision pair, so historical builds remain
inspectable. Match that pair to the live readiness snapshot before using it as
current-head evidence. The collector rechecks the build/timeline after reading
tests; movement leaves `collectionStable` false and cannot pass validation.

It reads the build timeline and paginated test runs, then fetches detailed
results for the requested suites. Run/build/result IDs and job-attempt metadata
must agree with a real timeline Job, even on the first attempt. Historical
results remain visible but do not satisfy a current
retried job; successful jobs still on attempt 1 retain their evidence. Counts
distinguish current result records from unique job/suite/test identities.
Missing, empty, incomplete, failed, or unexplained skipped results block success.

Use `--allow-skipped "Suite::Test"` only for an exact, documented intentional skip
whose reason is recorded in the checkpoint. It cannot exempt a named
`--expected-test`. The JSON keeps all outcomes and exemptions visible.
The scope is `expected_suites_only`: it is not an audit of every test in the
build. Jobs must succeed or have an explicitly documented conditional skip for
`jobGatesPassed` and `validationPassed`. For example, verify fork/path conditions
in the effective pipeline, then pass `--allow-skipped-job <exact-timeline-identifier>`
for each intentionally omitted job. `skippedJobs`, `allowedSkippedJobs`, and
`unresolvedJobs` remain visible; an unexplained skip still blocks. This cannot
replace missing expected suite/test evidence. Warning-bearing jobs need manual
task/result inspection under [CI triage](ci-triage.md).
Azure may represent an unstarted conditional job only as a Phase or Stage.
Leaf containers are included in the same gates with their `timelineType` and
exact identifier; they cannot provide test-run attempt evidence.
Nonzero exit means unresolved evidence, not necessarily a product defect.

Timeline failure-stage hints separate setup before tests, test execution,
publication after a successful test step, and hosted-agent loss. They are
diagnostic hints, not proof of an infrastructure cause or of passing tests.
Correlate actual published tests and inspect the failed task's logs locally.
Publication failure still blocks its required gate even when tests passed.

Authentication uses the existing Azure CLI login. Tokens stay in memory, requests
use only the fixed SynapseML Azure project, and redirects are refused. CLI stderr,
API error bodies, and timeline issue messages are not echoed. Do not add tokens
to arguments, evidence files, comments, or fixtures.
