# CI triage

Do not rerun a failed pipeline blindly. Preserve the job URL and first determine
which category the failure belongs to.

## Waiting for Azure Pipelines

Trigger CI only with explicit authorization. For external PRs, first recheck
the exact head using trusted safety guidance. Otherwise report missing CI as a
blocker and remain read-only.

After an authorized `/azp run`, confirm that the current-head build queued. Record its build
ID, PR head SHA, and the trigger comment's `created_at` as its kickoff time.
For a manually queued run without that comment, use Azure's `queueTime`, not
the time an agent first notices the build. Then run
[watch_azure_pipeline.py](../scripts/watch_azure_pipeline.py):

```text
python <watcher-script> --repo microsoft/SynapseML --pull-request <number> --head-sha <full-sha> --build-id <id> --kickoff-at <ISO-8601-time>
```

- Launch this command once through the terminal tool's attached
  asynchronous/background mode. Keep its job ID, confirm the startup message,
  and continue independent work. Do not detach it from the session unless asked.
- The process checks the named Azure build's GitHub status every **10 minutes
  (600 seconds)**, with a deadline **120 minutes after that run's kickoff**.
  Late starts and watcher restarts only get the remaining time.
  `--timeout-minutes` may shorten that limit, not increase it.
- It prints only startup and final JSON. Let the process sleep without model
  calls, subagents, recurring prompts, or short polls of the job's output.
  Read its result when the terminal tool sends a completion notification.
- Exit zero means the named check succeeded. Failure, timeout, query errors,
  or a changed PR head are nonzero results.
- The watcher queries only `microsoft/SynapseML`. The optional `--repo` flag
  accepts that name case-insensitively; a different repository is rejected
  before any query, even if it could replay a real Azure build URL.
- The watcher accepts only HTTPS build-results URLs for the trusted SynapseML
  Azure project, on `dev.azure.com/msdata` or `msdata.visualstudio.com`.
  Both the project GUID and its verified `A365` alias are accepted.
  A matching check name or numeric build ID alone is not proof of Azure origin.
  Unexpected hosts, projects, or paths are errors, not successful checks.
- Build IDs must fit Azure's positive `int32` range, `1` through `2147483647`,
  in both CLI arguments and result URLs. Oversized IDs produce an explicit
  error, not a replacement handoff or an unhandled conversion failure.
- A newer build returns `outcome: replaced` with its ID and URL. Confirm its
  kickoff time, then launch one new background job for that run. Its two-hour
  window starts at the new kickoff, not when the replacement is noticed.
  Do the same after a head change once its new run is confirmed.
- On timeout, report the build link and leave CI unresolved. The monitor does
  not cancel or trigger builds. Rechecking the same run never resets its clock;
  only a genuinely new run gets a fresh kickoff-based window.
  Even when no query fits before expiry, the timeout result includes the
  canonical link for the validated build ID without extending the deadline.
- After any monitor exit, recheck the current head and build, including for a
  new run triggered near the old cutoff. Inspect Azure jobs and published test
  results before declaring readiness; GitHub status can lag Azure.
- Do not trigger duplicate runs just because a build is pending. If authorized
  work requires a new run, record its new kickoff and replace the old monitor.
- `Get-PrReadiness.ps1 -PollSeconds` controls its wait for automated review and
  required checks to appear, not Azure pipeline completion. Keep that separate
  from the 10-minute pipeline-monitoring cadence.

The watcher regressions live in `tools/ci/tests/test_watch_azure_pipeline.py`
so the existing `CIHelpers` job runs them. For a focused local run, use
`python -m pytest tools/ci/tests/test_watch_azure_pipeline.py -q`.

## Product defect

The changed code compiled or ran and produced an incorrect result, crash,
resource leak, performance regression, or incompatible API/schema.

Action: reproduce locally or in the closest environment, add/strengthen the
regression test, fix, and rerun targeted plus full CI.

## Test defect

The product behavior is correct but the test has a race, wrong assumption,
unsafe cleanup, overly strict tolerance, environment-order dependence, or does
not test the public path.

Action: fix the test without weakening the requirement. Demonstrate the product
behavior separately.

## Baseline/pre-existing failure

The same failure occurs on the target SHA or is unrelated to every changed path.

Action: collect comparable target/head evidence. Do not silently ignore it; link
the tracking issue or repair it when tightly coupled.

## Infrastructure failure

Repository setup, capacity allocation, authentication, TLS, artifact download,
agent loss, or publishing failed independently of product behavior. Setup can
fail before tests run; test-result or coverage publication can fail after the
tests pass.

Action: cite where execution stopped and preserve any completed test evidence.
Do not claim execution for an abandoned test step, or dismiss a required
publication failure because the tests passed. Rerun the affected gate. Repeated
infrastructure failures still block readiness when they prevent required evidence.

## Reading job results correctly

Azure Pipelines job results are not binary. A job can end as `succeeded`,
`succeededWithIssues`, `failed`, `canceled`, or `skipped`, and a triage filter
that accepts only `succeeded` will report phantom failures.

`succeededWithIssues` most often comes from a non-gating task -- dependency
cache upload/download, TLS errors, artifact publishing -- while every test in
the job passed. Confirm by opening the job and finding which task raised the
warning, then read the published test results rather than trusting the job
badge in either direction:

- If the warning is from a non-gating task and the test run is complete and
  green, the job passed. Do not rerun it.
- If the warning is from a task that runs or publishes tests, treat it as a
  real failure until the test counts prove otherwise.

Job-level status also cannot tell you whether the tests you care about ran.
For any claim about a specific suite, read the per-test results from the test
run the job published, and compare them against a prior build. Comparing
per-test outcomes across builds is the only reliable way to tell a real fix
from a coincidence: a fix that changes nothing will leave the same tests
failing in the same way, which a green/red job summary will not reveal.

## Attempts, reviews, and dependency provenance

- Check the producer's build timeline as well as the GitHub check. Agent loss
  can leave a GitHub check showing `in_progress` after Azure has completed.
  Record the mismatch; a completed Azure build does not clear a pending required
  GitHub check.
- After a retry, verify which jobs actually advanced to another attempt.
  Preserve successful-job evidence and distinguish repeated results from unique
  tests; do not add attempt totals and call them new coverage.
- An approved automated-review policy can retain an old source commit. Compare
  the reviewed commit with the current PR head. An accepted reevaluation request
  is not proof that a fresh review ran.
- A downstream repair's own green build does not validate a public candidate.
  Record the actual downstream checkout SHA and exact public artifact version.
  If the pipeline selects a target branch, an unmerged repair is not consumed.
  After the dependency merges, validate the new source/artifact pair.
- For release replay, capture the target, patch baseline, and prerequisite
  revisions. A missing baseline and a genuine port conflict need different
  repairs; prerequisites can also be obsolete after a sync. Preserve conflict
  rejection and compilation. Follow `AGENTS.md` approval rules before changing
  release tooling rather than adding a blanket skip.
- Separate service/model failures from client lifecycle errors using comparable
  runs and targeted regressions. Keep the original fixtures and assertions.
  One passing rerun alone does not establish which change caused the recovery.

## After a maintainer merges

Record the landed commit and compare its tree with the validated PR source,
especially after a squash merge. Carry unresolved failures into linked,
scoped follow-up PRs or issues with their original evidence. Merging does not
retroactively turn a failed, skipped, or missing validation gate into a pass.

## False-green patterns to reject

- A job succeeded because the affected tests were skipped.
- The relevant suite was never selected by the matrix and therefore was not
  reported as skipped.
- A helper test passed while the transformer/request path remained broken.
- Provider/device discovery succeeded without executing real kernels.
- A custom native or local jar worked although the published artifact lacks it.
- Aggregate CI is green while a required branch replay never ran.
- A CI/path-filter fix passed because its own diff bypassed the path it changed;
  no representative product patch exercised the workflow.
- A test count increased but the requested edge case has no assertion.
- Commit ancestry is correct but a merge conflict discarded target content.
