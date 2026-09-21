# CI triage

Do not rerun a failed pipeline blindly. Preserve the job URL and first determine
which category the failure belongs to.

## Waiting for Azure Pipelines

After `/azp run`, confirm that the current-head build queued. Record its build
ID and the PR head SHA, then run
[watch_azure_pipeline.py](../scripts/watch_azure_pipeline.py):

```text
python <watcher-script> --repo <owner/repo> --pull-request <number> --head-sha <full-sha> --build-id <id>
```

- Launch this command once through the terminal tool's attached
  asynchronous/background mode. Keep its job ID, confirm the startup message,
  and continue independent work. Do not detach it from the session unless asked.
- The process checks the named Azure build's GitHub status every **10 minutes
  (600 seconds)**, with a **120-minute maximum** including queries and sleeps.
  `--timeout-minutes` may shorten that limit, not increase it.
- It prints only startup and final JSON. Let the process sleep without model
  calls, subagents, recurring prompts, or short polls of the job's output.
  Read its result when the terminal tool sends a completion notification.
- Exit zero means the named check succeeded. Failure, timeout, query errors,
  or a changed PR head are nonzero results. A missing or replaced build is an
  error rather than permission to follow another run.
- On timeout, report the build link and leave CI unresolved. The monitor does
  not cancel the Azure build, queue another run, or restart its own deadline.
  Do not automatically relaunch it to evade the two-hour limit.
- After completion, recheck the PR head and inspect Azure jobs and published
  test results before declaring readiness. GitHub status can lag Azure; this
  monitor is not a substitute for those checks.
- Do not post another `/azp run` during monitoring.
- `Get-PrReadiness.ps1 -PollSeconds` controls its wait for automated review and
  required checks to appear, not Azure pipeline completion. Keep that separate
  from the 10-minute pipeline-monitoring cadence.

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
