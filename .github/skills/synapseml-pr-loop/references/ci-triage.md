# CI triage

Do not rerun a failed pipeline blindly. Preserve the job URL and first determine
which category the failure belongs to.

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
agent loss, or publishing failed before relevant tests ran.

Action: cite the log line proving where execution stopped, verify no test result
was produced, and rerun. Repeated infrastructure failures still block readiness
when they prevent required evidence.

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
