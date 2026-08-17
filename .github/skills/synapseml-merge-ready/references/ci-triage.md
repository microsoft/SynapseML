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
the owner issue or repair it when tightly coupled.

## Infrastructure failure

Repository setup, capacity allocation, authentication, TLS, artifact download,
agent loss, or publishing failed before relevant tests ran.

Action: cite the log line proving where execution stopped, verify no test result
was produced, and rerun. Repeated infrastructure failures still block readiness
when they prevent required evidence.

## False-green patterns to reject

- A job succeeded because the affected tests were skipped.
- A helper test passed while the transformer/request path remained broken.
- Provider/device discovery succeeded without executing real kernels.
- A custom native or local jar worked although the published artifact lacks it.
- Aggregate CI is green while a required branch replay never ran.
- A test count increased but the requested edge case has no assertion.
- Commit ancestry is correct but a merge conflict discarded target content.
