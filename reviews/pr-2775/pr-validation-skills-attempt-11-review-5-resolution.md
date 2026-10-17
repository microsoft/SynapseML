# Driver resolution: pass 11, round 5

Accepted. Added offline cases for a hidden `startup_failure`, queued run,
in-progress run, and waiting approval, all with an otherwise successful check
rollup. Each case asserts the applicable workflow list, complete collection,
blocked remote/completeness gates, and blocked merge state.

The retry-supersession test now first proves that the old failed run alone
blocks the gate, then verifies that the newer successful run clears it across
two result pages.

`python -m pytest tools\ci\tests\test_get_pr_readiness.py -q --tb=short`
passed: 15 tests, 26 subtests. Black 22.3.0 and `git diff --check` passed.

Original reviewer prose is retained, with these terminology clarifications:
round 1's "aggregate comment" means the aggregate check; its active-Azure path
keeps waiting rather than immediately returning `azure_completed`. Round 2's
`attention_required` mismatch is between GitHub and Azure conclusions, not a
SHA mismatch. The watcher test module already existed and was expanded.

Both pass 11 findings were addressed together before generating pass 12.

## Publication record

- PR: microsoft/SynapseML#2775
- Source publication commit: `66d08e29cc9baf9d74a71f69ce7f59c6b9f1d299`
- Reviewed input manifest SHA-256: `4af6a3a53f6b1b6ba7a52f18e640eb6ef8f98717bee36ac8af18d70fea1852b5`
- Reviewed diff SHA-256: `bffec5c72c2886366b0342d7b080da336e45895af8e222e4bc90b7ddf0d3aeb6`
- Original artifact SHA-256: `6e96ae9fbb83c1e6efd4cf7438db066aaf369c2ff60855156a992ad6329c06d3`
- Dispatch mode: manual prompt fallback; no observed CLI receipt is claimed.
