# Driver resolution: pass 9, round 5

All three test-coverage findings were accepted. No implementation defect was
reported or production behavior changed.

1. **Unsafe CI triggers:** the offline PowerShell harness now records every
   attempted PR comment before enforcing its fixture expectation. Ten refusal
   cases require a nonzero exit, the appropriate guard message, and no comment:
   mismatched head/base, head/base movement during collection, unreadable rules,
   a closed PR, either missing expected SHA, multiple PRs, and waiting mode.
   Existing successful-trigger and no-trigger cases also assert the side effect.
2. **Non-success evidence:** skipped and neutral conclusions are exercised for
   the exact Azure aggregate and a ruleset-required check. The review's proposed
   `evidenceComplete: false` assertion was not adopted: a valid snapshot of a
   non-successful check is complete evidence, not a collection error. Tests
   assert the unproven check and blocked remote/completeness gates instead.
   Warning Jobs and leaf Phases cannot clear job gates, even with a skip
   exemption. A partially successful build cannot validate despite passing tests
   and stable collection; the successful control still passes.
3. **Redirect wiring:** the test inspects the actual AzureClient opener's
   redirect handlers and passes a 302 response through that opener's error
   dispatch. It requires EvidenceError and no follow-up request. The first test
   run caught an unrealistic case-sensitive dictionary header fixture; replacing
   it with the standard case-insensitive Message header object corrected the
   test without changing production code.

Validation:

- `python -m pytest tools\ci\tests\test_get_pr_readiness.py tools\ci\tests\test_collect_ci_evidence.py -q --tb=short`
  passed: 32 tests, 44 subtests.
- Black 22.3.0 formatted and then checked the two changed test modules: passed.
- `git diff --cached --check`: passed.
- Fresh upstream master remained `0543bf7e5266c587207ae8c28c3b696deeed3ed1`.

The new test content invalidates the pass 9 review fingerprint. A full frozen
pass 10 is required; the five clean pass 9 lenses are not carried forward.

## Publication record

- PR: microsoft/SynapseML#2775
- Source publication commit: `66d08e29cc9baf9d74a71f69ce7f59c6b9f1d299`
- Reviewed input manifest SHA-256: `e30c896294c0217401bfb9b79d1186d36e8373488d8a5a4db99c57a589109609`
- Reviewed diff SHA-256: `152561506548ba7824135deea64a9854b5c4e04fc6f2756040b0647a48db6c13`
- Original artifact SHA-256: `490858881089349c9efcba796e21a2e6f90b4b0ff7f5a0be12a6c7f8cc4aca6e`
- Dispatch mode: manual prompt fallback; no observed CLI receipt is claimed.
