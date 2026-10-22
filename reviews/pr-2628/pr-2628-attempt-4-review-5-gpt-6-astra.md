# PR 2628, attempt 4, round 5

## Scope and result

Direct test-coverage review by `gpt-6-astra`. Gemini attempts failed before
producing a review, so this is a documented fallback, not three-family coverage.
The reviewed working tree is based on the previous PR revision and canonical
master `681bd96990c421de3b91d2b1bf8f8f470764199d`. Final commit and hosted CI
validation remain separate gates.

No additional unresolved test-coverage defect was found in the bounded paths
below. Round-3 resolutions still require independent closure.

## Coverage checked

- Public plans and evidence retain strict schemas, source bindings and producer
  outcomes. Deeply nested JSON now has real refusal regressions for bootstrap
  input, compressed evidence, external profiles and Maven admission.
- Bootstrap tests cover preview versus apply, required approval, current
  canonical candidates, website and Azure checks, missing/conflicting tags,
  annotated-tag preservation and atomic-push failure. Dispatch tests distinguish
  an ambiguous timeout from a rejected request without echoing its payload.
- Recovery exercises the real CLI with local Git fixtures. It rejects wrong
  versions, missing or linked guides, shallow history, and snapshots committed
  or deleted in current HEAD ancestry. Staged and untracked new snapshots work.
  A primary snapshot on another branch does not block either Spark 4 port.
- Snapshot finalization covers positive removal, idempotence, missing input,
  duplicate headings, residual moving references, symlinks and hardlinks. Source
  guides and older snapshots remain unchanged.
- Website checks retain strict production publication locks. Preview mode only
  permits the documented lag and cannot select Pages upload or deployment.
  New source and prepared guides cannot invent R ZIP or DBC downloads. Historical
  R archive assertions remain in place.
- Release-note admission uses real local Git graphs for merge, squash and
  rebased-commit integration. Negative cases cover open or unmerged candidates,
  forks, changed heads, wrong branches or bases, malformed merge identities,
  noncanonical origins and tag mismatches. API response bounds, projected fields,
  pinned host/version, timeout handling and CLI wiring have direct assertions.

## Corrections made during validation

The first recovery implementation inspected every ref and reflog. Real sibling
branch regressions showed that this rejected a new port snapshot after primary
preparation committed the same version. Restricting the check to current HEAD
ancestry preserves historical protection without conflating runtime branches.
That regression moved from four failures and four passes to eight passes.

Public workflow dispatch also needs an explicit GitHub host. Regressions now set
an unrelated `GH_HOST` and require canonical public preview/apply destinations.
The first run had two failures and one pass; the corrected bootstrap suite passed.

Two oversized test parameter IDs exceeded Windows' environment-variable limit
before their assertions ran. Short explicit IDs fixed the fixtures without
reducing the payload sizes or weakening the parser assertions.

## Evidence and limits

- Combined release and CI-helper baseline: 1,132 tests and 63 subtests passed
  with JDK 11 selected on PATH. This predates the final round-3 changes.
- Recovery suite: 270 native tests passed. Real preview and idempotent recovery
  also passed on all three prepared candidates after restoring full ancestry.
- Each runtime's corrected website passed 37 checks and a full build. Hashes
  confirmed that 42 historical installation/R guides per runtime were unchanged.
- Round-4 independent closure passed 42 Python and 31 Node checks with no skips.
- Round-3 parser, inventory and dispatch changes passed 643 affected tests on
  Linux. The host-binding follow-up passed all 51 bootstrap tests.
- Notes, workflow and public-documentation checks passed 101 tests before the
  final transport timeout/version-pin additions; 16 focused checks passed after
  those additions. Final frozen-tree validation is still required.

A read-only GitHub probe confirmed that a past squash-merged PR remains
discoverable through its original head commit. Local graph tests do not replace
current-head hosted CI, a real bootstrap preview, signing approval, publication
receipts, package inspection or consumer validation.

## Final validation and release hold

The frozen release and CI-helper run passed 1,181 tests and 63 subtests, with one
opt-in SBT case skipped. That real SBT case passed separately on the clean public
feature tree with JDK 11 selected on PATH. The clean tree also passed all 270
version-bump/recovery tests. Subsequent documentation-only ordering changes
passed 14 public-guide checks, including a regression that places consumer
validation and merge-rule checks before tag approval.

A separate pre-publication investigation found an existing Python distribution
gap. The primary branch and Spark 4 branches contain different handwritten
Python code. An exact source-function replay using Spark 4.1-style image bytes
fails on primary and passes on the port; this is not a built-wheel test.
The aggregate wheel task uses the current branch's code, so successful port CI
does not establish compatibility of the primary PyPI wheel.

The release remains blocked on selecting and validating a Python distribution
for the ports. No approval for additional publication outputs was inferred.
No production tag or package was created, and these results do not establish
production readiness.
