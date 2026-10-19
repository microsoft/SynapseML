# Excluded review: attempt 13, round 1

The original runner artifact contains a local workstation path and remains
private with its response and execution receipt. It is not rewritten or counted
as a clean review.

- Runtime reviewer: `claude-opus-5.5`
- Outcome: `issues_found`
- Original artifact SHA-256: `0b45286f534a58e40f400403615d0af4b5e993dc6a90901be8d13d539e7eed67`
- Input SHA-256: `8f84019b55b1248ab856560555719883c9b16cb95b3cfdc15ea8ca5d86234b86`
- Source manifest SHA-256: `333965a1fa7e4814a894e476e623d4ab77007b24716b58e6437e3d8f95a45efe`

The six findings concern fork-run associations, classic-protection read
permissions, Windows executable lookup, malformed Azure fields, verified safety
inputs, and the PowerShell runtime requirement. They remain open pending fixes
and a new complete review pass.

## Driver resolution before attempt 14

All six findings were accepted:

- Empty fork associations now require matching repository/branch plus a unique
  PR at that SHA, including closed-PR history. A live query of this PR returned
  complete evidence without collection errors.
- Classic-protection read failures explain the Administration read requirement;
  the trigger remains blocked rather than bypassing missing policy evidence.
- Shared absolute executable resolution excludes current-directory and relative
  PATH entries. A Windows decoy regression reproduces the old lookup failure.
- Nested Azure fields are validated, nullable timeline fields are supported,
  and malformed evidence produces a sanitized error.
- The verifier covers owner, pipeline, and template inputs and reports its scope.
- The script and documentation explicitly require PowerShell 7.2 or newer.

Validation: 92 Windows helper tests and 143 subtests passed; the full Linux
CI-helper suite passed 310 tests and 109 subtests. Its 20 PowerShell skips passed
on Windows. These fixes still require a clean full-patch review and publication.
