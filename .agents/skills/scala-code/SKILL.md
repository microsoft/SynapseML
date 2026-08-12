# Scala Code Skill

Use this skill for any Scala code change in this repository.

## Objectives
- Keep Scala changes correct, minimal, and production-safe.
- Prevent CI/CD breakage by validating style and compilation before completion.
- Ensure every behavior change is covered by tests.
- Scala code should be optimized to be efficient and maintainable, following existing patterns and practices in the codebase.

## Scala best practices
- Follow existing SynapseML patterns (`DefaultParamsReadable`, `DefaultParamsWritable`, `Wrappable`, `SynapseMLLogging`).
- Keep business logic in Scala (not generated Python wrappers).
- Do not edit generated files under `target/`.
- Preserve license headers and existing package structure.
- Prefer small, focused changes and reuse existing helpers/traits.
- Avoid introducing flaky tests or network-dependent behavior unless already required by the suite.
- To get around scalastyle issues, don't just use `// scalastyle:off` or `//scalastyle:ignore`, but instead fix the underlying issue or refactor to avoid it. Unless, the refactor results in a more complex code structure, in which case, it may be acceptable to disable the specific scalastyle rule for that line or block of code, but this should be done sparingly and with justification.

## Required validation steps (must run)
Run these commands before finishing Scala changes:

1. Scala style checks:
   - `sbt scalastyle "Test / scalastyle"`
2. Scala compile checks:
   - `sbt compile`
   - `sbt test:compile`
3. Relevant tests for touched modules/files:
   - Example: `sbt "core/testOnly *SuiteName*"` or `sbt "cognitive/testOnly *SuiteName*"`

If a command fails, fix the issue and rerun until passing.

## Testing requirement for code changes
- Any Scala code change must be accompanied by tests (new tests or updates to existing tests).
- Bug fixes must include a regression test that fails before the fix and passes after.
- New logic/branches should include coverage for success and failure/edge cases where practical.
- Keep tests deterministic and aligned with current module test conventions.

## Completion checklist
- [ ] Code follows existing Scala/SynapseML conventions.
- [ ] Style checks pass.
- [ ] Compile checks pass.
- [ ] Relevant tests pass.
- [ ] Scala code changes include corresponding tests.
