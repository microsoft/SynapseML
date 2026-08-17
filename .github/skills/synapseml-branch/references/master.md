# `master`

- Default target for ordinary features, fixes, docs, and repository-wide work.
- Rebase feature PRs onto latest `master`; use a guarded force push.
- Read the current Spark/Scala baseline from `build.sbt`, not this reference.
- Cross-version changes land here first, then flow to port branches by merge.
- Release compatibility replay is additional evidence, not a substitute for
  target-branch validation or a later full port-branch sync.
- Confirm pipeline selection reaches every affected suite. A green matrix can
  silently omit a package or explicitly listed test class.
- Before readiness, check whether `master` advanced and rerun affected checks.
