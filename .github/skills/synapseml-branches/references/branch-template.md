# Branch reference template

Keep only rules that change how future work on this branch is done.
Use the headings that apply:

1. **Purpose and sources.** What targets the branch and where its configuration
   is defined. Link the shared source map rather than copying versions.
2. **Compatibility boundaries.** Deliberate differences from master or sibling
   ports, why they matter, and what must survive a sync.
3. **Runtime and validation.** Supported environments, intentional coverage
   limits, and checks specific to this branch.

Link shared sync, testing, and CI guidance instead of repeating it.
Keep PR/commit/build history, benchmark snapshots, and current failure status
in PR descriptions or review artifacts. Do not add a running incident log.
