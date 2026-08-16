# AGENTS.md

Entry point for coding agents working in this repository. Humans should start
with [CONTRIBUTING.md](CONTRIBUTING.md).

## Read this first

1. **This file** — how the repository is branched and which rules are universal.
2. **`AGENTS_<branch>.md`** — if you are on any branch other than `master`, read
   it before changing anything. It records what diverges on that branch and why.
3. [`.github/copilot-instructions.md`](.github/copilot-instructions.md) —
   architecture, the code generation pipeline, Scala patterns, and style rules.

## Branch model

| Branch | Purpose |
| --- | --- |
| `master` | Mainline. The Spark 3.x line, and the source of truth for everything not version-specific. |
| `spark4.0` | Spark 4.0 port. See `AGENTS_spark4.0.md`. |
| `spark4.1` | Spark 4.1 port. See `AGENTS_spark4.1.md`. |

Target `master` for ordinary work. Target a `spark4.x` branch only for changes
that exist *because of* that Spark version.

### Where instructions live

This file and `CONTRIBUTING.md` are meant to be **byte-identical on every
branch**, so they must stay free of version-specific facts — no Spark, Scala,
Java, or Python version numbers, and no paths containing a Scala version such as
`target/scala-<version>/`. Anything version-specific belongs in
`AGENTS_<branch>.md`.

If you find yourself wanting to add a version number here, that is the signal
that it belongs in the branch file instead.

Keeping the shared files identical is not just tidiness: it means a
`master` → branch sync merges them cleanly instead of producing a conflict that
someone has to resolve by hand on every sync.

## Syncing master into a Spark 4 branch

These branches are kept current by **merging** `master` in, not by rebasing.
Rebasing discards the accumulated conflict resolutions, which are the real
content of these branches.

The governing rule when resolving a conflict:

- Keep the branch's side where the difference exists **because of** the version
  upgrade.
- Take master's side otherwise.
- **Combine** where both sides changed for different reasons. This is the case
  people get wrong most often — a file can carry both a master bugfix and a
  branch-specific adaptation, and taking either side wholesale silently drops
  the other.

To tell which case you are in for a file, compare three versions: the merge
base, master, and the branch. If `git diff <merge-base> master -- <file>` is
empty, master never touched it and the divergence is deliberate branch work.

### Verifying a sync actually landed

Commit reachability is **not** sufficient evidence. `git log master ^<branch>`
being empty only proves the commits are ancestors; a conflict resolution can
still have discarded master's side while leaving the merge commit in place.

Check content instead: for each file master changed, confirm the lines master
added are present in the branch, then classify every difference as either an
intended version-driven divergence or a dropped change. Expect a large number of
legitimate hits — record why each one is intentional rather than skimming past
it.

## Rules that apply on every branch

- **Python wrappers are generated from Scala.** To change a feature, change the
  Scala source. Never edit generated output under a module's `target/`
  directory; it is overwritten on every build.
- Hand-written Python under `src/main/python/` is only for genuine overrides.
  Do not add an `__init__.py` that re-lists classes codegen already exports —
  codegen emits `import *` for every generated module, and a hand-maintained
  list goes stale silently. See `AGENTS_spark4.0.md` for a worked example of
  this breaking CI.
- A new Scala stage needs `Wrappable` (or it gets no Python wrapper),
  `SynapseMLLogging` with a `logClass` call, and a companion object extending
  `DefaultParamsReadable` (or model loading fails).
- Scalastyle enforces the Microsoft copyright header, a 120-column limit, and
  an 800-line file limit.
- Python is formatted with **black pinned to 22.3.0**. A newer black reports
  spurious failures.
- Use the DataFrame/Dataset API. Do not introduce RDD-based code — beyond style,
  it does not work under Spark Connect or Databricks Unity Catalog standard and
  serverless modes.

## Working effectively

- Prefer measuring over asserting. Where a claim can be checked with a command,
  check it, and prefer the smallest command that covers the change.
- Sanity-check negative results before trusting them. A search that returns
  nothing because a tool is missing looks exactly like a search that returns
  nothing because the thing is absent; confirm with a case you know should
  match.
- Record *why* a divergence exists at the point it is introduced — in a comment
  next to the change and, if it is durable, in `AGENTS_<branch>.md`. A pin with
  no rationale gets "helpfully" reverted by the next sync.
