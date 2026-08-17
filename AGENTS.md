# SynapseML agent guide

Use this file for repository-wide rules. Human contributors should start with
[CONTRIBUTING.md](CONTRIBUTING.md).

## Start here

1. Check the current branch. On `spark4.0` or `spark4.1`, also read
   `AGENTS_<branch>.md`; it explains intentional branch differences.
2. Read versions from [build.sbt](build.sbt) and
   [environment.yml](environment.yml). Do not copy version numbers into this
   shared guide.
3. Use the narrowest relevant repository skill:
   [Scala changes](.github/skills/scala-code/SKILL.md),
   [local setup](.github/skills/synapseml-local-setup/SKILL.md), and
   [code review](.github/skills/code-review/SKILL.md).

## Non-negotiable rules

- Never edit `target/`; generated files are overwritten.
- Never commit or print credentials, keys, connection strings, or `.env` files.
- Do not add RDD-based implementations. Use DataFrame/Dataset APIs so code works
  with Spark Connect and managed Spark modes.
- Do not rebase or force-push shared `spark4.x` branches.
- Do not add a hand-written `__init__.py` merely to re-export generated classes;
  a stale `__all__` can hide public APIs.
- Keep existing public JVM signatures and serialized parameter shapes unless a
  breaking change is explicitly approved.

Ask before changing [pipeline.yaml](pipeline.yaml), workflows under
[`.github/workflows/`](.github/workflows/), release tooling, or dependency
pins. These changes affect every branch and require CI evidence.

## Branch model

| Branch | Use it for |
| --- | --- |
| `master` | Ordinary features, fixes, and repository-wide changes |
| `spark4.0` | Differences required specifically by the Spark 4.0 port |
| `spark4.1` | Differences required specifically by the Spark 4.1 port |

Land cross-version changes on `master`; port branches receive them by merging
`master`. When resolving a port-branch merge:

- keep the branch side only for version-driven differences;
- take the `master` side for ordinary fixes;
- combine both when each changed the same file for a different reason.

Reachability is not proof that a sync preserved content. Compare the merge base,
`master`, and the port branch for every conflicted file, and verify that
`master` additions remain present.

`AGENTS.md` and [CONTRIBUTING.md](CONTRIBUTING.md) must stay identical across
branches. Put branch-only facts in `AGENTS_<branch>.md`.

## Repository map

| Module | Path | Purpose |
| --- | --- | --- |
| Core | [`core/`](core/) | SparkML foundations, IO, codegen, AutoML, causal and exploratory tools |
| Cognitive | [`cognitive/`](cognitive/) | Azure AI and OpenAI service stages |
| LightGBM | [`lightgbm/`](lightgbm/) | Distributed classifier, regressor, and ranker |
| Vowpal Wabbit | [`vw/`](vw/) | VW integration |
| Deep learning | [`deep-learning/`](deep-learning/) | ONNX Runtime inference |
| OpenCV | [`opencv/`](opencv/) | Image transformations |

All modules depend on `core`; `deep-learning` also depends on `opencv`.

Each module follows `src/main/scala`, optional hand-written
`src/main/python`, and `src/test/{scala,python}`. Follow nearby code before
introducing a new pattern.

## Scala-first API and code generation

Public SparkML behavior belongs in Scala. Classes mixing in
[`Wrappable`](core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/Wrappable.scala)
generate Python wrappers under
`target/scala-*/generated/src/python/synapse/ml/`.

For a new or changed stage, verify:

- companion object extends `DefaultParamsReadable[Stage]`;
- class accepts `uid: String` and has a random-UID no-arg constructor;
- `Wrappable` is present when a Python wrapper is required;
- [`SynapseMLLogging`](core/src/main/scala/com/microsoft/azure/synapse/ml/logging/SynapseMLLogging.scala)
  is mixed in, `logClass(...)` is called, and `fit`/`transform` is logged;
- `copy(extra)` preserves parameters, normally through `defaultCopy(extra)`;
- `transformSchema` matches runtime output;
- save/load and generated wrapper behavior are tested.

Hand-written Python may extend a generated `_ClassName` wrapper when JVM
delegation needs a Python convenience method. Do not move core behavior into
Python.

## Validation

Use the smallest command that proves the change, then expand when the affected
surface requires it. Run SBT with the JDK selected by the
[local setup skill](.github/skills/synapseml-local-setup/SKILL.md), not the
machine default.

```bash
sbt <module>/compile
sbt <module>/Test/compile
sbt "<module>/testOnly fully.qualified.Suite"
sbt <module>/scalastyle <module>/Test/scalastyle
sbt codegen
black --check --extend-exclude 'docs/' .
```

Black is pinned in [pyproject.toml](pyproject.toml); use that version.

Scala tests extend
[`TestBase`](core/src/test/scala/com/microsoft/azure/synapse/ml/core/test/base/TestBase.scala).
Add positive, negative, schema, persistence, and end-to-end coverage where the
behavior warrants it. A helper-only test is not proof that the public
transformer path works.

Tests that require Azure credentials should skip when credentials are absent.
Do not turn a skip into a pass by embedding a secret. Inspect service tests
before running them because some create or delete cloud resources.

## Pull requests and CI

- Use a conventional title: `feat:`, `fix:`, `test:`, `docs:`, `ci:`, or
  `chore:`.
- Target `master` unless the change exists only for a port branch.
- Resolve active and suppressed review findings; document why any finding is
  invalid.
- Trigger Azure validation with `/azp run` where supported. Branch-specific
  exceptions are documented in `AGENTS_<branch>.md`.
- Treat [GitHub Actions](.github/workflows/) as fast checks and
  [pipeline.yaml](pipeline.yaml) as the full build.
- Do not equate green checks with correctness: compare before/after failures,
  inspect skipped tests, and verify the requested behavior directly.
- Rebase feature PRs onto the latest target before final validation. Merge
  `master` into shared port branches instead of rebasing them.

## Keep this file useful

Add only durable, repository-wide guidance that changes an agent's decision.
Prefer a link to the source of truth over copied commands, versions, or long
examples. Put implementation detail beside the code, contributor process in
[CONTRIBUTING.md](CONTRIBUTING.md), and branch-specific facts in
`AGENTS_<branch>.md`.
