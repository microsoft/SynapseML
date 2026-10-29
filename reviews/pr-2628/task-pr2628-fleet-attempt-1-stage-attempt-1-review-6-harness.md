# Lens 6: polish, hardening and release operator UX

PR: microsoft/SynapseML#2628

- Head: `7bc3019409803a3eaf33e9b68b4f84debb5f4846`
- Base: `861c3a1e14a9511b5604563ff1e3976cefa82e90`, local `upstream/master`
- Reviewer/model: harness-selected.
- Contract: the human-assigned lens and read-only scope take precedence. No nested reviewers, live service operations, credentials, source changes or commits. This file is the only review write.
- Evidence: independently inspected this head and base. No old or sibling review reports were read or used.

## Verdict

**Request changes.** Two P2 documentation regressions affect a new default release. One P3 instruction is stale. The documented approval, bootstrap, ledger and freeze boundaries otherwise agree with the inspected implementation. Missing external access or human approval is not counted as an implementation defect.

## Findings

### F1: P2 - New-version API documentation links have no release publisher

**Changed location:** `scripts\release\README.md:583-588`, particularly the exclusion of generated docs from release mode. Corroboration: `pipeline.yaml:304-314`, `project\ReleaseVersion.scala:11-13`, `build.sbt:213-233`.

The procedure advances the public website to the new version after verifying packages and updating the port publication lock, but it leaves the API documentation links pointing at an output that this procedure never publishes.

**Reproduction without writes:** imported the actual version-bump module and applied its `analyze` and `apply` functions in memory to the checked-in files for `1.1.3 -> 1.2.0`. The resulting `README.md:18` links to `/docs/1.2.0/scala/index.html` and `/docs/1.2.0/pyspark/index.html`. The resulting `website\docusaurus.config.js` sets `version` to `1.2.0`; its navbar URLs at lines 62 and 66 therefore target that same new API documentation prefix, and its Scala footer link at line 98 also advances.

In the release pipeline, the `true` publication branch runs `packagePython` and `publishBlob`, not `publishDocs`. Only the ordinary CI branch invokes `publishDocs`, and the new version resolver forces that branch to use a `-SNAPSHOT` version. `publishDocs` uploads under `version.value`. Thus ordinary CI cannot fill the release-version prefix either. The base pipeline did call `publishDocs` for the tagged publication path, so this is a regression introduced by the changed publication contract, not merely an old bad link.

For a genuinely new version with no preexisting API docs, completing every documented step can publish a website whose API links have no corresponding uploaded files. No HTTP 404 was claimed or queried; this follows from the actual bump output and the available publication paths.

**Fix direction:** either retain an independently tracked, verified published API-doc version when preparing the website, or add an explicitly authorized, source-bound API-doc publication and verification step before switching those links. Do not treat the Maven port lock or a successful Pages deployment as API-doc publication evidence. Add a new-version regression covering the navbar and README API links, not only Maven and DBC examples.

### F2: P2 - Retained Spark 4.0 coordinates still receive new-primary Python instructions

**Changed locations:** `website\src\installArtifacts.js:20-26` and `scripts\bump-version.py:303-315`. Affected user instructions: `website\src\pages\index.js:294-296` and `docs\Explore Algorithms\Deep Learning\Getting Started.md:21-37`.

The new split-version policy correctly retains Spark 4.0's Python package and JVM coordinate, but not every installation instruction follows that split.

**Reproduction without writes:** used the real bump analysis and substitution for `1.1.3 -> 1.2.0`, then evaluated the resulting installation metadata with Node:

```text
primary version: 1.2.0
Spark 3.5 Python: synapseml==1.2.0
Spark 4.0 Python: synapseml==1.1.3
Spark 4.1 Python: synapseml==1.2.0
```

The homepage still renders "All released Python variants use synapseml==1.2.0", although its Spark 4.0 tab correctly specifies `1.1.3`. The deep-learning guide's single Python command becomes `pip install synapseml==1.2.0`, followed by the retained Spark 4.0 JVM alternative `com.microsoft.azure:synapseml_2.13:1.1.3-spark4.0`. Its link to the full installation guide does not explain that choosing this alternative also requires changing the preceding Python pin.

This combination was consistent at the base, where all runtimes used one version. It becomes contradictory after the first default bump. Spark 4.0 is explicitly outside the new release's qualification set, so the procedure has no evidence that the newly prescribed primary wheel works with that retained JVM release.

The changed website tests pass at the current version. They also pass against an in-memory new-version overlay with modeled new snapshots, both in preview mode and with the final Spark 4.1 lock advanced. The specialized-guide checks at `website\test\installDocs.test.js:403-406` check JVM coordinates, not the matching Python instruction; the homepage checks do not reject the shared-pin statement.

**Fix direction:** show runtime-specific Python pins in the homepage prose and provide an explicit retained Python pin for the deep-learning guide's Spark 4.0 alternative. Extend the coupled-guide checks to reject a default new-version release that still gives all runtimes the primary Python instruction.

### F3: P3 - The release skill describes a removed PR replay as advisory

**Location:** `.github\skills\synapseml-release\SKILL.md:85-87`.

The skill says "Spark 4.1 PR replay is advisory", while the operator guide at `scripts\release\README.md:228-230` says that replay has been removed. The inspected master branch reference also explicitly states that master PRs no longer replay onto Spark 4.1, and the reviewed pipeline has no such replay job.

An operator should not look for an advisory result from a job that no longer exists. This does not invalidate the correctly documented requirement for actual candidate validation.

**Fix direction:** state that the old replay is removed and retain the distinction between master validation and required validation of each selected release candidate.

## New-version operator simulation

| Checkpoint | Independently checked result |
| --- | --- |
| Scope and permissions | The runbook separates permission to prepare, dispatch validation, create production tags, publish, sign, publish notes and merge. App Contents read and Pull requests write match the token request; repository branch/tag writes use the workflow identity. Databricks and Blob access are separate prerequisites. |
| Preparation | `python -B scripts\bump-version.py --to 1.2.0 --dry-run` exited 0, reporting 21 files and 129 replacements without changing source. Current and historical snapshots are excluded from the source bump. The changed LightGBM references now have concrete SynapseML anchors. |
| Default and optional plans | Actual `build_plan` and `notes_plan` accepted schema-4 plans for `master,spark4.1` and for explicit `master,spark4.0,spark4.1`. Generated tag families matched the documented runtime selections. No Spark 4.0 binding is required by the default plan. |
| Pre-merge bootstrap | The required branch names match `candidate_branch`. Local preview precedes authorized preview dispatch and approved tag creation; publication preflight is correctly deferred until canonical tags exist. Candidate checks, canonical origin, clean checkout, current target ancestry and runtime/docs checks are present. Classic protection and ruleset inspection remains an explicit human prerequisite, not a claim that bootstrap automatically configures protection. |
| CLI compatibility | The actual argparse implementations accepted 18 documented command shapes across matrix, bootstrap, operations, verifier, guard and offline rehearsal. Each probe stopped immediately after parsing, before any command body or remote operation. |
| Durable handoff | Plan, ledger, persistent claim, exact paths/host, operator exclusion, run IDs, evidence and remaining approval scope are documented. The operations code distinguishes a read-only preview from applied resume and preserves ambiguous submissions. Adoption requires apply and exact-plan approval and does not queue unrelated work. |
| Freeze and source integration | Tagged candidate heads must remain unchanged. The notes guard accepts direct master ancestry or canonical merged-candidate provenance whose final head is the tagged SHA and whose merge result is on master. The runbook correctly requires primary integration before notes and leaves conflict reconciliation to a separate PR. |
| Evidence freshness | In an isolated synthetic-inventory probe of `validate_inventory`, 3,590-second-old evidence passed and 3,610-second-old evidence was rejected as stale. This tested TTL only, not producer completeness or live evidence. |
| Documentation publication | Preview mode permits preparation against the retained publication lock. Strict production mode requires the selected published versions. Workflow deployment is restricted to master. The instructions correctly defer the lock follow-up until verified artifacts and source integration, and explicitly warn that master deployment fails in the interim. F1 remains outside that lock's coverage. |
| Failure directions | Exit-code distinctions agree with `release_ops.main`; timeouts do not cancel builds. Recovery preserves ledgers and claims, requires explicit adoption, rejects tag movement, and distinguishes failed DBC handoff from successful earlier Maven publication. Docs finalization avoids rerunning the version bump or recreating an existing snapshot. |
| Links and public examples | All 12 relative release-skill/runbook/operator-guide links resolved locally. New notebook links use the runtime metadata's tag family. R installation now targets generated package directories containing `DESCRIPTION`, matching `CodegenPlugin.scala` and `RCodegen.scala`, rather than constructing unpublished R archive URLs. |

## Inspected paths

The complete changed documentation and website scope was inspected, not just the agent runbook:

- `AGENTS.md`, `README.md`, and `scripts\release\README.md`.
- `.github\skills\synapseml-release\SKILL.md` and every reference: `agent-runbook.md`, `automation-boundaries.md`, `preflight.md`, `recovery-and-rollout.md`.
- `docs\Get Started\Install SynapseML.md`, `docs\Reference\R Setup.md`, and both changed sections of `docs\Explore Algorithms\LightGBM\LightGBM - Quantile Regression for Drug Discovery (Scala).md`.
- `website\src\installArtifacts.js`, `website\src\pages\index.js`, `website\test\installDocs.test.js`, and `website\test\rSetupDocs.test.js`.
- `scripts\bump-version.py` and the relevant new-version, optional-runtime and recovery tests in `scripts\test_bump_version.py`.

Related files were read only to establish the documentation contracts: branch skill and master reference; `build.sbt`, `environment.yml`, `project\ReleaseVersion.scala`, `project\CodegenPlugin.scala`, `RCodegen.scala`; release preparation/tag/port-tag/notes and website workflows; the publication sections of `pipeline.yaml`; `release_matrix.py`, `bootstrap_release.py`, `release_guard.py`, `release_ops.py`, `verify_release.py`, `release_dry_run.py`, `test_release_rehearsal.py`, `test_public_release_docs.py`; website package/config/version/lock data; deep-learning and ONNX source guides. The website tests also read their current and historical documentation fixtures.

## Validation and gaps

- `node --test --test-reporter=tap website\test\installDocs.test.js website\test\rSetupDocs.test.js`: 34 passed, zero failed, zero skipped.
- Real source-bump analysis, metadata evaluation, plan derivation, parser probes and the isolated TTL probe ran without writing source, state or artifacts. A first TTL probe omitted the synthetic inventory-completeness field and was rejected before reaching TTL; the corrected isolated probe produced the results above.
- Both in-memory new-version website-test runs exited 0. Snapshot copying/finalization and the final publication-lock update were modeled in memory; these were not real Docusaurus builds or publication.
- No live App, protection/ruleset, Azure, Databricks, storage, PyPI, Maven, GitHub Release or Pages checks were attempted. Their availability and authorization remain unverified external gates, not findings.
- No offline rehearsal runner, SBT/R build, consumer-wheel test, full pytest suite or Docusaurus build was executed because those create files and/or exceed this read-only documentation review. Actual generated artifacts and runtime behavior are not certified by this report.
- Local links were checked for existence; remote URL reachability was not queried. Head remained the requested SHA and tracked source remained unchanged before this artifact was written.

## Post-fix validation handoff

Run these PowerShell commands from the reviewed worktree root. `website\package.json` defines `test` as `node --test`; specifying the two files avoids the other website tests and any network link crawl.

For candidate documentation before the publication-lock follow-up:

```powershell
$env:SYNAPSEML_DOCS_PREVIEW = 'true'
npm --prefix website test -- test\installDocs.test.js test\rSetupDocs.test.js
```

This covers the installation metadata and guides, R source/historical instructions, retained Spark 4.0 references, publication-lock validation and the preview-versus-production lock regression. There is no separate lock-test file; both lock tests live in `installDocs.test.js`.

After verified publication and the reviewed lock follow-up, the smallest strict lock-only rerun is:

```powershell
$env:SYNAPSEML_DOCS_PREVIEW = 'false'
npm --prefix website test -- --test-name-pattern="published Spark port versions are explicitly locked|unpublished documentation can be previewed but cannot be deployed" test\installDocs.test.js
```

Strict mode is expected to reject a newly prepared version before its lock follow-up. Do not change the lock merely to pass candidate tests. To rerun all relevant docs tests in strict mode, use the first npm command again with the variable still set to `false`.

Dependencies checked for this handoff: Node `v24.15.0` and npm `11.12.1` are available, satisfying the package's Node `>=24.0` requirement. `website\node_modules` and the local Docusaurus executable are absent. These two test files use Node built-ins and local repository files, so they need no `npm ci`, Docusaurus, credentials, network access, Python or JVM setup. Website build dependencies are not needed for the requested targeted validation.

The commands above are the parent's post-fix handoff, not a claim that fixes have already been applied or validated.

## Authorized implementation follow-up

The subsequent human request authorized scoped source/documentation edits and local tests. The original findings above remain the record of the reviewed head. The following resolution applies to working-tree changes on top of that head; no commit or live operation was performed.

### F2 resolved

The homepage installation table now displays each runtime's `pythonPackage` beside its matching Maven coordinate. The shared-primary-pin claim and unused `version` destructuring were removed. The deep-learning guide now provides three explicit Python/PySpark commands, including the retained Spark 4.0 pin.

The existing bump logic already recognizes the Spark 4.0 PySpark command and preserves its pin, so neither `installArtifacts.js` nor the bump implementation needed a new abstraction or heuristic. Historical snapshots and the publication lock were not changed.

Regressions now require runtime-specific Python commands in the installation guides, source deep-learning guide and newly generated snapshots, require each homepage table row to use its own Python metadata, and reject replacing a retained runtime pin with the primary pin. The repeated-bump test checks the deep-learning commands across two successive bumps, both with retained Spark 4.0 and after an explicit Spark 4.0 update.

Before the implementation, the new assertions produced two website failures and two parametrized bump failures. After the implementation:

| Command | Result |
| --- | --- |
| `$env:SYNAPSEML_DOCS_PREVIEW='true'; npm --prefix website test -- --test-reporter=tap test\installDocs.test.js test\rSetupDocs.test.js` | 35 passed, zero failures/skips |
| `$env:SYNAPSEML_DOCS_PREVIEW='false'; npm --prefix website test -- --test-reporter=tap test\installDocs.test.js test\rSetupDocs.test.js` | 35 passed, zero failures/skips |
| `python -m pytest scripts\test_bump_version.py -q -p no:cacheprovider -k repeated_default_release_bumps_keep_optional_runtime_installations` | 2 passed, 270 deselected; existing unregistered `slow` mark warning |
| `python -m pytest scripts\release\test_public_release_docs.py -q -p no:cacheprovider -k 'preserves_required_source_and_approval_boundaries or consumer_wheel_gate_precedes_tagging'` | 2 passed, 49 deselected |
| `python -m black --check scripts\test_bump_version.py` | Passed, file unchanged |
| `git diff --check` restricted to this implementation's paths | Passed |

Python test commands used `PYTHONDONTWRITEBYTECODE=1` and `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1`. No dependencies were installed.

### F3 resolved and final-wheel human gate clarified

The release skill now says the old Spark 4.1 replay is removed, while preserving required validation for every selected candidate.

The release README, skill, agent runbook and automation-boundaries reference now explicitly require a source/version-bound primary candidate wheel qualified on every selected runtime. They retain the candidate wheel, source/version bindings, environment and JVM evidence for handoff. Before readiness or notes, the release owner must compare the rebuilt published wheel's installable payload and relevant metadata with that candidate and verify final consumer behavior.

The comparison is not ZIP-byte equality. Archive timestamps, ordering and compression may change SHA-256 without changing the payload. The guide names the metadata to compare, requires validating each wheel's `RECORD`, and requires retaining both hashes and comparison evidence. A payload/metadata mismatch, missing qualification evidence or failed runtime check blocks readiness. This is expressly a human gate, not a claim that the current automated verifier performs candidate qualification.

No candidate or published wheel was built, downloaded or qualified during this implementation.

### F1 coordinated, pipeline wiring still owned by the artifact reviewer/parent

Sent the independently reproduced F1 and this proposal to the designated artifact reviewer. I did not edit pipeline or build files or create an API documentation publication lock.

The smallest proposed restoration is inside the existing approved release `Publish` path, after its exact-source guard and `packagePython`, only for the primary target:

```bash
if [ "$RELEASE_TARGET" = master ]; then
  sudo apt-get install graphviz doxygen -y
  sbt -DskipCodegen=true publishDocs
  DOCS_CHECK="$(mktemp)"
  trap 'rm -f "$DOCS_CHECK"' EXIT
  for PAGE in pyspark/index.html scala/index.html scala/com/microsoft/azure/synapse/ml/index.html; do
    curl --fail --silent --show-error --location \
      --connect-timeout 15 --max-time 60 \
      --output "$DOCS_CHECK" \
      "https://mmlspark.blob.core.windows.net/docs/${DOCS_VERSION}/${PAGE}"
    test -s "$DOCS_CHECK"
  done
fi
```

The pipeline owner must bind `RELEASE_TARGET` to the guard's `releaseTarget` and `DOCS_VERSION` to the resolved `packageVersion`, preserve the guard-provided release source/plan environment, and leave ordinary snapshot publication and port release behavior unchanged. This is a proposed pipeline fragment, not an executed command or completed fix. Its HTTP checks prove exact-version endpoint availability; source binding comes from generating/uploading within the existing approved exact-source job, not from HTTP status alone.

Focused pipeline regressions should require the primary-only gate, `publishDocs`, all three exact-version endpoints, failure propagation and unchanged snapshot behavior. Once wiring is confirmed, update the release README's generated-doc exclusion and the automation-boundaries reference to match it. Until then, F1 remains unresolved by this reviewer; the existing statements were not replaced with an unimplemented publication promise.

### Artifact-proof coordination update

After inspecting the artifact reviewer's new guard and download-verification helpers, the release README and automation-boundaries reference now document receipt schema 2, separate CDN `blob_artifacts` and ESRP `artifacts`, the producer-attempt-selected Blob receipt handoff, per-destination public-download comparisons, and refusal of older receipts lacking CDN proof. They do not require CDN bytes to equal ESRP bytes or authorize immutable retries. Stale blanket claims about clean/warning producer-evidence format numbers were removed; the pending producer-evidence integration was not claimed complete. The two targeted release-guide boundary tests passed again.

The final F1 placement prescription was refined to run primary-only `publishDocs` and all three endpoint checks after `packagePython` but **before `publishBlob`**, rather than after it as in the initial proposal. This avoids discovering documentation failures only after immutable Maven/PyPI/ESRP publication and leaves the new Blob-upload/receipt sequence contiguous. The artifact reviewer was asked to move the proposed after-ESRP task to this location and restore graphviz/doxygen prerequisites. Pipeline implementation remains outside this reviewer's edit ownership.

### F1 resolved in the collaborative working tree

After the artifact reviewer confirmed the final implementation, I independently inspected `pipeline.yaml`, the `api-docs` guard branch, and `verify_public_api_docs`. The confirmed release path is:

```text
packagePython
  -> primary-only graphviz/doxygen + publishDocs
  -> source/plan-bound api-docs public checks
  -> publishBlob
  -> Blob producer receipt
```

`PRIMARY_RELEASE` comes from the guarded target identity; unresolved values fail. The `api-docs` command revalidates the approved Maven plan, source tag/commit and clean checkout, rejects port targets and derives the version from the approved primary target. It accepts no arbitrary version argument. Anonymous downloads check the three specified Python/Scala index pages, reject redirects, apply a 60-second network timeout and 5 MiB page limit, and reject missing, empty, oversized or failed responses. A read-only ordering assertion passed and confirmed the provisional after-ESRP task was removed.

The release README, skill, agent runbook and automation-boundaries reference now describe this confirmed wiring. They explicitly state that API docs remain mutable and that successful guarded publication/availability checks are not an immutable API-doc receipt or a new publication lock. The existing package evidence and human final-wheel qualification boundaries remain separate.

The artifact reviewer reported 12 passing focused fixture/static tests. My documentation-boundary rerun passed both selected tests, and scoped whitespace checks passed. No live API endpoint or actual release publication was tested. All three original findings now have working-tree resolutions; final combined CI, live prerequisites and human release approvals remain the parent's release-readiness gates.
