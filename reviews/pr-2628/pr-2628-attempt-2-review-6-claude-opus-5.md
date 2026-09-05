# PR 2628, attempt 2, round 6

## Review summary

- Round: 6 of the sequential six-round review (final round).
- Theme: Polish and hardening — performance and bounded work, useful diagnostics,
  documentation and CLI accuracy, compatibility, observability, and clear
  operator behaviour.
- Mode: Sequential, read-only source review with small inert offline probes.
- Actual model: `claude-opus-5`, reasoning effort `max`.
- Work: <https://github.com/microsoft/SynapseML/pull/2628>.
- Target: `master` at `8c7143875c843c649a817cf3e8ba9c7bee23689c`.
- Committed head: `243694bccff1d8de0903d813993cdf838f8bd371`.
- Reviewed baseline: that head plus the frozen, uncommitted working-tree
  implementation, including untracked production and test files. The round-6
  inventory lists 31 public source files, captured `2026-09-05T10:48:31Z`.
- Issues: 3 (0 High, 1 Medium, 2 Low).
- Verdict: **ACCEPT WITH MINOR FINDINGS**. Nothing found in this round blocks the
  release-safety contract; all three items are hardening/diagnostics polish with
  small, local remedies.

This report contains only public source and public contract details. Internal,
publisher, and wiki implementation details are kept in their own repositories'
reports.

## Evidence checklist

- [x] Read `AGENTS.md` in this checkout (including the new
      `reviews/pr-<PR-number>/` artifact rule), and the public release skill
      `.github/skills/synapseml-release/SKILL.md` with
      `references/preflight.md`, `references/automation-boundaries.md`, and
      `references/recovery-and-rollout.md`.
- [x] Read the authoritative `review-context.json` and `command-contract.json`
      before inspecting source.
- [x] Verified SHA-256 and byte size for all 91 manifest entries in
      `review-6-source-manifest.json` **before and after** inspection: 91/91 OK
      both times. In particular `scripts/release/release_ops.py` remained
      124298 bytes / `23f7124dc82c7021cc4878fc398b26c9d31855963e4072b23768209ce4e402c5`
      and `scripts/release/README.md` remained 21781 bytes /
      `5bf529abd6ac1b2e5be7a5976de7ab2edb1ae37f29b90a4edd2e1ecff0e3841a`.
- [x] Verified all four committed heads before and after; all unchanged.
      Public head `243694bccff1d8de0903d813993cdf838f8bd371` on
      `work/pr-2628-release-skill`. Working-tree entry counts were identical
      before and after (public: 22 modified + 14 untracked = 36).
- [x] Read the full public driver `scripts/release/release_ops.py` (3048 lines),
      `release_matrix.py` (805 lines), `verify_release.py` (989 lines),
      `release_guard.py`, `bump_bbcvhd.py`, and `tools/esrp/prepare_jar.py`.
- [x] Read `scripts/release/README.md` end to end and checked every documented
      command, flag, exit code and coordinate claim against the actual argparse
      definitions and code paths (`release_matrix.py:731-805`,
      `verify_release.py:774-989`, `bump_bbcvhd.py:152-196`,
      `release_guard.py:228-252`, `release_ops.py:2930-3044`).
- [x] Read the changed public workflows (`release-prepare.yml`,
      `release-tag.yml`, `release-tag-spark.yml`, `release-notes.yml`,
      `pr-validation.yml`) and the `pipeline.yaml` release/publish jobs against
      `release_guard.py`, `project/ReleaseVersion.scala`, `project/build.scala`
      and `build.sbt`.
- [x] Cross-checked the derived coordinate contract end to end: publisher/driver
      action IDs, `_publish_flag` suffixes, `_source_tag` selection,
      `_required_rows` names versus `verify_release._check_plan` rows, and the
      `pypi/<wheel>` receipt path versus `publishPypi` in `build.sbt`.
- [x] Confirmed the release gates are fail-closed by construction: `Release`
      depends on `Publish`, `publishArtifacts`/`testStyle`/`testUnit`/`testPython`
      default to `true`, and the in-build guard steps fail the run when they are
      not, so `_refresh_group` can never mark such a run complete.

Inert offline probes (no network, no service calls, no repository writes) were
run from a named scratch directory
`release-safety-implementation/round6-probe/`:

```powershell
$root = 'C:\Users\singhrana\.copilot\session-state\16e6d9b2-ce73-41f9-9d38-9386edc5c48d\files\direct-pr-2628'
git --no-pager -C $root rev-parse HEAD
git --no-pager -C $root status --porcelain=v1 --untracked-files=all
python scripts/release/release_matrix.py --version 1.1.4 --json --oss-commit ... --internal-commit ... > ..\round6-probe\full-plan.json
python scripts/release/release_ops.py status --plan ..\round6-probe\full-plan.json --state ..\round6-probe\run\state.json --inspect-lock
python scripts/release/release_ops.py status --plan <plan> --state <plan>            # expects exit 2
python scripts/release/release_ops.py status --plan <plan> --state .\.release-plan-x.json --inspect-lock
python ..\round6-probe\ledger_probe.py                                                # StateStore discovery diagnostics
python ..\round6-probe\size_probe.py                                                  # ledger/report growth measurement
```

Probe results used below:

- `status --inspect-lock` produced the documented bounded, Azure-free report and
  exit `0`; the reserved-filename and `--state == --plan` paths produced exit `2`
  with their documented messages.
- The `--upack-iteration spark4.0=1` example in the README derives
  `oss_upack_version = 1.1.4-spark4-0-1` exactly as documented.
- `release_ops.now()` emits `+00:00`-offset timestamps, never a `Z` suffix.
- One retired retry attempt canonicalises to 19472 bytes, of which three copies
  of the base64 plan payload (3924 characters each) dominate.
- Probe scratch files were kept outside all four checkouts; the one
  `scripts/release/__pycache__` directory created by importing the driver was
  removed, and the post-review hash/head/status verification confirms the frozen
  trees are byte-identical.

No service or network calls, credential reads, Azure queries or previews, queue
operations, publications, permission changes, or Git mutations were performed.
No test or build suite was re-run.

## Documentation and CLI accuracy

Every command, flag, action ID, pipeline ID, exit code and coordinate example in
`scripts/release/README.md`, `SKILL.md`, `references/preflight.md` and
`references/recovery-and-rollout.md` matched the actual implementation, including:

- `release_matrix.py` flags `--version --internal-patch --targets
  --upack-iteration --internal-upack-iteration --scope --families --repositories
  --oss-commit --internal-commit --mode --pip-feed --upack-feed --json`.
- `release_ops.py` subcommands and gating (`--apply` XOR `--approve-plan`,
  one `--retry`, `--retry` incompatible with `--adopt`, `--state` required for
  `status`/`resume`, `--inspect-lock` read-only), the documented action-ID shape
  `publisher.<repository>.<target>.<family>`, and the documented exit codes
  0/1/2.
- `verify_release.py` mutual exclusions (`--plan` vs re-entered coordinates,
  `--state` vs `--inventory-only`, `--github-evidence` requiring `--state`), and
  the `complete=false` / `inventory_complete` distinction.
- `bump_bbcvhd.py` paired flags `--oss-plan --oss-evidence --approve-oss-plan`,
  the internal-only OSS-pin preservation rule, and the `--force-revision` rule.
- `scripts/bump-version.py --to X.Y.Z --dry-run` referenced from
  `references/preflight.md` exists with those exact flags.
- Pipeline IDs `17563` / `18453` / `35879` match `release_matrix.py:66-68`.

No documentation defect was found in this scope.

## Findings

### R6-PUB-1 — Medium — Azure UTC timestamps are rejected on CPython below 3.11, with a diagnostic that blames Azure

- Confidence: High for the mechanism and the effect; the trigger depends on the
  operator's interpreter version.
- Path: `scripts/release/release_ops.py:126-135` (`_time`), reached from
  `_validate_build` (`release_ops.py:1826-1827`, `1832`) via
  `status`/`resume`/`preflight` reconciliation, `_adopt`, `_retry`,
  `verified_evidence` and `validate_producer_evidence`; and indirectly from
  `verify_release.validate_evidence`, `bump_bbcvhd.py --apply --evidence` and
  `release_guard.py notes`. Documentation gap:
  `scripts/release/README.md:3-4` ("The commands below use Bash. The Python CLIs
  also run on Windows.") states no minimum interpreter.
- Condition: `_time` parses Azure's `queueTime` / `finishTime` with
  `datetime.fromisoformat(value)`. Azure DevOps returns UTC instants with a
  trailing `Z` (for example `2026-09-05T10:48:31.4066667Z`). `fromisoformat`
  only accepts a `Z` designator from CPython 3.11 onward. Nothing in the driver,
  the README or the skill declares a minimum interpreter, and no test feeds a
  `Z`-suffixed timestamp: every fixture uses `ops.now()`
  (`test_release_ops.py:237, 284, 291, 1948`), which the probe confirmed always
  emits `+00:00`.
- Effect on CPython 3.9/3.10 (still the system `python3` on several current LTS
  distributions): every reconciliation of a real Azure build fails.
  `_refresh_group` catches the `ReleaseError` (a `RuntimeError`) at
  `release_ops.py:2081-2085` and records the action as **`unknown`** with the
  message "Azure build queue has an invalid timestamp". A successful publication
  is therefore reported as the design's most expensive state — the one the
  README and `recovery-and-rollout.md` route to manual Azure inspection — and the
  documented escape hatch is also blocked, because `resume --adopt` reaches the
  same `_validate_build` and exits `2` with the same message. The diagnostic
  attributes the fault to Azure rather than to the local interpreter. This is
  fail-closed (no wrong publication), but it converts a green release into an
  ambiguous one. CI is unaffected: `pipeline.yaml` pins `UsePythonVersion 3.11`,
  `pr-validation.yml` pins `3.11`, and the `pipeline.yaml` release steps only
  invoke `release_guard.py maven`, which never imports `release_ops`.
- Minimal remedy (either, both are local): normalise the designator in `_time`
  before parsing, for example
  `value = value[:-1] + "+00:00" if isinstance(value, str) and value.endswith("Z") else value`;
  and/or add an explicit interpreter floor at the top of `release_ops.py`
  (`if sys.version_info < (3, 11): raise SystemExit("release_ops requires Python 3.11+")`)
  plus one sentence in `scripts/release/README.md` stating the supported
  interpreter. A single regression feeding a `Z`-suffixed `queueTime` would pin
  whichever choice is made.

### R6-PUB-2 — Low — Ledger-initialisation refusals do not name the file the operator must move

- Confidence: High (reproduced offline).
- Path: `scripts/release/release_ops.py:1436-1447` (`StateStore._legacy_conflicts`).
- Condition: during first-time ledger discovery the scan fails closed on two
  classes of sibling file without naming the candidate:
  `release_ops.py:1438-1441` raises "Unclassified files in ledger directory;
  nominate the existing ledger or use the authoritative plan directory" for any
  non-`.json` sibling, and `release_ops.py:1443-1447` raises "Cannot read Sibling
  ledger candidate" / "Sibling ledger candidate contains invalid JSON" for an
  unreadable or malformed `.json` sibling. The third branch of the same function
  already does the right thing and names the path
  (`release_ops.py:1453-1455`, "Another ledger for this plan exists in the same
  directory: {candidate}").
- Effect: the operator is told a file in the release-run directory is
  unacceptable but not which one, and must bisect the directory by hand at the
  moment they are trying to start a release. Reproduced with the offline probe
  (`round6-probe/ledger_probe.py`):

  | Directory contents | Result |
  | --- | --- |
  | `plan.json` | entered OK |
  | `plan.json`, `notes.txt` | `Unclassified files in ledger directory; ...` (no path) |
  | `plan.json`, `public-evidence.base64` | `Unclassified files in ledger directory; ...` (no path) |
  | `plan.json`, `handoff.json` (malformed) | `Sibling ledger candidate contains invalid JSON` (no path) |
  | `plan.json`, `evidence.json` | entered OK |

  The last two rows also confirm the documented layout is safe: a generated plan
  and an exported evidence report may sit beside the ledger, and
  `public-evidence.base64` only conflicts if it is created before the ledger.
- Minimal remedy: include `candidate` in both messages, matching the existing
  third branch — for example
  `raise ReleaseError(f"Unclassified file in ledger directory: {candidate}; ...")`,
  and pass a candidate-specific label into `_file_bytes` / `_json`
  (`f"Sibling ledger candidate {candidate}"`). No behaviour change.

### R6-PUB-3 — Low — `status`/`resume` output embeds whole retired attempts, so one retry adds ~39 KB of opaque base64 to every later report

- Confidence: High (measured offline).
- Path: `scripts/release/release_ops.py:2635-2658` (`_report`), specifically the
  `"attempts"` key at `release_ops.py:2654`, printed by `main` at
  `release_ops.py:3034-3035` for `preflight`, `status` and `resume`.
- Condition: `_report` deep-copies each action's complete `attempts` list. Every
  retired attempt embeds the base64 plan payload three times —
  `previous.command`, `previous.operation.parameters.release_plan_base64` and
  `proof.build.templateParameters` — plus the full recorded build, definition,
  job and absence proof. Measured on a bound full plan
  (`round6-probe/size_probe.py`): the canonical plan is 2942 bytes, its base64
  payload 3924 characters, and one attempt entry canonicalises to 19472 bytes.
  Because a retry is grouped, a single pip+UPack retry writes the same entry to
  both members, so the very first retry adds ~39 KB canonical (noticeably more
  as `indent=2` JSON) to the operator-facing report, and it is reprinted on every
  subsequent `status`. The ledger itself remains bounded — the worst case of 16
  attempts on 12 publisher actions is ~3.6 MB against the 16 MiB
  `MAX_JSON_BYTES` ceiling — so this is an observability problem, not an
  unbounded-work problem.
- Effect: the human-readable reconciliation document that the README tells
  operators to read before deciding to retry, adopt or stop becomes dominated by
  repeated opaque payloads that the reader cannot act on, which is exactly when
  clear output matters most.
- Minimal remedy: project the attempts in `_report` instead of copying them
  verbatim, keeping the full record in the ledger — for example emit, per
  attempt, `number`, `retried_at`, `previous.build_id`, `previous.status`,
  `previous.error`, `attempt_id`, and `proof.absence.checked_at`. The complete
  history stays authoritative in the state file and in
  `_validate_attempts`, which already re-validates it on every load.

## Resolution log

All findings are **OPEN** at the time of writing. The parent owns fixes, commits
and PR updates; none of the following has been performed by this reviewer.

| ID | Severity | Status | Owner | Notes |
| --- | --- | --- | --- | --- |
| R6-PUB-1 | Medium | OPEN | Parent | Normalise the `Z` designator in `_time` and/or declare the interpreter floor in `release_ops.py` and `scripts/release/README.md`; add one regression with a `Z`-suffixed Azure timestamp. |
| R6-PUB-2 | Low | OPEN | Parent | Name the offending candidate path in the two unnamed `_legacy_conflicts` refusals. |
| R6-PUB-3 | Low | OPEN | Parent | Summarise `attempts` in `_report`; keep the full history in the ledger. |

## Validation limits

- This was a read-only source, documentation and offline-probe review. No test,
  build or lint suite was re-run, and no dependency was added.
- Supplied evidence from earlier rounds (536 public cases before round 4; 395
  affected driver/evidence/verifier/BBC cases and 4 publisher consumer cases
  after round 4; separate real JDK 11 style/build/version and Linux pipeline
  checks; ordinary/release previews) was treated as context, not as
  current-round validation. Prior review verdicts were not treated as evidence.
- The R6-PUB-1 trigger rests on two external facts stated explicitly rather than
  measured here: Azure DevOps returns `Z`-suffixed build timestamps, and
  `datetime.fromisoformat` accepts that designator only from CPython 3.11. The
  code path, the absence of a declared interpreter floor, and the absence of a
  `Z`-suffixed fixture were all verified directly in this checkout.
- No real release, publication, approval, merge or rollout occurred.

## Parent resolution, 2026-09-05

The original review above is preserved. All three findings are now fixed.

| Finding | Resolution | Evidence |
| --- | --- | --- |
| R6-PUB-1 | `_time` normalizes UTC `Z` and variable-length fractions to a UTC offset and microseconds before parsing. Existing malformed, non-UTC and future-time refusals remain. | Three parser regressions and the actual status/reconciliation path now accept Azure timestamps. The compatibility fixture models the pre-3.11 parser restrictions under Python 3.13; it is not a claim of an installed Python 3.9 or 3.10 run. |
| R6-PUB-2 | Both legacy-discovery errors name the offending path. The same safe label passes through file reading and JSON decoding without printing file contents. | Non-JSON and malformed-JSON CLI cases name the file, preserve it, and create no ledger or queue request. |
| R6-PUB-3 | Reports project eight useful fields per retired attempt. The ledger still stores and validates the complete request and proof. | A grouped retry checks exact summaries, retained proof, unchanged history, no encoded plan payload in the report, and output below half the ledger size. The command guide describes the distinction. |

The seven new public cases failed before the fixes and passed afterwards.
The affected driver, plan-evidence, verifier and BBC-VHD suites then passed
402 cases. Unchanged build/pipeline files retain the earlier separate evidence;
this local result does not claim a new Azure build.

## Current-head CI and review follow-up, 2026-09-05

[GitHub run 33964603972](https://github.com/microsoft/SynapseML/actions/runs/33964603972)
reported 582 passing cases and one failure. The native SBT launcher treated the
sbt-extras `-batch` switch as a task name. Both release probes now use explicit
tasks with closed standard input instead of a launcher-specific switch. Two
isolated public/Internal probes passed through the native SBT launcher on JDK 11.

The current-head Copilot review also raised two actionable findings:

- [Blob inspection diagnostics](https://github.com/microsoft/SynapseML/pull/2628#discussion_r3940562986).
  An unsuccessful inspection now names the release destination and exit code.
  A missing executable keeps its `IOException` cause. Inspection failures still
  refuse upload; they are never interpreted as absence. The real SBT regression
  failed before the change and now covers absent, present, malformed, nonzero
  and missing-executable cases. It also compiles the complete current
  `BuildUtils` source, not just a replica of the changed method.
- [Repeated directory resolution](https://github.com/microsoft/SynapseML/pull/2628#discussion_r3940562998).
  ESRP staging now resolves each admitted module directory once and reuses that
  path for file containment. The regression observed three or four resolutions
  before the fix and one afterwards. Existing linked-directory checks remain.

After these fixes, the real public SBT probe and 28 ESRP/publication-guard cases
passed. Pinned Python formatting and whitespace checks passed. These are
follow-up fixes to the six-round implementation review, not a claim that the
initial CI run passed or that a live release was performed.
