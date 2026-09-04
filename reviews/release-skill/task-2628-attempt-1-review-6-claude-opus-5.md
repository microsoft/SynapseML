## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening
- **Mode**: sequential
- **Model**: claude-opus-5
- **Artifact**: `C:\Users\singhrana\.copilot\session-state\16e6d9b2-ce73-41f9-9d38-9386edc5c48d\files\direct-pr-2628\reviews\release-skill\task-2628-attempt-1-review-6-claude-opus-5.md`
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] `python -m pytest scripts/test_bump_version.py
      scripts/release/test_release_matrix.py
      scripts/release/test_verify_release.py
      scripts/release/test_bump_bbcvhd.py
      scripts/release/test_release_workflows.py -q` -> **313 passed** in 78.3s,
      up from the 311 recorded before the Round 6 fixes; the two added tests are
      the scope-reporting cases. The suite list still matches
      `scripts/release/README.md` (Tests) and
      `.github/skills/synapseml-release/references/preflight.md` (section 3).
- [x] `python -m black --check scripts/bump-version.py
      scripts/test_bump_version.py scripts/release/verify_release.py
      scripts/release/test_verify_release.py` -> "4 files would be left
      unchanged". `pyproject.toml` configures Black only; there is no `.flake8`,
      `setup.cfg`, or `tox.ini` line-length gate, and the new 101-character
      usage line at `verify_release.py:22` sits alongside pre-existing 101- and
      94-character lines at 589 and 594.
- [x] `python scripts/bump-version.py --to 1.1.4 --dry-run` exited 0, reported
      `21 files, 143 context-anchored replacements`, printed
      `[DRY RUN] Would run: sbt convertNotebooks` and
      `[DRY RUN] Would run: npm exec -- docusaurus docs:version 1.1.4`, emitted
      no `ERROR` or `FATAL` line, and `git status --porcelain` before and after
      was string-identical. The dry-run early return is
      `scripts/bump-version.py:406-410`; the `website/docs/` guard at 412-419
      still gates only the write path.
- [x] Prior Finding 1 is fixed and verified offline (stubbed network, no ADO or
      GitHub calls): `--internal-patch 1 --json` now emits keys
      `['version', 'internal_patch', 'scope', 'complete', 'rows']` with
      `scope=internal-only`, `internal_patch=1`, **19 rows**, and zero OSS rows;
      the text form opens with
      `SynapseML verification  OSS v1.1.3  Internal patch 1  scope=internal-only`
      above the unchanged `19 checks, 0 missing -> COMPLETE` footer.
      `--internal-patch 0` still resolves to `full` with **57 rows**, so the
      shared `_resolve_scope` helper keeps `run()` and `main()` in agreement.
- [x] Prior Finding 2 is fixed and verified by executing the exact command shape
      now published at `references/preflight.md:69-75`
      (`--internal-patch 0 --scope full --targets ... --skip internal`):
      **57 rows, 38 PRESENT / 19 SKIPPED**, with the skipped set exactly
      `ado/SynapseML-Internal`, `ado/SynapseML-Internal/same-commit`,
      `synapseml-internal`, `synapseml-internal_2.12`,
      `synapseml-internal_2.13`, and `synapseml_internal`. The retained OSS tag,
      Maven, PyPI, UPack, and pip rows are exactly the OSS-base proof the skill
      asks for. The counter guidance in the same section is accurate:
      `--upack-iteration spark4.0=1` moves the OSS UPack row to
      `1.1.3-spark4-0-1` on the first command, and
      `--internal-upack-iteration spark4.0=1` moves the Internal UPack row to
      `1.1.3-1-spark4.0-1` on the `--scope internal-only` command.
- [x] The scope guards fail closed: `--scope internal-only --internal-patch 0`
      -> exit 2 with `error: --scope internal-only requires a nonzero
      --internal-patch`, and `--scope full --internal-patch 1` -> exit 2 with
      `error: a nonzero Internal patch is an Internal-only hotfix; use --scope
      internal-only`. `--scope` draws its choices from the imported
      `RELEASE_SCOPES`, so argparse rejects any other value before any network
      call.
- [x] Compatibility holds. The only non-documentation caller is
      `.github/workflows/release-notes.yml:75-77`, which runs
      `verify_release.py --version "${TAG#v}" --skip ado,internal` with no
      `--internal-patch`; that still resolves to `scope=full` with the same
      57-row behavior, and the step consumes only the exit code, so the added
      header line cannot break it (`test_release_workflows.py:21` still
      matches). `run()` gained `scope` as a trailing keyword argument, leaving
      its six-positional callers working, and the JSON change is additive.
- [x] Documentation matches runtime. `scripts/release/README.md:106-108`
      ("Text and JSON output record the resolved scope and Internal patch ...
      infers `internal-only` from a nonzero `--internal-patch` and `full`
      otherwise") is exactly what the probes produce, and the README's
      Internal-only example matches the 19-row result. All 38 relative Markdown
      links across the four skill documents, `AGENTS.md`, and
      `scripts/release/README.md` resolve on disk (`broken: []`), and the skill
      front matter still uses the `name`/`description`/`compatibility` shape of
      `synapseml-branches`, `synapseml-local-setup`, and `synapseml-pr-loop`.
- [x] The `reviews` denylist entry remains load-bearing and narrowly scoped:
      `reviews` appears once in `scripts/bump-version.py` (line 131, inside
      `DENYLIST_DIRS`) and in no `EXPECTED_FILES` entry; the only tracked
      matching paths are the 19 files under `reviews/pr-2666/`; and the working
      tree's `reviews/release-skill/` artifacts remain untracked and were
      correctly ignored by the dry run above.

## Prior Findings

Both findings were raised in the first pass of this round and are resolved in
the current diff. They are retained for the audit trail.

### Issue 1: A scope-narrowed verification is not recorded anywhere in its own report
- **Severity**: Low
- **File**: `scripts/release/verify_release.py`
- **Lines**: 361, 372, 407-473, 541-543, 563-582
- **Description**: `run()` (361) and `main()` (541-543) both infer
  `internal-only` from a nonzero `--internal-patch`, and `include_oss` (372)
  then drops the OSS tag family, all seven public Maven coordinates, PyPI, the
  OSS UPack, and the OSS wheel from the report entirely (407, 430, 444, 463).
  Neither output form records that narrowing: the JSON object at 566-568 emits
  only `version`, `complete`, and `rows` - no scope and no `internal-patch` -
  and the text summary at 578-581 prints `19 checks, 0 missing -> COMPLETE`
  with no scope line. This is the opposite of the convention the same tool uses
  for `--skip`, where an excluded row is still emitted with status `SKIPPED` so
  the evidence shows exactly what was not checked, and it diverges from the
  sibling generator `release_matrix.render_text`, whose header already prints
  `scope=`. Before this diff the row set was scope-invariant (a nonzero patch
  still produced all 57 rows), so this ambiguity is new.
- **Risk**: The skill requires the plan and the verification output to be kept
  as release evidence (`SKILL.md` rules; `preflight.md` "Record every skip with
  the release evidence"). A reviewer reading a stored `--json` artifact cannot
  distinguish "OSS rows were intentionally out of scope" from "OSS rows were
  never checked", and a mistyped nonzero `--internal-patch` on a full release
  silently reduces 57 checks to 19 while still reporting `COMPLETE` and exit 0.
  Rated Low because the reviewed matrix plan does print `scope=`, the retained
  row names are self-identifying (`ado/SynapseML-Internal`,
  `synapseml-internal_2.12`), and the identifiers carry the Internal patch.
- **Suggested fix**: Record the resolved scope in both outputs: add `"scope"`
  (and the Internal patch) to the JSON object at 566-568, and print the scope
  in the text header or footer the way `release_matrix.render_text` does. That
  also gives `main()`'s scope resolution at 541-543 a purpose beyond
  duplicating `run()`'s default at 361; if the report is left unchanged,
  collapse the duplication instead by passing `scope=args.scope` and keeping a
  single inference site.

### Issue 2: The Internal-only track must confirm an existing OSS tag set, but no command is given and the scoped verifier no longer covers it
- **Severity**: Low
- **File**: `.github/skills/synapseml-release/SKILL.md`;
  `.github/skills/synapseml-release/references/preflight.md`;
  `scripts/release/verify_release.py`
- **Lines**: `SKILL.md` 76-79 (and 58-61); `preflight.md` 65-74;
  `verify_release.py` 372, 407
- **Description**: Step 3.3 of the skill says "Confirm that the plan uses an
  existing OSS tag set, then run `verify_release.py` on the in-scope rows ...
  Pass `--scope internal-only`", and points at `preflight.md` "for the exact
  commands". `preflight.md:67-68` repeats the same instruction. Every other
  preflight action has a fenced command - plan generation, the version-bump
  preview, the test list, the known-release replay, the BBC-VHD preview - but
  this confirmation has none. Until this diff the confirmation was implicit in
  the tool: `verify_release.py --version X --internal-patch N` still emitted the
  OSS tag, Maven, PyPI, UPack, and pip rows, so an Internal-only run
  incidentally proved the OSS base release existed. The new `include_oss`
  gating removes those rows, and `build_plan` rejects `--scope full` with a
  nonzero patch, so no single documented invocation proves the precondition any
  more.
- **Risk**: The one prerequisite that makes an Internal-only hotfix legitimate -
  a complete, already-published OSS release to hang it on - is left to
  improvisation, and whatever the operator improvises is not part of the
  release evidence. An Internal patch prepared against a version whose OSS tag
  set is incomplete would still verify `COMPLETE` for every Internal row. Rated
  Low because the matrix text output labels those tags "required existing" and
  a release engineer would likely check them by hand, but the skill's own bar is
  that a maintainer should not have to guess.
- **Suggested fix**: Add the concrete command to `preflight.md` section 2 and
  reference it from `SKILL.md` step 3.3, for example
  `python scripts/release/verify_release.py --version <X.Y.Z> --internal-patch 0
  --skip internal` - full scope with Internal rows suppressed, which per the
  `--skip` semantics documented in `scripts/release/README.md` leaves exactly
  the OSS tag, Maven CDN, PyPI, UPack, and pip rows - and require its output as
  the "existing OSS release" evidence captured before the
  `--scope internal-only` run.

## Resolution Log

### Issue 1

- **Status**: Resolved
- **What changed**: JSON output now records `internal_patch` and the resolved
  `scope`. Text output starts with a header containing the OSS version,
  Internal patch, and scope. A shared `_resolve_scope` helper keeps `run()` and
  `main()` inference consistent.
- **Why**: Stored verification evidence now shows why OSS rows are absent and
  which release track produced a `COMPLETE` result.
- **How verified**: New JSON and text tests assert the resolved Internal-only
  scope. Direct CLI probes show `scope=internal-only`, patch `1`, and seven
  Internal rows. The combined suite passed all 313 tests.

### Issue 2

- **Status**: Resolved
- **What changed**: `preflight.md` now gives separate copyable commands for
  the existing OSS base and the Internal-only patch. The OSS command uses full
  scope with `--internal-patch 0` and `--skip internal`; the second command
  uses `--scope internal-only`. Counter guidance names the flag that belongs on
  each command. `SKILL.md` directs operators to capture both results.
- **Why**: An Internal patch now has explicit evidence that its OSS base is
  complete before the scoped Internal verification begins.
- **How verified**: Both command shapes match the verified CLI scope rules, all
  38 local links resolve, no skill code block exceeds 15 lines, and the skill
  remains below 500 lines.

## Re-review Result

Re-reviewed the current worktree diff (10 files, +741/-37) against the Round 6
polish-and-hardening theme. Both prior findings are closed in the code, not just
in the log: `verify_release.py` now reports `internal_patch` and the resolved
`scope` in JSON and in a text header, and `references/preflight.md:69-84` gives
two copyable commands whose row sets I executed and confirmed. No new
performance, observability, documentation-accuracy, naming, compatibility, dead
code, or credential-handling defect was found in the diff.

Specifically re-checked and clear: the fixes add no unnecessary work (the
Internal-only path now issues fewer live requests, not more); failures stay
observable (the scope guards exit 2 with actionable messages, and `--skip` rows
are still emitted as `SKIPPED` rather than dropped); the new header does not
break the one workflow caller, which reads only the exit code; `_resolve_scope`,
`include_oss`, and the imported `RELEASE_SCOPES` are all used, so the change
leaves no dead code; and no output prints a token, coordinate secret, or
approval bypass. The `reviews` denylist entry is still the minimum needed to
keep immutable review evidence out of version replacement, and this round's own
artifacts stay untracked.

Verdict changed from ISSUES_FOUND (2) to CLEAN (0).
