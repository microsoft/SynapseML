# Agent-operated public release

Use this runbook with the [operator guide](../../../../scripts/release/README.md).
The agent calls the existing guarded scripts. It does not implement a second
publisher, infer approval from a plan hash or approve its own requests.

## 1. Establish scope and authority

Record the requested version, selected runtimes, normal or pre-merge procedure,
release owner and signing approver. Read the current branch policy and resolve
all source and runtime versions from the selected branches.

Ask for explicit authorization before creating candidate branches/PRs or
dispatching live validation. Approval to write automation or run offline tests
does not authorize those operations, production tags, packages, notes or merges.
Use the existing authorized identities, never credentials pasted into chat.

Confirm the one-time App setup in the operator guide. The App needs Contents
read and Pull requests write on the canonical repository. Check the presence,
not the values, of `RELEASE_APP_CLIENT_ID` and `RELEASE_APP_PRIVATE_KEY`.
An inaccessible settings API means unknown, not absent or configured. Missing
App settings block App-based preparation, not offline rehearsal. Confirm live
publisher, Databricks and storage access separately; an installed App grants
none of those permissions.

Choose one protected directory outside the checkout for the release plan,
ledger, persistent plan claim and operator evidence. Record its exact paths
and host. Keep one active operator. Local locks do not serialize copies on
different machines. Do not put raw ledgers or operator records in public PRs.

## 2. Rehearse offline

From the reviewed checkout run:

```text
python scripts/release/release_dry_run.py --report <new-local-report.json>
```

Use native path separators on Windows. The script needs Python with the
repository's pytest and PyYAML test dependencies, and Git on PATH. It runs
fixed tests with simulated services and disposable local Git repositories.
It accepts no release plan, credentials, approval ID or production switch.
It never overwrites an existing report. Missing dependencies, timeout, no
executed tests, failures or any skipped case are errors, not a successful run.

The JSON result must say `status: passed`, with nonzero test counts and zero
failures/skips. It always says `live_services_validated: false` and
`publication_authorized: false`. Record the source revision and uncommitted
diff alongside it. Rehearsal covers read-only checkpoints, approval rejection,
ledger resume, simulated publication/receipt verification, bootstrap preview,
local atomic tags and candidate-CI refusal. Simulated approval exists only
inside these isolated tests.

This result is not candidate qualification, a live App test, signing proof,
artifact publication evidence or permission to proceed.

## 3. Prepare and qualify candidates

After authorization for preparation, follow the operator guide to create
version/docs and selected port PRs. Do not skip versioned documentation or bump
the published-artifact lock yet. Run current-head Azure and GitHub validation.
The release owner must qualify the same source/version-bound primary candidate
wheel against each selected candidate's own JVM packages. Retain that wheel,
the exact source commits and release version, environment details, wheel/JAR
hashes and actual test results. This is a human qualification gate, not a result
inferred from the plan, offline rehearsal or a port's own generated wrappers.

For the first, pre-merge release, use the operator guide's exact candidate
branch names and bootstrap procedure. Check classic protection and active
rulesets before tagging. The unchanged candidate must remain mergeable under
those rules. Do not remove protection or assume an unreadable rule permits it.

Generate the public source-bound plan with `release_matrix.py`. Do not use
unbound drafts or alter an existing plan. In the bootstrap procedure it binds
the reviewed candidate commits before tags exist. In the normal procedure bind
the canonical tags' final commits.

## 4. Preview and request publication approval

For bootstrap, run `bootstrap_release.py --plan <plan>` first. With explicit
permission to dispatch a preview, add `--dispatch-preview` and inspect that
run's result. This creates a workflow run but no tags. The normal tag workflow
must retain its master-ancestry requirement.

After canonical tags exist, run these existing read-only commands:

```text
python scripts/release/release_ops.py preflight --plan <plan> --state <ledger>
python scripts/release/release_ops.py resume --plan <plan> --state <ledger>
```

They queue nothing but can save local state. Before bootstrap tags exist,
publication preflight cannot pass; use the bootstrap preview instead and run
publication preflight after approved tagging. Do not treat missing tags as a
reason to bypass the guard.

Present the maintainer with the version, targets, exact candidate commits,
canonical tag set, artifact destinations, plan ID, candidate test evidence and
remaining service/signing gates. Obtain explicit approval for that exact plan
and the requested tag/publication operations. The agent may display the digest
but must not supply its own approval. Changed source, scope or version requires
a new plan and fresh approval.

For bootstrap, use the approved `bootstrap_release.py --dispatch` command
from the operator guide. Verify the completed run and canonical refs, then run
publication preflight. Freeze every tagged candidate: no new commits, rebase,
force-push, amend or Update branch, even if the general PR skill suggests it.
If preflight reveals a new blocker, stop before queueing publication.

## 5. Publish and monitor

Only after recorded exact-plan approval, use the operator guide's
`release_ops.py resume --apply --approve-plan <approved-id> --wait` with the
original plan and ledger. Do not substitute raw pipeline queue commands.

Capture the returned build IDs and watch the exact tagged source. Stop for the
human signing gate. A timeout or interrupted session does not cancel a build.
Use `status --wait` to monitor without queueing more work; use approved `resume`
only when execution is still authorized.

Exit `0` from preflight means the inspection succeeded, not that publication
completed. Exit `1` from resume/status means incomplete work; inspect the JSON
and do not blindly retry. Exit `2` means invalid state, policy, approval or
transport and requires investigation. Unknown submissions, partial publication
or failed required tasks block progress. Follow
[recovery](recovery-and-rollout.md); never delete the ledger, self-adopt a run,
overwrite artifacts or retry immutable Maven coordinates.

## 6. Verify and finish

Run `verify_release.py --plan <plan> --state <ledger> --json` and retain fresh
producer receipts, hashes and public download evidence. Verify the final wheel
and installation on every selected runtime. The release owner must compare the
rebuilt published wheel's installable payload and relevant distribution metadata
with the retained qualified candidate, using the operator guide's
[comparison requirements](../../../../scripts/release/README.md#python-distribution-readiness).
ZIP timestamps, ordering or compression may change archive hashes without
changing payload; retain both hashes and the comparison evidence rather than
requiring identical ZIP bytes. A payload or relevant metadata mismatch, missing
candidate evidence, or failed consumer check blocks readiness and notes.
The verifier does not enforce this human qualification gate. Offline rehearsal,
candidate tests and green producer CI do not replace final-wheel sign-off.

Request the human merge of the unchanged primary candidate first. If it
cannot merge, preserve tags/artifacts and stop for source reconciliation. Do
not declare the automation PR merged merely because a candidate includes it.

Once the notes workflow exists on master, run
`release_guard.py verify-primary-integration` with the actual primary tag and
commit. Generate fresh public evidence, then dispatch notes only with explicit
authorization using the operator guide. Keep private state out of its inputs.
Evidence expires after one hour; regenerate it rather than editing timestamps.

Prepare the reviewed published-artifact lock follow-up only after source
integration and artifact verification. The primary producer generates API docs
from its approved source and checks their version-specific public entry points
before Maven upload. Retain that producer result, but do not describe mutable
API docs as an immutable package receipt. The maintainer merges the lock
follow-up. Verify the actual website deployment, not only its build, before
declaring docs updated.

## Handoff at every stop

Keep a local record of the source revision, plan ID and paths, last completed
checkpoint, unchanged candidate SHAs, exact build/run IDs, test/artifact
evidence, outstanding approvals and the next permitted command. Distinguish
offline rehearsal, live preflight, candidate qualification and published
artifact verification. Do not label any of the first three "release complete".
Include the retained candidate wheel, source/version bindings, final-wheel
payload/metadata comparison and release owner's qualification sign-off.

An agent resuming work must reread that record and the authoritative ledger,
check for another operator, refresh service state and recheck the approval's
scope. Conversation history alone is not the release ledger.
