## Review Summary

Publication note: this prerequisite-specific directory preserves the separate
port review records. Paths in the original review describe its review-time
location. Machine-local prefixes were removed: artifact references are
repository-relative and locally retained validation logs are named by file.

- **Round**: 1 only, attempt 1, master companion
- **Theme**: Broad sweep, correctness, security, logic, and spec conformance
- **Mode**: sequential
- **Model**: gpt-6-astra
- **Reasoning**: xhigh
- **Target**: master
- **Branch**: `fix/fabric-cleanup-relations-20260921`
- **Artifact**: `reviews/sync-20260921/task-spark4-sync-20260921-attempt-1-review-1-gpt-6-astra.md`
- **HEAD**: `714d365e71f6d2db5b7072094a4a3ad22485eb57`
- **Initial baseline index tree**: `70d410b583dc2d9a9a60711ae6cf4acb1bcb30fd`
- **Reviewed index tree**: `e8d864108bf2b8f285890e794604ee464bed2969`
- **Reviewed content**: The three-file fix identified by the blob hashes below,
  initially unstaged and observed as staged during final verification. The
  final index contains those three changes, not this untracked review artifact.
- **Issues Found**: 0
- **Verdict**: CLEAN for the narrow companion diff; not a readiness assessment

## Evidence Checklist

- [x] Read this worktree's `AGENTS.md`, master branch reference, and relevant
  version declarations in `build.sbt` and `environment.yml`. Applied the active
  code-review and synapseml-branches skills. Master's Spark 3.5.0 and Scala
  2.12.17 baseline remains unchanged.
- [x] Inspected the complete three-file diff against HEAD and checked
  `git diff HEAD --check`. No workflow, runtime, dependency-pin, or unrelated
  master changes are included. No broader master re-audit was performed.
- [x] Verified the two fixed Scala blobs exactly match the fix already reviewed
  in both port candidates:

  | File | Reviewed blob |
  | --- | --- |
  | `core\src\test\scala\com\microsoft\azure\synapse\ml\nbtest\FabricArtifactCleanup.scala` | `fc9f27368c896bba8c5934d3824a7ef015d8442f` |
  | `core\src\test\scala\com\microsoft\azure\synapse\ml\nbtest\FabricTestArtifactTrackerSuite.scala` | `a8d0676aa6b292a581843c09a6249c940fe19586` |
  | `docs\Reference\Developer Setup.md` | `86607666023a9611cbdbe7cce3d877637a37c512` |

- [x] Confirmed that the documentation diff adds only the shared parser-contract
  paragraph. It does not import the ports' branch-disabled Fabric E2E wording.
  A final read caught and checked a wording-only refinement to that paragraph:
  invalid metadata fails inventory rather than authorizing cleanup with an
  incomplete graph. The table records this final documentation blob; both
  Scala blobs remained unchanged.
- [x] Rechecked the malformed-reference fix. Every leaf must be a GUID string;
  nested objects and arrays must be nonempty and every child must validate.
  Unsupported values throw a field-specific error without echoing payload
  values. A valid GUID cannot mask an invalid sibling. `item` traverses each
  entry once, while null and empty outer relation collections still indicate
  no relations.
- [x] Reviewed the shared regression's four relation fields and nine malformed
  values per field. Each of the 36 combinations asserts inventory failure,
  zero DELETE calls, and an unchanged store. The positive nested-container
  case retains valid references and canonicalizes mixed-case GUIDs.
- [x] The conservative contract explicitly rejects unknown metadata rather
  than guessing undocumented relationship field names. It closes the imported
  microsoft/SynapseML#2728 finding recorded in the port artifacts. The fix
  introduces no port-specific logic or newer-JDK API dependency.
- [x] Retained the already inspected supporting evidence: the original parser
  failed the mixed-valid/malformed regression in `files\spark41-validation.log`,
  with 73 other tests passing. Both port fixed logs subsequently showed 44/44
  selected cleanup tests passing with no failed, canceled, ignored, or pending
  tests. These results support the identical source fix but do not substitute
  for master's own runtime validation.
- [x] Inspected startup of session `files\master-cleanup-validation.log`.
  It identifies this worktree, JDK 11.0.31, and the intended core compile,
  test-compile, style, `FabricTestArtifactTrackerSuite`, and
  `FabricArtifactNamesSuite` commands.
- [ ] Successful completion of master's JDK 11 compilation and fake-client
  tests is not established by the inspected startup output. The parent reports
  that run as in progress; no completion is claimed here.
- [ ] No live Fabric or remote-service validation was performed. No source
  edits, staging, commits, pushes, or agent dispatch were performed by this
  reviewer.

The companion isolates the portable fix for master-first integration. The
review verifies content equivalence, not that the master change has landed or
that either port has subsequently merged it.
