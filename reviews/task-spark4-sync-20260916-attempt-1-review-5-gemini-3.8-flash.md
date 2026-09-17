# Spark 4.0 sync, attempt 1, round 5

## Review summary

- Round: **5 only**, Testing & Coverage, sequential, `gemini-3.8-flash` (high reasoning).
- Verdict: **CLEAN** for merge-specific test coverage and validation evidence. Issues found: **0**.
- Scope: Verification of test coverage for merge-adapted paths, regression defenses, and packaging/schema validation.
- No agents/factories launched; no product/test source edits; no staging, commits, pushes, or cloud calls.
- Only this review artifact is created; prior review artifacts remain intact.

## Snapshot

| Item | Value |
| --- | --- |
| Checkout | Repository root, branch `sync/spark4.0-master-20260916` |
| Branch | `sync/spark4.0-master-20260916` |
| Target / HEAD | `ecec8dd58b7a07ebc24d816e321a85ff5dc19d57` |
| Master / MERGE_HEAD | `1305587a4afe92d27c8e28894b90e38020252e04` |
| Graph merge base | `a833941704b5e8334ddb40a9d601d7e0c7c0ce9f` |
| Last integrated master | `a6fd536ad76eb1b60ac82f31a362ae624886c6ff` |
| Staged index tree (`write-tree`) | `8b737ee65320c24fa5f3e1658ee313c9f234d5d0` |
| Working tree delta vs index | 0 files (working tree matches staged index) |
| Snapshot check timestamp | `2026-09-16T10:25:00Z` |

## Evidence checklist

- [x] **Merge Adaptation vs Imported Scope**: Evaluated test evidence separating 619 byte-exact incoming master files from the 35 port-adapted paths recorded in `spark40-audit.json`. Incoming master tests cover multimodal OpenAI, schema verification, and auth retries; merge adaptations are covered by targeted suites.
- [x] **Codegen Foreign-Param Regression Coverage**: Inspected `core/src/test/scala/.../codegen/PyCodegenSuite.scala:253-270` (`ForeignParamPythonStage`). Test demonstrates Spark 4's `IllegalArgumentException` on `stage.getDefault(stage.text)`, executes `makePyFile`, and verifies both runtime `.py` (`text=None`) and stub `.pyi` (`text: Optional[str] = ...`) with `assertPythonCompiles`. Red-to-green logs (`spark40-stub-red.log`, `spark40-stub-green.log`) confirm clean failure prior to fix and pass across all 17 `PyCodegenSuite` tests.
- [x] **Scala Test XML & Targeted Suite Results**: Target XMLs under `target/test-reports` and `spark40-targeted.log` confirm 186 targeted test passes across 19 suites: Core 35 (`PyCodegenSuite`, `RCodegenSuite`, `VerifyDefaultParamInfo`, `ValidateComplexParamSerializer`), OpenAI 89 (multimodal chat, responses, prompt requests, response schema, format ordering, auth retry), and LightGBM 62 (group ID, train utils, validation data server flow/lifecycle/support/retry).
- [x] **Python Packaging, Syntax & Export Verification**: `spark40-final-python.log` proves PySpark 4.0.1 successfully executes package export tests (`synapsemltest.export.test_package_exports`), syntax parses 325 `.py` and 222 `.pyi` generated files without error, and imports all 3 OpenAI schema APIs (`OpenAIResponseSchema`, `HasOpenAIResponseSchema`, `OpenAIRequestBody`) directly from generated output.
- [x] **CI Policy & Pipeline Coverage**: `tools/ci/tests/test_pipeline_yaml.py` confirms `FabricE2E` job is disabled via `assert fabric_e2e["condition"] is False` while retaining all Key Vault integration and template assertions. Linux CI run (`spark40-final-ci.log`) passed 137/137 tests with 0 skips, including all 77 pipeline tests after removing the duplicate `BUILD_SBT` constant at line 33. Black 22.3.0 confirmed 214 files clean.

## Testing & coverage assessment

1. **Parameter and Schema Paths**: `ValidateComplexParamSerializer`, `VerifyDefaultParamInfo`, and `OpenAIResponseSchemaSuite` exercise parameter serialization, JSON encoding, and schema reflection against Spark 4 types.
2. **Persistence and Packaging Paths**: `verifyPythonPackaging` pipeline step combined with generated Python export smoke guarantees package integrity without manual `__init__.py` re-export drift.
3. **Public API Integrity**: Public stage constructors and wrapper stubs correctly handle optional defaults, preserving binary and source contracts.

## Findings and limitations

- **Verdict:** **CLEAN** (0 merge-related test deficiencies or regressions found).
- **Coverage Limitations:**
  - `FabricE2E` remains disabled (`condition: false`) on this port; no live Fabric cloud tests were executed. Master baseline `236185691` failed during Fabric provisioning (display name collision) before test execution.
  - Full JVM-backed Python `fit`/`transform` end-to-end execution and Databricks streaming suites were not executed in this local validation and await remote Azure Pipelines CI.
  - Pre-existing raw `RWrappable.rParamArg` default lookup remains unamended on master and targets; it is outside this bounded sync.
