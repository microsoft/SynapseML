# Spark 4.1 sync, attempt 1, round 5

## Review summary

- Round: **5 only**, Testing & Coverage, sequential, `gemini-3.8-flash` (high reasoning).
- Verdict: **CLEAN** for merge-specific test coverage and validation evidence. Issues found: **0**.
- Scope: Verification of test coverage for merge-adapted paths, regression defenses, Docker CI helpers, and Python 3.13/Spark 4.1 runtime smoke.
- No agents/factories launched; no product/test source edits; no staging, commits, pushes, or cloud calls.
- Only this review artifact is created; prior review artifacts remain intact.

## Snapshot

| Item | Value |
| --- | --- |
| Checkout | Repository root, branch `sync/spark4.1-master-20260916` |
| Branch | `sync/spark4.1-master-20260916` |
| Target / HEAD | `06897e5b27e28d84ce7ffa33e93d7f756992d0f2` |
| Master / MERGE_HEAD | `1305587a4afe92d27c8e28894b90e38020252e04` |
| Graph merge base | `a833941704b5e8334ddb40a9d601d7e0c7c0ce9f` |
| Last integrated master | `a6fd536ad76eb1b60ac82f31a362ae624886c6ff` |
| Staged index tree (`write-tree`) | `1d13aec1838335846c676c256ea63f922b76192e` |
| Source report tree | `678e48f77876df8d982718513222314d4be52879` |
| Working tree delta vs index | 0 files (working tree matches staged index) |
| Snapshot check timestamp | `2026-09-16T10:25:00Z` |

## Evidence checklist

- [x] **Merge Adaptation vs Imported Scope**: Verified test coverage separating 612 byte-exact incoming master files from port-adapted files listed in `final-preservation-audit.json` and `validation-report.json`. Master multimodal OpenAI and schema tests pass cleanly alongside Spark 4.1 port adaptations.
- [x] **Docker Helper Red-to-Green Test Coverage**: `tools/ci/get_python_version.sh` was updated to accept the preserved `python=3.13` pin. `tools/ci/tests/test_python_version.py` demonstrates red-to-green progression (2 fail / 8 pass before fix; 10 pass after fix), validating exact repo default input, patch pins (`3.11.8`), minor-series (`3.13`), invalid formats (`3`, `3.13.*`, `3.13.0.1`, `3.13rc1`, `>=3.13`), duplicate detection, and `mapfile` absence.
- [x] **Scala Test XML & Suite Results**: `scala-test-results.json` and target XML reports confirm 235 tests passing across 26 suites with 0 failures/errors: Core 60 (7 suites including `PyCodegenSuite` 17 tests with foreign-param coverage, `ValidateComplexParamSerializer`, `UDFTransformerSuite`), Cognitive 112 (10 suites including multimodal OpenAI, schema, retry, and request body suites), and LightGBM 63 (9 suites including streaming preflight, size, layout, lifecycle, and validation data suites exercising `runtime.LongOffset`).
- [x] **Python 3.13 & PySpark 4.1.1 Smoke**: `spark41-final-python.log` documents isolated Python 3.13.13 and PySpark 4.1.1 smoke testing passing 2 package export tests, syntax-parsing 325 `.py` and 222 `.pyi` generated files without error, and importing all 3 OpenAI schema APIs directly from generated artifacts.
- [x] **CI Policy & Pipeline Regression**: `tools/ci/tests/test_pipeline_yaml.py` validates disabled `FabricE2E` (`condition is False`) while asserting all Key Vault parameters and templates. Following removal of the duplicate `BUILD_SBT` constant at line 32, focused Linux pipeline testing passed all 77 tests with 0 skips, full Linux CI suite passed 142/142 tests, release tests passed 213, and Black 22.3.0 verified 214 files clean.

## Testing & coverage assessment

1. **Parameter and Schema Paths**: `ValidateComplexParamSerializer` (63.3s) and `VerifyJsonEncodableParam` thoroughly test parameter serialization and reflection under Spark 4.1 internals.
2. **Streaming and Runtime Packaging**: Spark 4.1-specific streaming suites directly exercise `org.apache.spark.sql.execution.streaming.runtime.LongOffset`.
3. **Branch Invariant Preservation**: All runtime pins (Scala 2.13.17, Java 17, Python 3.13, unpinned NumPy, DBR 18.0) and SAR `Seq[Row]` baseline contracts are strictly retained and covered.

## Findings and limitations

- **Verdict:** **CLEAN** (0 merge-related test deficiencies or regressions found).
- **Coverage Limitations:**
  - `FabricE2E` remains disabled (`condition: false`); no cloud Fabric execution was performed. Master baseline `236185691` failed during Fabric provisioning (display name collision) before running tests.
  - Python 3.13 testing is verified via AST parsing, package exports, and schema imports; full JVM-backed Python `fit`/`transform` end-to-end testing awaits remote Azure Pipelines CI.
  - Release snapshot unanchored Quantile Regression doc failure reproduces on exact master (`snapshot-master-baseline.json`) and is not counted as a candidate regression.
