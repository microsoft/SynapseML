## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: parallel
- **Model**: gemini-3.7-flash
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] tools/ci/tests/test_sbt_retry.py: Verified test coverage maps directly to public wrapper execution via `_run` (`subprocess.run(["bash", SCRIPT, ...])`), validating end-to-end wrapper behavior rather than internal shell functions in isolation.
- [x] tools/ci/tests/test_sbt_retry.py: Verified test fixture fidelity for mock executables (`_fake_sbt`, `_fake_sbt_unresolved`, `_fake_sbt_multiple_unresolved`, `_state_dependent_sbt`, `_fake_curl`, `_fake_sleep`), ensuring deterministic execution without flaky network calls or sleep delays.
- [x] tools/ci/tests/test_sbt_retry.py: Verified regression and negative control assertions (`test_retry_succeeds_because_eviction_clears_the_blocking_state` proving targeted eviction resolves cache blockage, paired with `test_without_eviction_the_same_state_exhausts_every_attempt` proving failure recurrence when eviction cannot reach the directory).
- [x] tools/ci/tests/test_sbt_retry.py: Verified edge condition testing coverage across boundary cases (`test_terminal_unresolved_failure_is_probed_without_eviction`, `test_probe_has_invocation_wide_coordinate_cap`, `test_intermediate_symlinks_cannot_redirect_eviction`, `test_missing_revision_never_evicts_a_module`, `test_home_unset_disables_default_cache_eviction`, and `test_unsafe_cache_roots_are_skipped`).
- [x] tools/ci/tests/test_pipeline_yaml.py: Verified resolver registration assertion (`test_build_has_canonical_maven_central_fallback`) ensures exact singleton registration of `"Maven Central fallback"` and `"https://repo.maven.apache.org/maven2"`.
- [x] tools/ci/tests/: Executed the test suite under WSL; all 30 tests in `test_sbt_retry.py` (and all 92 tests overall across the test directory) passed cleanly.

## Verification Rerun 1

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: parallel
- **Model**: gemini-3.7-flash
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] tools/ci/tests/test_pipeline_yaml.py: Verified `test_build_has_canonical_maven_central_fallback` performs comment-aware regex parsing to validate active, build-wide resolver fallback configuration (`ThisBuild / resolvers += ...`) in `build.sbt`.
- [x] tools/ci/tests/test_sbt_retry.py: Verified production `HOME` defaults in `test_home_defaults_evict_ivy_cache_local_and_both_coursier_hosts`, confirming full eviction across `~/.ivy2/cache`, `~/.ivy2/local`, and `~/.cache/coursier` for both Maven Central hostnames without manual environment overrides.
- [x] tools/ci/tests/test_sbt_retry.py: Verified state-dependent multi-coordinate eviction via `_multi_state_dependent_sbt` in `test_multiple_coordinates_are_evicted_with_one_probe_pair`, proving all incomplete modules are cleared while diagnostic probes remain strictly bounded to a single pair.
- [x] tools/ci/tests/test_sbt_retry.py: Verified curl failure resilience and argument passing in `test_probe_request_failure_is_logged_without_changing_outcome` and `test_probe_reports_maven_central_status_on_resolution_failure` (verifying `--max-time 15`, stderr capture, and exit code neutrality).
- [x] tools/ci/tests/test_sbt_retry.py: Verified mock fixture fidelity (`_fake_sbt`, `_fake_sbt_unresolved`, `_state_dependent_sbt`, `_fake_curl`, `_fake_sleep`), guaranteeing all tests exercise the public wrapper script deterministically without live sleeps or network calls.
- [x] tools/ci/tests/: Verified execution of the 94-test suite across `test_sbt_retry.py` (32 tests) and `test_pipeline_yaml.py` (62 tests) under WSL, with all assertions passing cleanly.

## Verification Rerun 2

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: parallel
- **Model**: gemini-3.7-flash
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] tools/ci/tests/test_sbt_retry.py: Verified `test_probe_reports_maven_central_status_on_resolution_failure` asserts exact matching of the full curl argument vector `["-sS", "-o", "/dev/null", "-w", "%{http_code}", "--max-time", "15", url]`, strictly rejecting argument deviations such as `--max-time 150`, duplicate flags, omissions, or malformed request URLs.
- [x] tools/ci/tests/test_pipeline_yaml.py: Verified `test_build_has_canonical_maven_central_fallback` performs comment-filtered regex matching to enforce that the active resolver configuration in `build.sbt` (`ThisBuild / resolvers += ...`) is present exactly once and not commented out.
- [x] tools/ci/tests/test_sbt_retry.py: Verified production `HOME` defaults (`test_home_defaults_evict_ivy_cache_local_and_both_coursier_hosts`) cover complete cache clearing across `~/.ivy2/cache`, `~/.ivy2/local`, and `~/.cache/coursier` for both Maven Central hostnames without requiring environment overrides.
- [x] tools/ci/tests/test_sbt_retry.py: Verified multi-coordinate state-dependent recovery in `test_multiple_coordinates_are_evicted_with_one_probe_pair` using `_multi_state_dependent_sbt`, proving that all unresolvable cache entries are evicted while diagnostic probes remain strictly bounded to one invocation-wide pair.
- [x] tools/ci/tests/test_sbt_retry.py: Verified error path and exit code resilience (`test_probe_request_failure_is_logged_without_changing_outcome` and `test_missing_probe_command_never_changes_the_outcome`), ensuring network errors or missing curl binaries fail open without corrupting exit codes or preventing retries.
- [x] tools/ci/tests/: Validated all 94 unit and pipeline tests under WSL (32 in `test_sbt_retry.py` and 62 in `test_pipeline_yaml.py`), confirming 100% pass rate without live network dependencies or flaky timing sleeps.

## Verification Rerun 3

The `gemini-3.7-flash` slot returned the following clean review while self-labeling its response as `gpt-5.4`; the dispatch model remains the artifact identity.

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: parallel
- **Model**: gpt-5.4
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Fake-curl argv logging and negative control verification** (`tools/ci/tests/test_sbt_retry.py:95-139, 174-192`): Verified that `_fake_curl` logs invocation argument counts (`printf '%s\0' "$#"`) and elements (`printf '%s\0' "$@"`) with NUL terminators, parsed by `_read_argv_log` into discrete argument lists. Checked `test_fake_curl_preserves_argument_boundaries` which exercises negative control comparisons (`["-sS", "-o"]` vs `["-sS -o"]`), confirming argv boundaries and whitespace are preserved without word-splitting.
- [x] **Discrete CLI argument and timeout assertions** (`tools/ci/tests/test_sbt_retry.py:626-650`): Verified `test_probe_reports_maven_central_status_on_resolution_failure` validates exact curl argument lists `["-sS", "-o", "/dev/null", "-w", "%{http_code}", "--max-time", "15", url]` for both `repo1.maven.org` and `repo.maven.apache.org`, ensuring individual probe limits and endpoint URLs are regression-tested.
- [x] **Cache eviction and probe bounding test suite** (`tools/ci/tests/test_sbt_retry.py:407-797`): Inspected and verified comprehensive test coverage for intermediate symlink protection (`test_intermediate_symlinks_cannot_redirect_eviction`), multi-host default directory eviction (`test_home_defaults_evict_ivy_cache_local_and_both_coursier_hosts`), once-per-endpoint probe bounding across retries (`test_probe_runs_once_per_endpoint_across_retries`), multi-module eviction with single probe pair (`test_multiple_coordinates_are_evicted_with_one_probe_pair`), probe request failures (`test_probe_request_failure_is_logged_without_changing_outcome`), and terminal failure probing without cache eviction (`test_terminal_unresolved_failure_is_probed_without_eviction`).
- [x] **Build configuration and pipeline template fallback validation** (`tools/ci/tests/test_pipeline_yaml.py:117-130`): Verified `test_build_has_canonical_maven_central_fallback` confirms active `ThisBuild / resolvers` registration of `https://repo.maven.apache.org/maven2` while stripping comments.
- [x] **Full test execution** (`pytest tools/ci/tests/test_pipeline_yaml.py tools/ci/tests/test_sbt_retry.py`): Executed the complete test suite (95/95 tests passing) validating all CI retry script and pipeline verification logic without regressions or assertion gaps.

## Verification Rerun 4

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: parallel
- **Model**: gemini-3.7-flash
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Count-prefixed NUL argv logging & boundary validation**: Verified `_fake_curl` writes `$#` count followed by `$@` arguments delimited by `\0`, and `_read_argv_log` decodes and reconstructs exact argument arrays. Validated via `test_fake_curl_preserves_argument_boundaries` in `tools/ci/tests/test_sbt_retry.py:174` (distinguishing separate flags `-sS`, `-o` from compound string `"-sS -o"`).
- [x] **Exact curl CLI option assertions**: Verified `test_probe_reports_maven_central_status_on_resolution_failure` in `tools/ci/tests/test_sbt_retry.py:655` asserts exact options `["-sS", "-o", "/dev/null", "-w", "%{http_code}", "--max-time", "15", url]` for both probed URLs (`repo1.maven.org` and `repo.maven.apache.org`).
- [x] **Nearest-neighbor eviction controls**: Verified `test_evicts_incomplete_module_then_succeeds` in `tools/ci/tests/test_sbt_retry.py:349` sets up and asserts retention of same-organization Ivy cache/local modules, alternate Coursier revisions, and sibling Coursier modules on both Maven Central hosts.
- [x] **Full test execution suite**: Executed `pytest -v tools/ci/tests/test_sbt_retry.py` and `tools/ci/tests/test_pipeline_yaml.py` (**95 passed**: 33 `test_sbt_retry.py` unit tests covering symlink defense, multi-host Coursier eviction, single-probe bounding, terminal failure probing without eviction, and fallback resolver regex parsing).
