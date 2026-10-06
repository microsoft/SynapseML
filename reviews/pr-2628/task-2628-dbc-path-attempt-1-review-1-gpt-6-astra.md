# PR 2628: Round 1

## Review Summary

**ISSUES_FOUND: 2 findings (1 P1, 1 P2).**

| Field | Value |
| --- | --- |
| Round/theme | 1 - Broad Sweep: correctness, security, logic and spec conformance |
| Actual model | `gpt-6-astra` |
| Mode | Sequential direct-contract |
| Base/merge base | `9d51ad1acd765b3246bec15517abf6b62e1d5d70` |
| HEAD | `3ce916902329c20d5c37de43d7c43d805f28e748`, including the staged DBC directory correction |
| Frozen patch fingerprint | `6f01f334a1231ce5c23ed809780188e551e0ac448a202a024d31690d3997a324` |
| Full diff SHA256 | `9d42a8a8785d3606222a23100abb5d904b2258e4b387d0c2762c2d38429da210` |

## Evidence Checklist

- Reviewed the complete supplied 55-path merge-base-to-index patch, including production code, workflows, documentation, tests and the pending DBC correction. Review scope was not limited to that correction.
- Traced the findings through verification callers, plan selection, profile loading and error handling; inspected narrowly relevant surrounding build source. No prior review verdict was used.
- Static review only: no tests, builds, service requests, cloud operations or Git mutations were performed. The supplied regression and formatting results were not independently reproduced. Endpoint availability was not probed. Runtime wheel compatibility, native-service behavior and production publication remain unverified release gates.

## Issues

### 1. [P1] Mandatory Maven verification still targets the retired CDN resolver

**Location:** `scripts/release/verify_release.py:66`; consumers at lines 296-327.

`MAVEN_BASE` points to `https://mmlspark.azureedge.net/maven`. `Checker.public_maven()` calls `_maven()` without overriding that default, so every public Maven inventory queries the old host. The installation contract in `README.md` requires `https://mmlspark.blob.core.windows.net/maven`, and `project/BlobMavenPlugin.scala:23-42` publishes to and advertises that Blob repository. The removed R-guide compatibility warning also explicitly identifies the old Azure CDN resolver as retired.

**Trigger and consequence:** Verify a release whose artifacts have been published at the supported Blob coordinates. The required Maven rows are checked against a different, retired endpoint: a 404 becomes `MISSING`, while a transport failure raises an error. Neither path checks Blob instead. This affects the driver's `Remote.inventory()` call to `run_plan()` and the release-notes inventory gate, preventing successful verification even when the supported installation artifacts are present. `test_public_maven_uses_release_specific_coordinate` currently asserts the obsolete URLs, so it preserves the defect.

**Minimal fix:** Use the supported Blob Maven base for these mandatory checks and update the URL assertions. Keep Maven Central verification as its separate required check.

### 2. [P2] Public-only historical verification loads a private profile before honoring skips

**Location:** `scripts/release/verify_release.py:486-496`, particularly lines 493-496.

**Trigger:** With no private release profile configured, invoke the documented public-only diagnostic:

```text
python scripts/release/verify_release.py --version 1.1.4 --skip ado,internal
```

`run()` unconditionally constructs a plan containing both repositories and all three artifact families. Consequently, `release_matrix.build_plan()` takes the private branch and calls `load_profile()` at line 378. `release_config.load_profile()` raises when the profile is absent, before `_check_plan()` receives the skip selection; `main()` then returns exit code 2 without checking the public artifacts.

**Consequence:** Explicitly skipping every private-backed check still makes an external private configuration a prerequisite for the advertised public-only inventory. The correctly isolated `--plan` path does not repair this separate CLI path. The verifier tests import an autouse synthetic-profile fixture, which supplies the otherwise missing prerequisite.

**Minimal fix:** Apply the skip selection before deriving the historical inventory and avoid constructing a private plan when no private checks remain. Preserve the unbound historical-check semantics and existing approved-plan identities. Cover this command with the private profile absent.

## Driver resolution notes

This pass ended when its findings required source changes. A new full pass is
required on the corrected patch.

### Maven endpoint

The publisher and supported installation instructions use the Blob Maven
repository. The verifier now checks that same endpoint, while bound release
plans still check Maven Central separately.

The outage claim was not reproduced. Anonymous HEAD requests for the
`synapseml_2.12:1.1.3` POM returned HTTP 200 from both hosts. This is a confirmed
resolver-contract mismatch, not evidence of a current CDN outage. Two
coordinate assertions failed against the previous verifier URL.

### Public-only historical inventory

Six CLI cases reproduced the profile dependency with private configuration
absent. The correction selects public Maven scope before constructing a plan
when all private-backed checks are skipped. It derives a legacy public
inventory, with no DBC requirement or approval authority. Four negative cases
confirm that enabled private checks still require explicit configuration.

Historical checks retain their existing non-strict coverage. Maven Central
remains a required check for bound release plans, not a new obligation for
older unbound diagnostics.
