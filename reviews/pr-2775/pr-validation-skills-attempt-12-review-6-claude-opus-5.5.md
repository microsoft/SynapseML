# Review Round 6 — Polish & Hardening

## Summary
- Round: 6
- Theme: Polish & Hardening
- Mode: manual prompt fallback
- Requested model: claude-opus-5.5 (max)
- Issues: 0
- Verdict: CLEAN

## Evidence Checklist
- [x] `.github/skills/synapseml-pr-loop/references/ci-evidence.md:30-37` — The readiness table matches the fields the script outputs at `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1:448-461,548-559,572-573`. That includes `completeness.complete`, which is only an alias for "remote engineering gates clear."
- [x] `.github/skills/synapseml-pr-loop/references/ci-evidence.md:39-44` — The documented `-RunPipeline` rules match `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1:97-102,496-510`:
  - one PR only, with both expected SHAs
  - cannot be combined with `-WaitForReview`
  - evidence must be complete and the PR must be open
  - `/azp run` is posted only when the `microsoft.SynapseML` aggregate is missing
  - at most one request per PR per run
- [x] `.github/skills/synapseml-pr-loop/references/ci-triage.md:38-56` — The documented watcher outcomes, exit codes and `--repo` handling match `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py:210,251,263,308-312,374-375`. Both `azure_completed` and `attention_required` exit with code 4. `--repo` is compared without regard to case and rejected before any query.
- [x] `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py:33-45,107-114` — Error messages leave out CLI stderr, HTTP response bodies and the bearer token. `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py:16-23` turns off bytecode writing (`sys.dont_write_bytecode`) before it imports the collector, so running the watcher from the verified guidance snapshot doesn't add `__pycache__` files to it.
- [x] `pipeline.yaml:112-126` — The CIHelpers job runs `python3 -m pytest tools/ci/tests/ -q` on ubuntu-22.04 (Python 3.10) and installs only pytest and pyyaml. The new helpers use nothing that needs Python 3.11 or later, and every import is used.

## Publication record

- PR: microsoft/SynapseML#2775
- Source publication commit: `66d08e29cc9baf9d74a71f69ce7f59c6b9f1d299`
- Reviewed input manifest SHA-256: `1d7987ebe5de682ada6cfd4c0f66db85d028ebd8e1418f424f0f9b69d54d6636`
- Reviewed diff SHA-256: `df7ac7c309f23bf644e55765296cb4a92594b70392e178c7040cf8616bf65733`
- Original artifact SHA-256: `2ac87cec9e4c625aeb93f2bb085ee68b523e978b0206d5110d01ba22052bb2b8`
- Dispatch mode: manual prompt fallback; no observed CLI receipt is claimed.
