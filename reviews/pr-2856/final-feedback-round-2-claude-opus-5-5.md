# SynapseML PR #2856 — cleanup feedback review, round 2

Reviewer: Claude Opus 5.5 through the existing co-create route; independent Codex agreement required.

Feedback: [r4604058054](https://github.com/microsoft/SynapseML/pull/2856#discussion_r4604058054), Copilot review 5944395440. The previous four findings are marked resolved; this round addresses the subsequently reported setup-failure context leak.

## Source identity

Source base `2982f5a9b198fc3c35e601187887e18b57634faa`; prior published head `9bf2b98823fbcf2baf4101e42a0136dbc251d5b2`. The reviewed cleanup-only diff has SHA-256 `0b1b0e7e36a5e8f1e460e27d59eb44682eeb3f2258f648d580f193e0ea0289f3`.

Integration source commit: `de92878406230518e8ea981751b6d9e53bbbac76`, created after this review from the listed tested postimages and independently checked. This review did not read that future raw commit object.

| Repository-relative path | SHA-256 |
| --- | --- |
| `core/src/main/python/synapse/ml/causal/DoubleMLModel.py` | `a3c6ebf85a7f877d128d5194f243c0fc30d764cb75064e3305ded788b9541aa3` |
| `core/src/main/scala/com/microsoft/azure/synapse/ml/causal/DoubleMLEstimator.scala` | `0b43a14e4760d67dae18360dcb98845e76a4c6d352f98178341684c3acdc465c` |
| `core/src/main/scala/com/microsoft/azure/synapse/ml/causal/DoubleMLMultipleTreatmentResults.scala` | `3ad12f95007d555dfc635c47b980765488728420a5e4e9e9da2a095f974b2478` |
| `core/src/main/scala/com/microsoft/azure/synapse/ml/causal/DoubleMLMultipleTreatmentTraining.scala` | `267e060d5b07aefef267fc4c3cf738df813a4ee95620a9893f0464ee1d7b0a17` |
| `core/src/main/scala/com/microsoft/azure/synapse/ml/causal/HasTreatmentCols.scala` | `dfc3f41bc426afc73aa3376bde42f854e042139dab0a0f2ab47068b33c101c41` |
| `core/src/test/python/synapsemltest/causal/test_double_ml_multiple_treatments.py` | `ce5391a0e64bd2c8175cde8234965ae15971bb652f3b635ea0204e2967a84141` |
| `core/src/test/scala/com/microsoft/azure/synapse/ml/causal/DoubleMLMultipleTreatmentsSuite.scala` | `ef1bb659ab46ab2278c724bbdf41f563e3eef12d5284e1eff1b92f92a2a70592` |
| `core/src/test/scala/com/microsoft/azure/synapse/ml/causal/VerifyDoubleMLEstimator.scala` | `64859654ffb438ffc69b113674d2c319aa1f2c38e83bc42e7f8497410c6d1e87` |
| `docs/Explore Algorithms/Causal Inference/Quickstart - Measure Causal Effects.ipynb` | `561100f9bee680a89178a0a736116d539999a52b88314bd20f5593ae03375e11` |
| `docs/Quick Examples/estimators/causal/_causalInferenceDML.md` | `187d47aa8446a2dd56eeb4b255f8b59d9a86e7f334fe31ce54aba7a35535009f` |

## Resolution and actual validation

Immediately register `cls.spark.stop` with `addClassCleanup` after creating an owned context, and remove the redundant `tearDownClass`. Borrowed contexts are untouched.

[I4](https://github.com/leninworld/SynapseML/actions/runs/44156511453), diagnostic commit `9604f9c5ebbb0e0dd489f7b7a44d7cb15f9eae1d`, passed agent-controlled hosted Docker build/test compile, 14 ScalaStyle checks, Black (225 unchanged), fresh wheel/JAR/codegen/install, nine Python methods with 23 successful subtests, and two generated constructor tests. Installed wheel identities/module paths were verified for both consumers; explicit JVM origins were recorded in the generated consumer. Source hashes matched before/after; all owned containers exited 0 without OOM.

After normal pytest completion proved owned-context cleanup, four separate real-Spark controls through stdlib unittest reproduced the exact prior fixture's leak and verified repaired owned provenance/initial-fit failure cleanup and borrowed-context preservation. Each intentionally produced one setup error and ran zero test methods; they are not added to the scientific test count. Failed setup was not directly induced through pytest.

All five production files, both Scala tests and both docs are unchanged from the previously accepted scope. I2's successful Scala lanes remain reusable, while I2 overall remains failed. Earlier website/base qualifications and warnings remain. No full Azure Pipelines/live-service acceptance, all-thirty-draws success, maintainer approval, thread resolution, legal certification or user-reading fact is asserted.

Actual final verdict: GO: I4 results and exact cleanup source accepted within the stated scope; publication steps remain separately gated.
Actual read coverage: Read the cleanup diff, the changed test-file region, the plugin's control and session-finish code, both actual pytest XML reports, the handwritten session and cleanup-control records, the results summary, report and public templates. Unchanged postimages were read in earlier rounds under identical SHA-256. Artifact manifests, logs and remaining evidence files relied on hash verification. The future integration commit objects were not read.
A later published-revision verification is separate from this review.
