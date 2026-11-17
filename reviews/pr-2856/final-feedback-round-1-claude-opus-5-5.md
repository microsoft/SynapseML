# SynapseML PR #2856 — final feedback source/results review

Review stage: final feedback integration, round 1. Reviewer: Claude Opus 5.5 through the existing co-create session; independent Codex agreement required.

Reviewed the exact ten postimages listed below and the actual results of runs at diagnostic commits `519c0d89af2cba8939f6ea8baab98ad85eac8257` and `542533b88bafecd313493874688c01431d5ef71a`, on upstream `2982f5a9b198fc3c35e601187887e18b57634faa`. The diagnostic commit objects were not read.

| Repository-relative path | SHA-256 |
| --- | --- |
| `core/src/main/python/synapse/ml/causal/DoubleMLModel.py` | `a3c6ebf85a7f877d128d5194f243c0fc30d764cb75064e3305ded788b9541aa3` |
| `core/src/main/scala/com/microsoft/azure/synapse/ml/causal/DoubleMLEstimator.scala` | `0b43a14e4760d67dae18360dcb98845e76a4c6d352f98178341684c3acdc465c` |
| `core/src/main/scala/com/microsoft/azure/synapse/ml/causal/DoubleMLMultipleTreatmentResults.scala` | `3ad12f95007d555dfc635c47b980765488728420a5e4e9e9da2a095f974b2478` |
| `core/src/main/scala/com/microsoft/azure/synapse/ml/causal/DoubleMLMultipleTreatmentTraining.scala` | `267e060d5b07aefef267fc4c3cf738df813a4ee95620a9893f0464ee1d7b0a17` |
| `core/src/main/scala/com/microsoft/azure/synapse/ml/causal/HasTreatmentCols.scala` | `dfc3f41bc426afc73aa3376bde42f854e042139dab0a0f2ab47068b33c101c41` |
| `core/src/test/python/synapsemltest/causal/test_double_ml_multiple_treatments.py` | `e566433f3c6074ea1389d8f8fc354043757901a0e3072b6102f64669fbec28f8` |
| `core/src/test/scala/com/microsoft/azure/synapse/ml/causal/DoubleMLMultipleTreatmentsSuite.scala` | `ef1bb659ab46ab2278c724bbdf41f563e3eef12d5284e1eff1b92f92a2a70592` |
| `core/src/test/scala/com/microsoft/azure/synapse/ml/causal/VerifyDoubleMLEstimator.scala` | `64859654ffb438ffc69b113674d2c319aa1f2c38e83bc42e7f8497410c6d1e87` |
| `docs/Explore Algorithms/Causal Inference/Quickstart - Measure Causal Effects.ipynb` | `561100f9bee680a89178a0a736116d539999a52b88314bd20f5593ae03375e11` |
| `docs/Quick Examples/estimators/causal/_causalInferenceDML.md` | `187d47aa8446a2dd56eeb4b255f8b59d9a86e7f334fe31ce54aba7a35535009f` |

Integration source commit: upstream `2982f5a9b198fc3c35e601187887e18b57634faa` plus the listed postimages, repository tree `90b4ad07ffb0dbe2868369e988122c82cc63e1f3`, created after this review and independently checked. This review did not read that future commit object.

Actual final review disposition: CONDITIONAL_GO: tested source and scoped composite accepted; corrective publication conditions remain separately enforced. Actual read coverage: Read all ten listed postimages and the installed-result record, both Python XML reports, generated test source and compile/style log. Session data embedded in the result was read; separate artifact/session JSONs and event/control records relied on hash verification. Unchanged Scala results were read in the preceding review stages. Diagnostic commit objects were not read. The final review packet fingerprint is `0cb7db17c9484a8cd334b9d116e07ca4e46f3921a754c2ff5536cbb1f38c918e`.

## Findings and resolution

- Human review [5936081260](https://github.com/microsoft/SynapseML/pull/2856#pullrequestreview-5936081260): categorical treatment metadata is rejected before fitting; offset auxiliary inputs are remapped on nuisance copies without mutating callers; ambiguous singular/plural model statistics reject copied and reloaded mixed-route models.
- Earlier Copilot inline findings 4595850829/4595850866/4595850784 map to those same repairs. Inline 4595850737 maps to a plural-only per-fold MLP layers clone. The later human review identifies that MLP behavior as inherited in the singular path; it is not attributed to a new singular regression.
- `copyForPrediction` extracts the identical per-fold copy/clone block to satisfy ScalaStyle. It preserves the nuisance parameters and call timing.

## Actual validation

Agent-controlled **hosted Docker**, exact source base and postimages above:

- [I2](https://github.com/leninworld/SynapseML/actions/runs/44128124454): full 32 plural + 10 legacy DML cases passed, including all eight feedback regressions and real copy/reload, categorical and GLR offset behavior. The separate legacy lane passed 10 cases, registration 1 and root fuzzing 10 with one pre-existing ignore. Repeated legacy cases are not additional unique tests.
- [I3](https://github.com/leninworld/SynapseML/actions/runs/44135619861): full root compile/test compile, 14 style checks and Black (225 files unchanged); fresh wheel/JAR/codegen/install; nine handwritten Python methods with 23 successful subtests and two generated constructors; installed wheel identity/module paths for both invocations, JVM class origins in the generated invocation, and source equality before/after passed.
- The report-checker correction retains exact method/class, failure/error/skip and total checks. Real pinned-pytest success/failing-subtest controls and five negative XML controls passed before the expensive build.

I1 and I2 remain failed runs; this is an explicitly scoped composite of I2's successful Scala lanes and I3's successful installed lane. All ten product postimages are identical across these components. Original website evidence remains limited to unchanged documentation/site inputs on its original base; no new website run is claimed here.

No live Azure/WASBS, full Azure Pipelines or hosted-notebook acceptance. No native LightGBM auxiliary-fit proof; `censorCol` is unsupported and auxiliary columns remain covariates. Solver fallback, deprecation and post-success temporary Hadoop JAR shutdown diagnostics are retained. `allThirtyDrawsSucceeded` remains unestablished; a passing B30 case is not that claim.

No maintainer approval, merge, thread resolution, legal certification or user reading is asserted by this record. A later published-revision readback is separate from this private final review.
