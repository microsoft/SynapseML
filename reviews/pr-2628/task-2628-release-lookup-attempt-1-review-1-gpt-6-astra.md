# Release lookup fix: round 1

- Theme: correctness and scope.
- Reviewer: GPT-6 Astra, direct coordinator review.
- Reviewed base: `55b5ed5fb91e75e081429e9da7c3501378c3640d`.
- Target: `384f27a5d0e01271f67fdc81505c37e3016d52ea`.
- Finding: https://github.com/microsoft/SynapseML/pull/2628#discussion_r4098469595

The workflow now treats only a completed HTTP 404 as absence. HTTP 200 keeps
the existing release; every other status and every transport failure stops.
The change does not alter plan approval, source integration or artifact gates.

Before the fix, the regression passed its two normal cases and failed all
11 error cases because publication proceeded. After the fix, all 13 pass.
No outstanding issue found in this bounded change.

These six themed passes are one coordinator's review, not independent
multi-model coverage. They do not approve production publication.
