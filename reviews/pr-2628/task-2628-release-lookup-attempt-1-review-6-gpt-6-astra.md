# Release lookup fix: round 6

- Theme: documentation and final polish.
- Reviewer: GPT-6 Astra, direct coordinator review.
- Reviewed base: `55b5ed5fb91e75e081429e9da7c3501378c3640d`.

The operator guide now states the 404-only creation rule and tells operators
to resolve lookup failures before retrying. Workflow errors distinguish an
incomplete request from an unexpected HTTP status.

The change is limited to release lookup behavior, its regression and related
documentation. Pinned Python formatting and whitespace checks passed. No
additional hardening work was added outside the confirmed finding.

No outstanding issue found in the bounded fix. These are six direct review
passes, not independent multi-model reviews. Current-head hosted CI and
review must still run after pushing. The separate Python-wheel release
blocker remains open.
