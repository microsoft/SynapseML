# Follow-up round 2: exception boundaries and CI wiring

**Result:** CLEAN for the bounded correction.
**Reviewer:** GPT-6 Astra, direct fallback, 2026-09-21.
**Scope:** the confirmation-read follow-up to microsoft/SynapseML#2732.

- The private `tryDeleteItem` helper returns a recorded DELETE error or permits
  confirmation. The caller logs/accumulates returned errors; they are not silent
  fallback results. Read/confirmation errors escape to one fail-fast handler.
- This avoids tagging or wrapping exceptions and preserves their identity.
  The `run` method stays within the existing 50-line style limit without a
  waiver or a mutable state abstraction.
- `pipeline.yaml` selects `FabricTestArtifactTrackerSuite` explicitly. The
  extracted failure tests therefore form a private mix-in on that existing
  suite, not a separate unscheduled suite. The two test bodies remain unchanged.
  No pipeline edit or expansion of CI permissions was needed.
- The exact CI selector ran 43 tests, including both mixed-in cases. Adding the
  existing naming suite ran 46 tests. Test style passed; the main file remains
  below 800 lines.
- Review-record changes only normalize publication paths, preserving original
  findings, hashes, and resolutions. Source references are repository-relative;
  unpublished logs are described as locally retained evidence, not public links.

The helper and mix-in are private test infrastructure. Generated wrappers,
serialized parameters, runtime pins, and production request paths are unchanged.
The unavailable Gemini slot remains explicitly unfulfilled.
