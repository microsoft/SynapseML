# Master branch context

Master is the canonical development branch. Portable fixes land here before
being merged into the active ports. A matching Spark generation does not make a
historical release branch active; use the [active scope](../SKILL.md#active-branches).

## Compatibility boundaries

- Read versions from the [target's source files](../SKILL.md#sources-of-truth).
  Do not import a port's Scala, JDK, Python, or dependency settings merely because
  they make its tests pass.
- Check the JDK used by each setup, compilation, publication, and compatibility
  job. A Java setup template does not control every stage. Shared helpers must
  use APIs supported by the oldest JDK that actually builds them.
- Class-loader APIs can differ across JDK generations. A launcher that compiles
  on a newer JDK can still break older build stages.
- Apply the [portable sync lessons](branch-spark4-common.md#portable-sync-lessons)
  to codegen, defaults, package exports, and artifact coordinates here too.
  Validate against master's own runtime and Scala version.

## Release validation

- Replay uses the selected release target, not an unmerged sync proposal.
  Recheck the target and prerequisite baseline when content has moved.
- Preserve genuine port resolutions and conflict rejection. Validate resolved
  syncs separately; do not weaken patch application or skip a required leg.
- A replay covers only its selected branch and checks. It does not establish
  full compatibility for every active port.
