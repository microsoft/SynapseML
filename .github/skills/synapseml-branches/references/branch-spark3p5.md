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

- Master's own validation covers its primary runtime. Master PRs no longer
  replay patches onto Spark 4.1 automatically.
- Validate port changes and resolved syncs on the actual port branch, preserving
  genuine version-driven resolutions. Pipeline triggers for Spark 4.1 pushes
  and PRs remain enabled; verify the Azure definition also queues the build.
- Verify the selected targets and affected suites actually ran. A green matrix
  can omit a package or test class, and a green master build does not establish
  compatibility with every active port.
