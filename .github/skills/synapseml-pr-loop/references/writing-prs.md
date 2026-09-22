# Writing PRs for reviewers

A reviewer should understand the change and its purpose at first glance.
Write as a teammate explaining the work, not as a commit log or file inventory.

## Lead with the what and why

- Keep the conventional title prefix, but describe the outcome in plain
  language. Prefer `fix: keep string ranking queries separate` to
  `fix: update GroupIdManager`. Avoid vague titles such as "improve handling",
  unexplained acronyms, internal identifiers, and claims larger than the diff.
- Open with two or three short sentences: what changes, who it helps, and why
  the old behavior or workflow needed changing. A small before/after comparison
  can do this better than a technical paragraph.
- Aim for roughly 100 words of overview before implementation detail. This is
  a readability guide, not a quota: a small fix may need only one sentence.
  Use natural, specific language rather than promotional or generated-sounding
  claims, and do not repeat the summary under several headings.
- Keep breaking changes, migration steps, security implications, meaningful
  limitations, and known blockers visible. Include a short, honest validation
  status; distinguish local checks, pending CI, failed CI, and current-head
  evidence. Do not make an unvalidated PR look finished.

## Use a visual when it explains faster than prose

| Change | Useful visual |
| --- | --- |
| Decisions, workflow, or data flow | A small Mermaid flowchart |
| Calls between services or components | A sequence diagram |
| User interface or interaction | Real before/after screenshots or a short recording |
| A measured performance change | A labeled chart with its measurements and baseline |

- Prefer one compact visual showing the important change. Do not force a
  diagram onto a simple fix or add decorative images. A short list or table
  may be clearer.
- GitHub renders [Mermaid diagrams](https://docs.github.com/en/get-started/writing-on-github/working-with-advanced-formatting/creating-diagrams)
  directly in PR descriptions. Use a `mermaid` fenced block for flows rather
  than committing a generated image when editable text is enough.
- Label decisions and important failure paths. Keep diagrams readable at PR
  width, add a short caption or text explanation, and use meaningful alt text
  for images. The visual must agree with the code and the written claims.
- Screenshots must show real behavior. Do not fabricate UI, measurements, or
  test evidence. Redact secrets and private data before attaching media.
  Use GitHub-hosted attachments or approved repository assets; no local paths,
  temporary URLs, or uploads of private content to outside diagram services.
- Check diagram syntax and preview rendering before publishing. Verify image
  links and readability. A diagram supplements the explanation, not the
  evidence that the feature works.

## Disclose detail after the overview

- Preserve the repository's required PR-template sections and fields. Put the
  overview first where the template permits; do not delete required content
  just to shorten the page.
- Move implementation notes, alternatives, long test output, commands, build
  IDs, and supporting links below the overview. Use descriptive
  [expandable sections](https://docs.github.com/en/get-started/writing-on-github/working-with-advanced-formatting/organizing-information-with-collapsed-sections)
  for material that only a deeper review needs.
- Keep the current test/CI status outside the fold; detailed evidence can be
  inside it. Never hide a risk, permission requirement, or blocker there.

```markdown
Local checks passed; full CI is still pending.

<details>
<summary>Implementation and validation details</summary>

Explain the approach, alternatives, exact checks, and evidence links here.

</details>
```

Before publishing, read only the title and opening: can a teammate explain
what changes and why? Then check the visuals, current scope, and visible
status. Update the title/body when authorized, and leave existing discussion
comments intact.
