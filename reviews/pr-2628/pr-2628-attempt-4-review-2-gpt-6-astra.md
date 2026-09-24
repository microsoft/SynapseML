# PR 2628, attempt 4, round 2

**Direct architecture review: no additional concrete finding. Required Gemini coverage is unavailable.**

- Theme: architecture and integration contracts.
- Reviewer: parent assistant, `gpt-6-astra`.
- Base HEAD: `a617374f54c7629da3216112ce8c1196bd735224`; reviewed the current uncommitted release changes.
- The requested `gemini-3.8-flash` review and a narrower `gemini-3.7-flash` fallback both failed before producing a review with HTTP 400. This report does not substitute a claim that the required three-family gauntlet passed.

## Evidence

- `release_matrix.py:269-323` separates the public allowlisted document, exact digest verification, full in-memory re-derivation and execution admission. Public export rejects private in-memory mutations rather than silently removing them.
- `release_matrix.py:335-388,642-698` derives public plans without reading a private profile and checks exact root/target keys before accepting serialized input. Bound reading remains separate from public admission.
- `release_matrix.py:704-732` retains historical configuration only to read sealed legacy documents. Legacy execution requires regeneration and reapproval rather than treating an old approval as permission for the new format.
- `release_config.py` confines optional configuration to an explicit absolute path outside checkouts, bounds file size, rejects duplicate keys/non-finite numbers, validates the complete profile and requires private package identities without production defaults.
- Bootstrap uses the shared notes/public-plan admission before dispatch, preserves the normal master ancestry guard and keeps package submission separate from atomic tag creation.
- `website-deploy.yml` permits unpublished documentation previews only where Pages cannot deploy. The normal master build still requires the publication lock to match. `test_release_workflows.py` checks that separation.
- `bump-version.py` preserves release and website test fixtures, including the last-published lock. Native successive-bump/history validation passed 231 tests.

## Integration findings handled during candidate preparation

The primary candidate exposed a circular requirement: its website check required already-published package coordinates before bootstrap could create the source tags needed to publish them. The explicit non-deploying documentation preview mode removes that cycle without marking unpublished versions as published. Eight installation-documentation tests passed, including refusal of an unpublished production lock.

Both port candidates also exposed a newly added fork-protection assertion that rejected their intentionally disabled Fabric job. The assertion now accepts a literal disabled condition or a real fork guard. Both ports passed the eight focused workflow/fork-contract cases; no Fabric runtime was enabled.

## Limits

These are local source/test results. They do not establish candidate CI, live bootstrap permissions, publication, signed artifact contents or consumer behavior. The failed reviewer invocations remain a missing review gate, not clean reviews. No production tags or packages were created.
