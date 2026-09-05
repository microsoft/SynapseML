# Release automation boundaries

| Phase | Automation | Approval boundary |
| --- | --- | --- |
| Matrix | Derives a sealed schema-v1 plan | Review scope, commits, coordinates, counters, and feeds |
| Preflight/status | Reads source, policy, destinations, Azure runs, and artifacts; updates local state | No remote writes |
| Version PR | Release Prepare bumps versions/docs and opens a PR | Explicit workflow dispatch, then reviewed PR merge |
| Public tags | Tags recorded merge commits; atomically pushes and confirms derivative families | Version/port PR merges authorize tagging |
| Internal tags | Preview-first clean-source/ref validation and atomic publication | `tag --apply --approve-plan` |
| Maven/pip/UPack | Driver queues missing authorized actions and records build IDs | `resume --apply --approve-plan`; ESRP/SAW remain manual |
| Recovery | Adopts matching runs or retries a terminal failed pip/UPack group with complete absence evidence | Explicit approved `--adopt` or `--retry`; Maven needs a new coordinate and plan |
| Lock inspection | Reads bounded local metadata without Azure calls or file changes | Confirm a dead owner and Azure outcomes before any manual lock removal |
| Evidence | Revalidates producer outcomes, receipts, and artifact visibility | Inventory alone is not approval |
| GitHub Release | Checks the public plan/evidence and current artifacts, then creates primary notes | Manual Release Notes dispatch |
| BBC-VHD | Previews or edits selected pins and component revision | Complete fresh evidence plus `--apply --approve-plan`, then PR review |
| Fabric rollout | Existing image/train deployment systems | BBC-VHD CI, White-Glove, train selection, and monitoring |

Normal CI remains snapshot-only. A clean tagged checkout does not authorize
release coordinates or overwriting existing artifacts. Producer pipelines
validate explicit release inputs before publication.

The driver preserves the sealed plan. Per-run flags may narrow its authorized
operations, never expand them. The publisher's own source SHA is distinct from
the released code SHA and is recorded separately.

Retain the authoritative ledger and its persistent same-directory claim.
Neither a new state filename nor a copied directory is a supported way to
discard failed or unknown work. Lock metadata is not proof of owner death.

Full releases may use staged OSS-first and Internal-only-repository plans.
Internal hotfixes do not run public tag workflows, public Maven publication, or
GitHub Release creation.

Do not remove human approval boundaries by adding credentials to GitHub,
auto-merging PRs, clicking approvals, changing permissions, or editing a ledger.
