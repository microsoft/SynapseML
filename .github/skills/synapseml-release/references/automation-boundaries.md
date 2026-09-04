# Release automation boundaries

The release is deliberately split between repeatable automation and
credentialed human decisions.

| Phase | Trigger | Automation | Human gate |
| --- | --- | --- | --- |
| Plan | Release engineer runs `release_matrix.py` | Validates inputs and prints tags, package coordinates, and queue commands | Review and accept the plan |
| Preview | Release engineer runs dry-run commands | Reports version and BBC-VHD edits without writing | Decide whether to start |
| Version PR | **Release Prepare** runs on the canonical branch | Bumps versions, snapshots docs, pushes a branch, opens a PR, and starts checks | Review and merge the PR |
| Primary tag | Version PR merges | Verifies the merge and tags that exact commit | The PR merge is the approval |
| Derivative work | Primary tag is created | Creates canonical derivative tags and opens ordered Spark release PRs | Review and merge each Spark PR |
| Spark tags | Spark release PR merges | Tags the recorded merge commit and removes the temporary branch | Verify content and merge order |
| Maven | Engineer runs matrix commands | Public and Internal tag builds create Maven artifacts | Queue access plus ESRP and SAW approval |
| pip and UPack | Engineer runs Publish-Official | Builds selected pip and UPack packages | Queue and publication approval |
| Artifact proof | Engineer runs `verify_release.py` | Reads live tags and package stores and reports missing rows | Investigate every missing row |
| GitHub Release | Engineer runs **Release Notes** on the primary tag | Verifies public artifacts, generates notes, and creates one Release | Select the tag and start the workflow |
| BBC-VHD | Engineer runs `bump_bbcvhd.py` | Calculates or writes the package and component revision changes | Review, CI, and White-Glove approval |
| Fabric rollout | Release train deploys | Platform deployment follows the train schedule | Select and monitor the train |

## State-changing points

- **Release Prepare** is the first repository write. It creates a branch,
  commit, and pull request.
- Merging the version PR creates the primary tag and starts derivative
  automation.
- Merging each Spark PR creates its derivative tags.
- Queueing Maven or Publish-Official can create immutable package versions.
- **Release Notes** creates the public GitHub Release.
- `bump_bbcvhd.py` writes only when `--dry-run` is absent.

## Actions that remain manual

- Accepting the release plan.
- Approving and merging every release pull request.
- Queueing public and Internal Maven builds.
- Completing ESRP and SAW approvals.
- Queueing Publish-Official.
- Reviewing and merging the BBC-VHD pull request.
- Completing White-Glove approval.
- Selecting and monitoring the Fabric release train.

Do not add credentials or automatic approvals to remove these boundaries.
