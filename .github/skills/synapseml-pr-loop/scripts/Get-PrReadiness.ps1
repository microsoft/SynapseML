#Requires -Version 7.2

<#
.SYNOPSIS
    Captures GitHub merge-readiness evidence for one or more pull requests.
.DESCRIPTION
    Reports head/base state, target divergence, checks, active review threads,
    and review bodies containing suppressed comments. Review threads and reviews
    are fully paginated, and the emitted `completeness` object reports page
    counts plus any thread whose comments were truncated, so an incomplete
    snapshot is visible rather than silent.

    Automated review is asynchronous, so a snapshot taken right after a push
    describes the previous head and can report zero findings for code nobody has
    reviewed yet. `automatedReviewCoversHead` compares the newest automated
    review's commit with the current head and gates `complete`, and
    `suppressedReviewBodiesForHead` narrows suppressed feedback to that head.
    Poll until it is true rather than trusting a single clean snapshot.

    A pull request that was never reviewed at all reports the same zero findings,
    so `-RequestReview` asks for one as a fallback. Review is normally triggered
    automatically, so the usual need is not to ask but to wait: `-WaitForReview`
    blocks until the review of the current head arrives, instead of returning a
    premature all-clear and leaving a human to notice the comments later.
    Suppressed comments are reported too: they live inside the review body rather
    than as review threads, so a thread query alone never surfaces them.
    Requesting a review is limited to one open PR with stable revision evidence,
    but does not require unrelated policy/workflow evidence to be complete or
    authorize CI.

    Checks are reported by absence as well as outcome. `failedChecks` and
    `pendingChecks` can only describe checks that exist, so a head whose build
    never started scores zero in both and reads as finished;
    `missingRequiredChecks` catches that and gates `complete`. The Azure DevOps
    build is the usual casualty because it does not queue itself on a push -- it
    waits for an `/azp run` comment. The guarded `-RunPipeline` posts one only
    for a single open PR with matching expected head/base SHAs, complete evidence,
    and an absent exact Azure aggregate. It does not authorize CI and cannot be
    combined with `-WaitForReview` or `-RequestReview`.

    Workflow runs (including hidden approval gates), rulesets, and classic
    branch protection are collected separately from the check rollup. The
    readiness object separates evidence, remote engineering checks, human
    approval, and merge readiness. Local behavior and review evidence still
    require assessment; completeness.complete is a compatibility alias for
    remoteEngineeringGatesClear, NOT merge approval.
    Output can contain review content; keep it local or redact it before sharing.
#>
param(
    [Parameter(Mandatory)]
    [int[]]$PullRequest,

    [string]$Repo = "microsoft/SynapseML",

    # Logins whose reviews count as automated coverage of the head commit. Matched
    # exactly, with an optional "[bot]" suffix, so a human account that merely
    # contains one of these words is never mistaken for the reviewer. Defaults to
    # this repo's reviewer bot only: shorter generic logins such as "copilot" are
    # registerable by humans, and an exact match on one of those would count a
    # human review as automated coverage. Pass -AutomatedReviewer to extend.
    [string[]]$AutomatedReviewer = @("copilot-pull-request-reviewer"),

    # Block until the automated review of the current head arrives. Review is
    # triggered automatically on push but lands afterwards, so a snapshot taken
    # immediately reports zero findings for code nobody has reviewed yet. Waiting
    # here keeps that from being mistaken for a clean result.
    [switch]$WaitForReview,

    [ValidateRange(1, 240)]
    [int]$TimeoutMinutes = 20,

    [ValidateRange(5, 600)]
    [int]$PollSeconds = 60,

    # Fallback only. Review is normally triggered automatically; use this when a
    # pull request has somehow never been reviewed at all, which reports the same
    # zero findings as a clean review.
    [switch]$RequestReview,

    # Handle used when requesting that review. This is the requestable handle, not
    # the login the submitted review is attributed to.
    [string]$ReviewerHandle = "Copilot",

    # Additional required check prefixes, matched case-insensitively.
    # The exact SynapseML Azure aggregate is always required.
    [string[]]$RequiredCheck = @(),

    # Post `/azp run` only when the exact Azure aggregate is absent from the head.
    # Expected revisions and the caller's trusted authorization gate are required.
    [switch]$RunPipeline,

    [ValidatePattern('^[0-9a-fA-F]{40}$')]
    [string]$ExpectedHeadSha,

    [ValidatePattern('^[0-9a-fA-F]{40}$')]
    [string]$ExpectedBaseSha
)

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $false
$azureCheck = "microsoft.SynapseML"

if (($ExpectedHeadSha -or $ExpectedBaseSha) -and $PullRequest.Count -ne 1) {
    throw "Expected revision arguments apply to exactly one PR."
}
if ($RequestReview -and $PullRequest.Count -ne 1) {
    throw "-RequestReview requires exactly one PR."
}
if ($RunPipeline -and ($WaitForReview -or $RequestReview -or -not $ExpectedHeadSha -or -not $ExpectedBaseSha)) {
    throw "-RunPipeline requires expected head/base SHAs and cannot be combined with -WaitForReview or -RequestReview."
}

if (-not (Get-Command gh -ErrorAction SilentlyContinue)) {
    throw "GitHub CLI 'gh' is required."
}

$repoParts = $Repo.Split("/")
if ($repoParts.Count -ne 2 -or -not $repoParts[0] -or -not $repoParts[1]) {
    throw "Repo must use owner/name format; got '$Repo'."
}
$owner = $repoParts[0]
$name = $repoParts[1]

$automatedLogins = @($AutomatedReviewer |
    Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
    ForEach-Object { ($_.Trim() -replace '\[bot\]$', '').ToLowerInvariant() } |
    Where-Object { $_ })
if (-not $automatedLogins) {
    throw "AutomatedReviewer must contain at least one non-blank login."
}

# Review requests are attempted at most once per PR per invocation; see the
# -RequestReview block in Get-PrSnapshot.
$script:reviewRequestOutcome = @{}

$threadQuery = @'
query($owner: String!, $name: String!, $number: Int!, $cursor: String) {
  repository(owner: $owner, name: $name) {
    pullRequest(number: $number) {
      reviewThreads(first: 100, after: $cursor) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          isResolved
          isOutdated
          path
          line
          comments(first: 100) {
            pageInfo { hasNextPage }
            nodes {
              author { login }
              body
              url
            }
          }
        }
      }
    }
  }
}
'@

$reviewQuery = @'
query($owner: String!, $name: String!, $number: Int!, $cursor: String) {
  repository(owner: $owner, name: $name) {
    pullRequest(number: $number) {
      reviews(first: 100, after: $cursor) {
        pageInfo { hasNextPage endCursor }
        nodes {
          submittedAt
          body
          commit { oid }
          author { login }
        }
      }
    }
  }
}
'@

function Invoke-PagedQuery {
    param(
        [Parameter(Mandatory)][string]$Query,
        [Parameter(Mandatory)][string]$Owner,
        [Parameter(Mandatory)][string]$Name,
        [Parameter(Mandatory)][int]$Number,
        [Parameter(Mandatory)][scriptblock]$Select,
        [Parameter(Mandatory)][string]$Description
    )

    # GraphQL responses are sparse: absent fields are simply missing rather than
    # null, and this function reads them positionally. Under StrictMode that is a
    # terminating error, so a caller with StrictMode enabled would get no snapshot
    # at all instead of a snapshot reporting what is missing. Scoped to this
    # function so dot-sourcing the script cannot disable StrictMode session-wide.
    Set-StrictMode -Off

    $nodes = @()
    $cursor = $null
    $pages = 0

    do {
        $requestCursor = $cursor
        $ghArgs = @("api", "graphql", "-f", "query=$Query",
                    "-F", "owner=$Owner", "-F", "name=$Name", "-F", "number=$Number")
        if ($cursor) { $ghArgs += @("-F", "cursor=$cursor") }

        $text = & gh @ghArgs
        if ($LASTEXITCODE -ne 0) {
            throw "$Description failed for PR #$Number"
        }

        $response = $text | ConvertFrom-Json
        if ($response.PSObject.Properties['errors'] -and $response.errors) {
            $messages = @($response.errors | ForEach-Object { $_.message }) -join "; "
            throw "$Description returned GraphQL errors for PR #${Number}: $messages"
        }
        $pullRequest = $response.data.repository.pullRequest
        if (-not $pullRequest) {
            throw "$Description returned no pull request data for PR #$Number"
        }
        $connection = & $Select $pullRequest
        if (-not $connection -or -not $connection.pageInfo) {
            throw "$Description returned an incomplete connection for PR #$Number"
        }
        $nodes += @($connection.nodes)
        $cursor = $connection.pageInfo.endCursor
        $hasNext = $connection.pageInfo.hasNextPage
        $pages++

        # Guard against a cursor that never advances rather than looping forever.
        if ($hasNext -and (-not $cursor -or $cursor -eq $requestCursor)) {
            throw "$Description reported more pages without advancing the cursor for PR #$Number"
        }
    } while ($hasNext)

    [pscustomobject]@{ nodes = $nodes; pages = $pages }
}

function Get-RestEvidence {
    param([string]$Endpoint, [switch]$Paged)

    $ghArgs = @("api", $Endpoint)
    if ($Paged) { $ghArgs += @("--paginate", "--jq", "tojson") }
    $text = & gh @ghArgs 2>$null
    if ($LASTEXITCODE -ne 0) {
        $script:collectionErrors.Add(
            "Could not read $Endpoint (GitHub CLI exit $LASTEXITCODE); check authentication, permissions, and network access.")
        if ($Endpoint -like "*/protection") {
            $script:collectionErrors.Add(
                "Reading classic branch protection requires repository Administration read permission.")
        }
        return $null
    }
    try {
        $body = ($text -join "").Trim()
        if ([string]::IsNullOrWhiteSpace($body) -or $body -eq "null") {
            throw "Empty evidence."
        }
        if ($Paged) {
            # One compact JSON page per line works without newer gh --slurp.
            foreach ($page in $text) {
                if ([string]::IsNullOrWhiteSpace($page) -or $page.Trim() -eq "null") {
                    throw "Empty evidence page."
                }
                $page | ConvertFrom-Json
            }
        } else {
            $text | ConvertFrom-Json
        }
    } catch {
        $script:collectionErrors.Add("Invalid JSON from $Endpoint.")
    }
}

function Get-VerifiedRunAssociations {
    param($Run)

    $Run.pull_requests | Where-Object {
        $_.number -gt 0 -and $_.base.repo.url -eq "https://api.github.com/repos/$Repo" -and
        $_.head.sha -eq $Run.head_sha
    }
}

function Get-HeadRepositoryName {
    param($PullRequest)

    if ($PullRequest.headRepository.nameWithOwner) {
        return $PullRequest.headRepository.nameWithOwner
    }
    if ($PullRequest.headRepositoryOwner.login -and $PullRequest.headRepository.name) {
        return "$($PullRequest.headRepositoryOwner.login)/$($PullRequest.headRepository.name)"
    }
    return $null
}

function Get-PrSnapshot {
    param([int]$number)

    # Same reason as Invoke-PagedQuery: sparse GraphQL fields are read
    # positionally here, and StrictMode is scoped to this function so
    # dot-sourcing cannot change the caller's session.
    Set-StrictMode -Off

    $script:collectionErrors = [System.Collections.Generic.List[string]]::new()
    $jsonFields = "number,title,state,isDraft,mergeable,mergeStateStatus,reviewDecision," +
        "headRefOid,headRefName,headRepository,headRepositoryOwner,isCrossRepository," +
        "baseRefOid,baseRefName,statusCheckRollup,url"
    $viewText = & gh pr view $number --repo $Repo --json $jsonFields
    if ($LASTEXITCODE -ne 0) {
        throw "gh pr view failed for PR #$number"
    }
    $view = $viewText | ConvertFrom-Json

    $threadPage = Invoke-PagedQuery -Query $threadQuery -Owner $owner -Name $name `
        -Number $number -Description "GraphQL review-thread query" `
        -Select { param($pr) $pr.reviewThreads }

    $reviewPage = Invoke-PagedQuery -Query $reviewQuery -Owner $owner -Name $name `
        -Number $number -Description "GraphQL review query" `
        -Select { param($pr) $pr.reviews }

    # Escape the base ref: branch names legitimately contain '/', which would
    # otherwise produce an invalid REST path.
    $baseRef = [uri]::EscapeDataString($view.baseRefName)
    $branch = Get-RestEvidence "repos/$Repo/branches/$baseRef"
    $baseSha = $branch.commit.sha
    if ($branch.protected -isnot [bool] -or $branch.commit.sha -notmatch '^[0-9a-fA-F]{40}$') {
        $script:collectionErrors.Add("Branch protection metadata is incomplete.")
    }
    $rulePages = @(Get-RestEvidence "repos/$Repo/rules/branches/$baseRef" -Paged)
    $rules = @($rulePages | ForEach-Object { $_ } | Where-Object { $_ })
    if (@($rules | Where-Object { -not $_.type }).Count -gt 0) {
        $script:collectionErrors.Add("Effective branch rules are malformed.")
    }
    # Copilot automation is covered by the current-head review gate below.
    # Deletion and history-rewrite restrictions do not gate an ordinary PR merge.
    $unevaluatedRules = @($rules | Where-Object {
        $_.type -and $_.type -notin @(
            "required_status_checks", "pull_request", "copilot_code_review",
            "deletion", "non_fast_forward")
    })
    $classic = $null
    # protected=true also covers rulesets. enabled=false explicitly rules out
    # classic protection; a 404 from /protection alone could be a permission error.
    if ($branch -and $branch.protection.enabled -ne $false -and $branch.protected) {
        $classic = Get-RestEvidence "repos/$Repo/branches/$baseRef/protection"
    }
    $unevaluatedClassic = @($classic.PSObject.Properties | Where-Object {
        $_.Name -notin @(
            "url", "required_status_checks", "required_pull_request_reviews",
            "required_conversation_resolution", "enforce_admins",
            "allow_force_pushes", "allow_deletions", "block_creations", "allow_fork_syncing") -and
        $null -ne $_.Value -and $_.Value -ne $false -and $_.Value.enabled -ne $false
    } | ForEach-Object { $_.Name })
    $ruleChecks = @($rules | Where-Object { $_.type -eq "required_status_checks" } |
        ForEach-Object { $_.parameters.required_status_checks } |
        Where-Object { $_.context })
    $exactRequiredChecks = @(@($azureCheck) +
        @($ruleChecks | ForEach-Object { $_.context }) +
        @($classic.required_status_checks.contexts) +
        @($classic.required_status_checks.checks | ForEach-Object { $_.context }) |
        Where-Object { $_ } | Sort-Object -CaseSensitive -Unique)
    $approvalRules = @($rules | Where-Object { $_.type -eq "pull_request" } |
        ForEach-Object { $_.parameters })
    if ($classic.required_pull_request_reviews) {
        $approvalRules += $classic.required_pull_request_reviews
    }
    $rulesComplete = $script:collectionErrors.Count -eq 0

    $compare = $null
    if ($baseSha -match '^[0-9a-fA-F]{40}$') {
        # PR baseRefOid may lag the branch tip until the next head push.
        $compare = Get-RestEvidence "repos/$Repo/compare/$baseSha...$($view.headRefOid)"
    }
    $workflowPages = @(Get-RestEvidence `
        "repos/$Repo/actions/runs?head_sha=$($view.headRefOid)&per_page=100" -Paged)
    $allWorkflows = @($workflowPages | ForEach-Object { $_.workflow_runs } | Where-Object { $_ })
    $expectedWorkflowCount = ($workflowPages | Select-Object -First 1).total_count
    if ($null -eq $expectedWorkflowCount -or $allWorkflows.Count -lt $expectedWorkflowCount) {
        $script:collectionErrors.Add("Workflow run pagination is incomplete.")
    }
    if (@($allWorkflows | Where-Object {
        $_.head_sha -ne $view.headRefOid -or -not $_.workflow_id -or -not $_.id -or -not $_.event
    }).Count -gt 0) {
        $script:collectionErrors.Add("Workflow identities do not match the requested head.")
    }
    $uniqueForkBranch = $false
    $headRepo = Get-HeadRepositoryName $view
    if ($view.isCrossRepository -and $headRepo -and $view.headRefName -and
        @($allWorkflows | Where-Object {
            $_.event -like "pull_request*" -and @(Get-VerifiedRunAssociations $_).Count -eq 0
        }).Count -gt 0) {
        $headFilter = [uri]::EscapeDataString("$($view.headRepositoryOwner.login):$($view.headRefName)")
        $headPages = @(Get-RestEvidence "repos/$Repo/pulls?head=$headFilter&state=all&per_page=100" -Paged)
        if (@($headPages | ForEach-Object { $_ } | Where-Object {
            -not $_.number -or $_.head.sha -notmatch '^[0-9a-fA-F]{40}$' -or
            -not $_.head.ref -or -not $_.head.repo.full_name -or -not $_.base.repo.full_name
        }).Count -gt 0) {
            $script:collectionErrors.Add("PR head association lookup returned incomplete identities.")
        }
        # A closed PR can have run this revision before its recorded head moved.
        # Current head SHAs cannot disambiguate historical empty-association runs.
        $headPulls = @($headPages | ForEach-Object { $_ } | Where-Object {
            $_.head.ref -eq $view.headRefName -and
            $_.head.repo.full_name -eq $headRepo -and $_.base.repo.full_name -eq $Repo
        })
        $uniqueForkBranch = $headPulls.Count -eq 1 -and $headPulls[0].number -eq $number -and
            $headPulls[0].head.sha -eq $view.headRefOid
    }
    $scopedWorkflows = @($allWorkflows | Where-Object {
        if ($_.head_sha -ne $view.headRefOid) { return $false }
        if ($_.event -like "pull_request*") {
            $associations = @(Get-VerifiedRunAssociations $_)
            if ($associations.Count -eq 0 -and $uniqueForkBranch -and
                $_.head_repository.full_name -eq $headRepo -and $_.head_branch -eq $view.headRefName) {
                return $true
            }
            if ($associations.Count -ne 1 -or $associations[0].number -le 0) {
                $script:collectionErrors.Add("Workflow run $($_.id) has no unambiguous PR association.")
                return $false
            }
            return $associations[0].number -eq $number
        }
        return $true
    })
    # Scope PR-triggered runs before supersession: another PR can share this head.
    $workflows = @($scopedWorkflows |
        Group-Object workflow_id,event | ForEach-Object {
            $_.Group | Sort-Object { [long]$_.id } -Descending | Select-Object -First 1
        } | Select-Object id,workflow_id,name,event,head_sha,head_repository,head_branch,status,conclusion,run_attempt,html_url,pull_requests)
    $workflowApprovalsRequired = @($workflows | Where-Object {
        $_.conclusion -eq "action_required" -or $_.status -in @("action_required", "waiting")
    })
    $failedWorkflows = @($workflows | Where-Object {
        $_.conclusion -in @("failure", "cancelled", "timed_out", "stale", "startup_failure")
    })
    $pendingWorkflows = @($workflows | Where-Object {
        $_.status -ne "completed" -or -not $_.conclusion
    })

    $failedChecks = @($view.statusCheckRollup | Where-Object {
        $_.conclusion -in @("FAILURE", "CANCELLED", "TIMED_OUT", "ACTION_REQUIRED", "STALE") -or
        $_.state -in @("ERROR", "FAILURE")
    } | ForEach-Object { if ($_.name) { $_.name } else { $_.context } })

    $pendingChecks = @($view.statusCheckRollup | Where-Object {
        ($_.status -and $_.status -ne "COMPLETED") -or
        $_.state -in @("EXPECTED", "PENDING")
    } | ForEach-Object { if ($_.name) { $_.name } else { $_.context } })

    # A check that never started cannot fail and cannot be pending, so it leaves
    # no trace in either set above. Require the exact Azure aggregate; other
    # caller-supplied names retain legacy prefix matching.
    $checkNames = @($view.statusCheckRollup |
        ForEach-Object { if ($_.name) { $_.name } else { $_.context } } |
        Where-Object { $_ })
    $missingRequiredChecks = @()
    foreach ($required in $RequiredCheck) {
        $matchingChecks = @($checkNames | Where-Object {
            $_.StartsWith($required, [StringComparison]::OrdinalIgnoreCase)
        })
        $exactRequiredChecks += $matchingChecks
        if ($matchingChecks.Count -eq 0) {
            $missingRequiredChecks += $required
        }
    }
    $missingRequiredChecks = @($missingRequiredChecks + @($exactRequiredChecks |
        Where-Object { $_ -cnotin $checkNames }) | Sort-Object -CaseSensitive -Unique)
    $unprovenRequiredChecks = @($view.statusCheckRollup | Where-Object {
        $checkName = if ($_.name) { $_.name } else { $_.context }
        $checkName -cin $exactRequiredChecks -and
        $_.conclusion -ne "SUCCESS" -and $_.state -ne "SUCCESS"
    } | ForEach-Object { if ($_.name) { $_.name } else { $_.context } })

    $threads = @($threadPage.nodes)
    $unresolved = @($threads | Where-Object { -not $_.isResolved })
    $truncatedThreadComments = @($threads |
        Where-Object { $_.comments.pageInfo.hasNextPage } |
        ForEach-Object { "$($_.path):$($_.line)" })
    $suppressed = @($reviewPage.nodes | Where-Object {
        $_.body -and $_.body -imatch 'suppressed'
    } | ForEach-Object {
        [pscustomobject]@{
            author = $_.author.login
            submittedAt = $_.submittedAt
            commit = $_.commit.oid
            body = $_.body
        }
    })

    $automatedReviews = @($reviewPage.nodes | Where-Object {
        $_.author.login -and
        ($automatedLogins -contains ($_.author.login -replace '\[bot\]$', '').ToLowerInvariant())
    })
    # Coverage must be decided by commit, not recency. A force-push back to an
    # already-reviewed commit leaves the newest review pointing at a commit that
    # is no longer head, which would report a reviewed head as uncovered and
    # send -WaitForReview into a pointless wait.
    # Only submitted reviews count. A pending review already carries a commit oid
    # but has a null submittedAt, so matching on the oid alone would report the
    # head as covered while the review is still being written - the exact
    # premature all-clear this gate exists to prevent.
    $reviewsForHead = @($automatedReviews |
        Where-Object { $_.submittedAt -and $_.commit.oid -eq $view.headRefOid })
    $latestAutomated = if ($reviewsForHead) {
        $reviewsForHead |
            Sort-Object { [datetime]$_.submittedAt } |
            Select-Object -Last 1
    } else {
        $automatedReviews |
            Where-Object { $_.submittedAt } |
            Sort-Object { [datetime]$_.submittedAt } |
            Select-Object -Last 1
    }
    $automatedReviewCoversHead = [bool]$reviewsForHead
    $suppressedForHead = @($suppressed |
        Where-Object { $_.commit -eq $view.headRefOid })

    $freshText = & gh pr view $number --repo $Repo `
        --json "headRefOid,headRefName,headRepository,headRepositoryOwner,baseRefOid,baseRefName,state" 2>$null
    $fresh = $null
    $freshHeadRepo = $null
    if ($LASTEXITCODE -ne 0) {
        $script:collectionErrors.Add("Could not recheck PR revisions (GitHub CLI exit $LASTEXITCODE).")
    } else {
        try {
            $fresh = $freshText | ConvertFrom-Json
            $freshHeadRepo = Get-HeadRepositoryName $fresh
            if (-not $fresh -or $fresh.headRefOid -notmatch '^[0-9a-fA-F]{40}$' -or
                -not $fresh.headRefName -or -not $freshHeadRepo -or
                -not $fresh.baseRefName -or $fresh.state -notin @("OPEN", "CLOSED", "MERGED")) {
                throw "Incomplete PR revision evidence."
            }
        } catch {
            $fresh = $null
            $script:collectionErrors.Add("Invalid PR revision recheck evidence.")
        }
    }
    $freshBranch = Get-RestEvidence "repos/$Repo/branches/$baseRef"
    $revisionErrors = @(
        if (-not $view.headRefOid -or -not $baseSha) { "Missing head/base SHA." }
        if (-not $fresh) {
            "Could not recheck PR revisions."
        } elseif ($fresh.headRefOid -ne $view.headRefOid -or
            $fresh.headRefName -ne $view.headRefName -or
            $freshHeadRepo -ne $headRepo -or
            $fresh.baseRefName -ne $view.baseRefName -or $fresh.state -ne $view.state) {
            "PR head, base, or state changed during collection."
        }
        if ($freshBranch.commit.sha -notmatch '^[0-9a-fA-F]{40}$') {
            "Could not recheck target branch."
        } elseif ($baseSha -match '^[0-9a-fA-F]{40}$' -and $freshBranch.commit.sha -ne $baseSha) {
            "Target branch moved during collection."
        }
        if ($ExpectedHeadSha -and $ExpectedHeadSha -ne $view.headRefOid) { "Expected head SHA does not match." }
        if ($ExpectedBaseSha -and $ExpectedBaseSha -ne $baseSha) { "Expected base SHA does not match." }
    )
    $evidenceComplete = $revisionErrors.Count -eq 0 -and $script:collectionErrors.Count -eq 0 -and
        $truncatedThreadComments.Count -eq 0
    $remoteGatesClear = $evidenceComplete -and $automatedReviewCoversHead -and
        $unevaluatedRules.Count -eq 0 -and $unevaluatedClassic.Count -eq 0 -and
        $unresolved.Count -eq 0 -and $suppressedForHead.Count -eq 0 -and
        $missingRequiredChecks.Count -eq 0 -and $unprovenRequiredChecks.Count -eq 0 -and
        $failedChecks.Count -eq 0 -and $pendingChecks.Count -eq 0 -and
        $workflowApprovalsRequired.Count -eq 0 -and $failedWorkflows.Count -eq 0 -and
        $pendingWorkflows.Count -eq 0 -and $compare.behind_by -eq 0 -and
        $view.reviewDecision -ne "CHANGES_REQUESTED" -and $view.mergeable -eq "MERGEABLE"
    $humanApproval = if (-not $rulesComplete) { "unknown" }
        elseif ($view.reviewDecision -eq "APPROVED") { "approved" }
        elseif ($view.reviewDecision -in @("REVIEW_REQUIRED", "CHANGES_REQUESTED")) { "pending" }
        elseif (@($approvalRules | Where-Object {
            ($_.required_approving_review_count -isnot [long] -and
                $_.required_approving_review_count -isnot [int]) -or
            $_.required_approving_review_count -ne 0 -or
            $_.require_code_owner_review -or $_.require_code_owner_reviews -or
            $_.require_last_push_approval -or $_.required_reviewers
        }).Count -eq 0) { "not_required" }
        else { "unknown" }

    $reviewRequested = $false
    if ($script:reviewRequestOutcome.ContainsKey($number)) {
        $reviewRequested = $script:reviewRequestOutcome[$number]
    }
    # Under -WaitForReview this function runs once per poll, so requesting
    # unconditionally would re-POST the request every cycle until coverage
    # arrived - notification spam and a rate-limit risk. Attempt it at most once
    # per PR per invocation and reuse the recorded outcome afterwards.
    if ($RequestReview -and -not $automatedReviewCoversHead -and
        -not $script:reviewRequestOutcome.ContainsKey($number)) {
        if ($revisionErrors.Count -gt 0 -or $view.state -ne "OPEN") {
            throw "PR #${number}: Review request blocked: PR must be open with complete, unchanged revision evidence."
        }
        # Requesting by handle through the REST endpoint is the only path that works.
        # The GraphQL requestReviews mutation rejects the reviewer's Bot node id
        # ("Could not resolve to User node"), and `gh pr edit --add-reviewer` cannot
        # resolve the bot login at all. The reviewer also never shows up in
        # requested_reviewers afterwards, so a successful request cannot be confirmed
        # by reading that list back; confirm it by polling until
        # automatedReviewCoversHead turns true.
        gh api "repos/$owner/$name/pulls/$number/requested_reviewers" `
            -X POST -f "reviewers[]=$ReviewerHandle" *> $null
        $reviewRequested = ($LASTEXITCODE -eq 0)
        $script:reviewRequestOutcome[$number] = $reviewRequested
        if (-not $reviewRequested) {
            Write-Warning "PR #${number}: could not request a review from '$ReviewerHandle'."
        }
    }

    $pipelineRunRequested = $false
    $pipelineRunComment = $null
    # CI execution conservatively requires complete collection, unlike a review notification.
    if ($RunPipeline -and -not $evidenceComplete) {
        $gaps = @(
            $revisionErrors
            $script:collectionErrors
            if ($truncatedThreadComments.Count -gt 0) { "Review-thread comments were truncated." }
        )
        throw "CI trigger blocked by incomplete or stale evidence: $($gaps -join ' ') Run without -RunPipeline for the diagnostic snapshot."
    }
    if ($RunPipeline -and $view.state -ne "OPEN") {
        throw "CI trigger requires an open PR."
    }
    # This revision check catches stale requests, not a trigger race. The caller
    # must enforce the trusted CI delegation contract for secret-bearing jobs.
    if ($RunPipeline -and $azureCheck -cnotin $checkNames) {
        # A comment is the only trigger the pipeline honours from here; queueing
        # through the ADO API needs credentials this script does not assume.
        $commentText = gh api "repos/$owner/$name/issues/$number/comments" `
            -X POST -f "body=/azp run" 2>$null
        $pipelineRunRequested = ($LASTEXITCODE -eq 0)
        if (-not $pipelineRunRequested) {
            Write-Warning "PR #${number}: could not confirm '/azp run' was posted. Inspect existing comments and builds before retrying."
        } else {
            # Receipt parsing must not turn a successful POST into a failed request.
            $comment = $null
            try {
                $comment = ($commentText -join "`n") | ConvertFrom-Json
            } catch [System.ArgumentException] {
                $comment = $null
            }
            if ($comment.id -and $comment.html_url -and $comment.created_at) {
                $pipelineRunComment = $comment | Select-Object id, html_url, created_at
            } else {
                Write-Warning "PR #${number}: '/azp run' was posted but its receipt is incomplete. Inspect existing comments and builds before retrying."
            }
        }
    }

    [pscustomobject]@{
        number = $view.number
        title = $view.title
        url = $view.url
        state = $view.state
        draft = $view.isDraft
        mergeable = $view.mergeable
        mergeState = $view.mergeStateStatus
        reviewDecision = $view.reviewDecision
        base = $view.baseRefName
        baseSha = $baseSha
        prBaseRefSha = $view.baseRefOid
        headSha = $view.headRefOid
        revisionErrors = $revisionErrors
        collectionErrors = @($script:collectionErrors.ToArray())
        effectiveRules = $rules
        unevaluatedRules = $unevaluatedRules
        classicProtection = $classic
        ruleRequiredChecks = $ruleChecks
        workflows = $workflows
        workflowApprovalsRequired = $workflowApprovalsRequired
        failedWorkflows = $failedWorkflows
        pendingWorkflows = $pendingWorkflows
        targetStatus = $compare.status
        aheadBy = $compare.ahead_by
        behindBy = $compare.behind_by
        failedChecks = $failedChecks
        pendingChecks = $pendingChecks
        missingRequiredChecks = $missingRequiredChecks
        unprovenRequiredChecks = $unprovenRequiredChecks
        unresolvedThreads = $unresolved
        suppressedReviewBodies = $suppressed
        suppressedReviewBodiesForHead = $suppressedForHead
        readiness = [pscustomobject]@{
            evidenceComplete = $evidenceComplete
            remoteEngineeringGatesClear = $remoteGatesClear
            engineering = if ($remoteGatesClear) { "requires_manual_evidence" } else { "blocked" }
            humanApproval = $humanApproval
            merge = if (-not $remoteGatesClear -or $humanApproval -notin @("approved", "not_required") -or
                $view.state -ne "OPEN" -or $view.isDraft -or $view.mergeStateStatus -ne "CLEAN") {
                "blocked"
            } else { "requires_engineering_evidence" }
            unassessedGates = @("Public behavior and local validation", "Full review findings and gauntlet",
                "Azure merge provenance, actual tests and skips", "Required check issuer identities",
                "Workflow trigger provenance for shared branches or revisions") +
                @($unevaluatedRules | ForEach-Object { $_.type } | Sort-Object -Unique |
                    ForEach-Object { "Effective branch rule requires assessment: $_" }) +
                @($unevaluatedClassic | ForEach-Object { "Classic protection requires assessment: $_" })
        }
        completeness = [pscustomobject]@{
            reviewThreadPages = $threadPage.pages
            reviewThreadCount = $threads.Count
            reviewPages = $reviewPage.pages
            reviewCount = @($reviewPage.nodes).Count
            threadsWithUnreadComments = $truncatedThreadComments
            latestAutomatedReviewCommit = if ($latestAutomated) { $latestAutomated.commit.oid } else { $null }
            latestAutomatedReviewAt = if ($latestAutomated) { $latestAutomated.submittedAt } else { $null }
            latestAutomatedReviewAuthor = if ($latestAutomated) { $latestAutomated.author.login } else { $null }
            automatedReviewCoversHead = $automatedReviewCoversHead
            automatedReviewRequested = $reviewRequested
            pipelineRunRequested = $pipelineRunRequested
            pipelineRunComment = $pipelineRunComment
            # Backward-compatible remote-evidence alias, never merge approval.
            complete = $remoteGatesClear
        }
    }
}

$results = @(foreach ($number in $PullRequest) {
    $snapshot = Get-PrSnapshot -number $number

    # Automated review lands some time after a push, so a single snapshot taken
    # straight after one reports zero findings for code that has not been looked
    # at yet. The required build is worse: it does not start at all until someone
    # comments, so waiting only on the review would still return a head with no
    # CI on it. Wait for both, for this exact head.
    if ($WaitForReview) {
        $deadline = (Get-Date).AddMinutes($TimeoutMinutes)
        while (-not ($snapshot.completeness.automatedReviewCoversHead -and
                @($snapshot.missingRequiredChecks).Count -eq 0) -and
            (Get-Date) -lt $deadline) {
            Write-Verbose ("PR #{0}: waiting for automated review and required checks of {1}" -f
                $number, $snapshot.headSha)
            Start-Sleep -Seconds $PollSeconds
            $snapshot = Get-PrSnapshot -number $number
        }
        if (-not $snapshot.completeness.automatedReviewCoversHead) {
            Write-Warning (("PR #{0}: no automated review covered {1} within {2} minute(s). " +
                "Findings for this head may still be pending; do not read this as clean.") -f
                $number, $snapshot.headSha, $TimeoutMinutes)
        }
        if (@($snapshot.missingRequiredChecks).Count -gt 0) {
            Write-Warning (("PR #{0}: required check(s) '{1}' never appeared on {2} within " +
                "{3} minute(s). Inspect the owning checks and workflow approval gates; " +
                "missing status alone does not prove that a build never ran.") -f
                $number, ($snapshot.missingRequiredChecks -join ", "),
                $snapshot.headSha, $TimeoutMinutes)
            if ($azureCheck -cin $snapshot.missingRequiredChecks) {
                Write-Warning (("PR #{0}: the Azure aggregate is absent. Inspect existing runs first; " +
                    "only after the trusted authorization gate, use -RunPipeline with expected " +
                    "head/base SHAs if a new run is needed.") -f $number)
            }
        }
    }

    $snapshot
})

ConvertTo-Json -InputObject $results -Depth 12
