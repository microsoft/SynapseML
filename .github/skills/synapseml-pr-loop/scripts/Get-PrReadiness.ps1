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

    It does not make the readiness decision; use the skill's evidence gates for
    that judgment. Output can contain review content; keep it local or redact it
    before sharing.
#>
param(
    [Parameter(Mandatory)]
    [int[]]$PullRequest,

    [string]$Repo = "microsoft/SynapseML",

    # Logins whose reviews count as automated coverage of the head commit. Matched
    # exactly, with an optional "[bot]" suffix, so a human account that merely
    # contains one of these words is never mistaken for the reviewer.
    [string[]]$AutomatedReviewer = @("copilot-pull-request-reviewer", "github-copilot", "copilot"),

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
    [string]$ReviewerHandle = "Copilot"
)

$ErrorActionPreference = "Stop"

# GraphQL responses are sparse: absent fields are simply missing rather than null,
# and this script reads them positionally. Under StrictMode that is a terminating
# error, so a caller with StrictMode enabled would get no snapshot at all instead
# of a snapshot reporting what is missing. Pin the mode off for this scope so the
# script's behaviour does not depend on the caller's session state.
Set-StrictMode -Off

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

function Get-PrSnapshot {
    param([int]$number)

    $jsonFields = "number,title,state,isDraft,mergeable,mergeStateStatus,reviewDecision," +
        "headRefOid,baseRefName,statusCheckRollup,url"
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
    $compareText = & gh api "repos/$Repo/compare/$baseRef...$($view.headRefOid)"
    if ($LASTEXITCODE -ne 0) {
        throw "Target comparison failed for PR #$number"
    }
    $compare = $compareText | ConvertFrom-Json

    $failedChecks = @($view.statusCheckRollup | Where-Object {
        $_.conclusion -in @("FAILURE", "CANCELLED", "TIMED_OUT", "ACTION_REQUIRED", "STALE") -or
        $_.state -in @("ERROR", "FAILURE")
    } | ForEach-Object { if ($_.name) { $_.name } else { $_.context } })

    $pendingChecks = @($view.statusCheckRollup | Where-Object {
        ($_.status -and $_.status -ne "COMPLETED") -or
        $_.state -in @("EXPECTED", "PENDING")
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
    $latestAutomated = $automatedReviews |
        Where-Object { $_.submittedAt } |
        Sort-Object { [datetime]$_.submittedAt } |
        Select-Object -Last 1
    $automatedReviewCoversHead =
        [bool]($latestAutomated -and $latestAutomated.commit.oid -eq $view.headRefOid)
    $suppressedForHead = @($suppressed |
        Where-Object { $_.commit -eq $view.headRefOid })

    $reviewRequested = $false
    if ($RequestReview -and -not $automatedReviewCoversHead) {
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
        if (-not $reviewRequested) {
            Write-Warning "PR #${number}: could not request a review from '$ReviewerHandle'."
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
        headSha = $view.headRefOid
        targetStatus = $compare.status
        aheadBy = $compare.ahead_by
        behindBy = $compare.behind_by
        failedChecks = $failedChecks
        pendingChecks = $pendingChecks
        unresolvedThreads = $unresolved
        suppressedReviewBodies = $suppressed
        suppressedReviewBodiesForHead = $suppressedForHead
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
            complete = ($truncatedThreadComments.Count -eq 0 -and $automatedReviewCoversHead)
        }
    }
}

$results = @(foreach ($number in $PullRequest) {
    $snapshot = Get-PrSnapshot -number $number

    # Automated review lands some time after a push, so a single snapshot taken
    # straight after one reports zero findings for code that has not been looked
    # at yet. Wait for the review of this exact head rather than leaving a human
    # to notice that the comments arrived later.
    if ($WaitForReview -and -not $snapshot.completeness.automatedReviewCoversHead) {
        $deadline = (Get-Date).AddMinutes($TimeoutMinutes)
        while ((Get-Date) -lt $deadline) {
            Write-Verbose ("PR #{0}: waiting for automated review of {1}" -f
                $number, $snapshot.headSha)
            Start-Sleep -Seconds $PollSeconds
            $snapshot = Get-PrSnapshot -number $number
            if ($snapshot.completeness.automatedReviewCoversHead) { break }
        }
        if (-not $snapshot.completeness.automatedReviewCoversHead) {
            Write-Warning ("PR #{0}: no automated review covered {1} within {2} minute(s). " +
                "Findings for this head may still be pending; do not read this as clean." -f
                $number, $snapshot.headSha, $TimeoutMinutes)
        }
    }

    $snapshot
})

ConvertTo-Json -InputObject $results -Depth 12
