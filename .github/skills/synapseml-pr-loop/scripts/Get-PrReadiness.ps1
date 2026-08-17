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

    It does not make the readiness decision; use the skill's evidence gates for
    that judgment. Output can contain review content; keep it local or redact it
    before sharing.
#>
param(
    [Parameter(Mandatory)]
    [int[]]$PullRequest,

    [string]$Repo = "microsoft/SynapseML"
)

$ErrorActionPreference = "Stop"

if (-not (Get-Command gh -ErrorAction SilentlyContinue)) {
    throw "GitHub CLI 'gh' is required."
}

$repoParts = $Repo.Split("/")
if ($repoParts.Count -ne 2 -or -not $repoParts[0] -or -not $repoParts[1]) {
    throw "Repo must use owner/name format; got '$Repo'."
}
$owner = $repoParts[0]
$name = $repoParts[1]

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
        if ($response.errors) {
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

$results = @(foreach ($number in $PullRequest) {
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
        $_.author.login -and $_.author.login -imatch 'copilot'
    })
    $latestAutomated = $automatedReviews |
        Where-Object { $_.submittedAt } |
        Sort-Object { [datetime]$_.submittedAt } |
        Select-Object -Last 1
    $automatedReviewCoversHead =
        [bool]($latestAutomated -and $latestAutomated.commit.oid -eq $view.headRefOid)
    $suppressedForHead = @($suppressed |
        Where-Object { $_.commit -eq $view.headRefOid })

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
            latestAutomatedReviewCommit = $latestAutomated.commit.oid
            latestAutomatedReviewAt = $latestAutomated.submittedAt
            automatedReviewCoversHead = $automatedReviewCoversHead
            complete = ($truncatedThreadComments.Count -eq 0 -and $automatedReviewCoversHead)
        }
    }
})

ConvertTo-Json -InputObject $results -Depth 12
