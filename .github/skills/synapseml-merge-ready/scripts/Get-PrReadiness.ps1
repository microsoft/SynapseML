<#
.SYNOPSIS
    Captures GitHub merge-readiness evidence for one or more pull requests.
.DESCRIPTION
    Reports head/base state, target divergence, checks, active review threads,
    and review bodies containing suppressed comments. It does not make the
    readiness decision; use the skill's evidence gates for that judgment.
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

$repoParts = $Repo.Split("/", 2)
if ($repoParts.Count -ne 2) {
    throw "Repo must use owner/name format."
}
$owner = $repoParts[0]
$name = $repoParts[1]

$query = @'
query($owner: String!, $name: String!, $number: Int!) {
  repository(owner: $owner, name: $name) {
    pullRequest(number: $number) {
      reviewThreads(first: 100) {
        nodes {
          isResolved
          isOutdated
          path
          line
          comments(first: 20) {
            nodes {
              author { login }
              body
              url
            }
          }
        }
      }
      reviews(last: 50) {
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

$results = foreach ($number in $PullRequest) {
    $jsonFields = "number,title,state,isDraft,mergeable,mergeStateStatus,reviewDecision," +
        "headRefOid,baseRefName,statusCheckRollup,url"
    $viewText = & gh pr view $number --repo $Repo --json $jsonFields
    if ($LASTEXITCODE -ne 0) {
        throw "gh pr view failed for PR #$number"
    }
    $view = $viewText | ConvertFrom-Json

    $reviewText = & gh api graphql -f "query=$query" `
        -F "owner=$owner" -F "name=$name" -F "number=$number"
    if ($LASTEXITCODE -ne 0) {
        throw "GraphQL review query failed for PR #$number"
    }
    $review = ($reviewText | ConvertFrom-Json).data.repository.pullRequest

    $compareText = & gh api "repos/$Repo/compare/$($view.baseRefName)...$($view.headRefOid)"
    if ($LASTEXITCODE -ne 0) {
        throw "Target comparison failed for PR #$number"
    }
    $compare = $compareText | ConvertFrom-Json

    $failedChecks = @($view.statusCheckRollup | Where-Object {
        $_.conclusion -in @("FAILURE", "CANCELLED", "TIMED_OUT", "ACTION_REQUIRED") -or
        $_.state -in @("ERROR", "FAILURE")
    } | ForEach-Object { if ($_.name) { $_.name } else { $_.context } })

    $pendingChecks = @($view.statusCheckRollup | Where-Object {
        ($_.status -and $_.status -ne "COMPLETED") -or
        $_.state -in @("EXPECTED", "PENDING")
    } | ForEach-Object { if ($_.name) { $_.name } else { $_.context } })

    $threads = @($review.reviewThreads.nodes)
    $unresolved = @($threads | Where-Object { -not $_.isResolved })
    $suppressed = @($review.reviews.nodes | Where-Object {
        $_.body -and $_.body.Contains("Suppressed comments")
    } | ForEach-Object {
        [pscustomobject]@{
            author = $_.author.login
            submittedAt = $_.submittedAt
            commit = $_.commit.oid
            body = $_.body
        }
    })

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
    }
}

$results | ConvertTo-Json -Depth 12
