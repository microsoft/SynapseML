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

    Checks are reported by absence as well as outcome. `failedChecks` and
    `pendingChecks` can only describe checks that exist, so a head whose build
    never started scores zero in both and reads as finished;
    `missingRequiredChecks` catches that and gates `complete`. The Azure DevOps
    build is the usual casualty because it does not queue itself on a push -- it
    waits for an `/azp run` comment.

    This helper is deliberately read-only. GitHub has no conditional comment
    operation and `/azp run` carries no commit SHA, so a helper cannot atomically
    bind that command to the reviewed head. It reports immutable, exact-SHA
    evidence for a maintainer to inspect but never posts the privileged command.
    Copilot review guidance is advisory and its overview format is not a machine
    authorization token. Copilot also reads instructions and skills from the
    pull-request head, so a head that changes those inputs requires independent
    maintainer review.

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

    # Checks that must be PRESENT on the head commit, matched as a name prefix so
    # one entry covers a pipeline's several legs. Absence is the point: the
    # failed/pending sets can only describe checks that exist, so a head whose CI
    # never started scores zero failures and zero pending and looks finished. The
    # Azure DevOps build does not queue itself on every push here -- it needs an
    # `/azp run` comment -- which is exactly the check most likely to be missing.
    [string[]]$RequiredCheck = @("microsoft.SynapseML")
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

# Git trees are immutable for one base/head pair. Cache by PR and both commit
# SHAs so polling does not repeatedly traverse the repository; a push or target
# advance gets a new key and therefore a fresh comparison.
$script:fileInventoryByHead = @{}

function Test-CopilotReviewInfluencePath {
    param([Parameter(Mandatory)][string]$Path)

    $normalized = $Path.Replace("\", "/").TrimStart("/")
    $leaf = [IO.Path]::GetFileName($normalized)

    if ($normalized -ieq ".github/copilot-instructions.md" -or
        $normalized.StartsWith(".github/skills/", [StringComparison]::OrdinalIgnoreCase) -or
        ($normalized.StartsWith(
                ".github/instructions/", [StringComparison]::OrdinalIgnoreCase
            ) -and $normalized.EndsWith(
                ".instructions.md", [StringComparison]::OrdinalIgnoreCase
            )) -or
        $leaf -in @("AGENTS.md", "CLAUDE.md", "GEMINI.md", "REVIEW.md") -or
        $normalized -in @(
            ".github/workflows/copilot-code-review.yml",
            ".github/workflows/copilot-code-review.yaml",
            ".github/workflows/copilot-setup-steps.yml",
            ".github/workflows/copilot-setup-steps.yaml"
        )) {
        return $true
    }
    return $false
}

function Get-CurrentPullRequestRefs {
    param([Parameter(Mandatory)][int]$Number)

    $refsText = & gh api "repos/$Repo/pulls/$Number" `
        --jq '{baseSha:.base.sha,headSha:.head.sha}'
    if ($LASTEXITCODE -ne 0) {
        throw "Pull-request ref query failed for PR #$Number"
    }
    $refs = $refsText | ConvertFrom-Json
    if ($refs.baseSha -notmatch '^[0-9a-fA-F]{40}$' -or
        $refs.headSha -notmatch '^[0-9a-fA-F]{40}$') {
        throw "Pull-request ref query returned incomplete data for PR #$Number"
    }
    return $refs
}

function Get-GitTreeSha {
    param(
        [Parameter(Mandatory)]
        [ValidatePattern('^[0-9a-fA-F]{40}$')]
        [string]$CommitSha,
        [Parameter(Mandatory)][int]$Number
    )

    $commitText = & gh api "repos/$Repo/git/commits/$CommitSha"
    if ($LASTEXITCODE -ne 0) {
        throw "Commit query failed for PR #$Number at $CommitSha"
    }
    $commit = $commitText | ConvertFrom-Json
    $treeSha = if ($commit.tree) { [string]$commit.tree.sha } else { "" }
    if ($commit.sha -ine $CommitSha -or
        $treeSha -notmatch '^[0-9a-fA-F]{40}$') {
        throw "Commit query returned mismatched or incomplete data for PR #$Number"
    }
    return $treeSha.ToLowerInvariant()
}

function Get-CompleteGitTree {
    param(
        [Parameter(Mandatory)]
        [ValidatePattern('^[0-9a-fA-F]{40}$')]
        [string]$TreeSha,
        [Parameter(Mandatory)][int]$Number
    )

    $treeText = & gh api "repos/$Repo/git/trees/$TreeSha`?recursive=1"
    if ($LASTEXITCODE -ne 0) {
        throw "Git-tree query failed for PR #$Number at $TreeSha"
    }
    $tree = $treeText | ConvertFrom-Json
    if ($tree.sha -ine $TreeSha -or
        -not $tree.PSObject.Properties['truncated'] -or
        $tree.truncated -ne $false -or
        -not $tree.PSObject.Properties['tree']) {
        throw "Git-tree query returned mismatched, truncated, or incomplete data for PR #$Number"
    }

    $entries = [Collections.Generic.Dictionary[string, object]]::new(
        [StringComparer]::Ordinal
    )
    $seenPaths = [Collections.Generic.HashSet[string]]::new(
        [StringComparer]::Ordinal
    )
    foreach ($entry in @($tree.tree)) {
        $path = [string]$entry.path
        $sha = [string]$entry.sha
        $mode = [string]$entry.mode
        $type = [string]$entry.type
        if ([string]::IsNullOrWhiteSpace($path) -or
            $sha -notmatch '^[0-9a-fA-F]{40}$' -or
            [string]::IsNullOrWhiteSpace($mode) -or
            $type -notin @("blob", "tree", "commit") -or
            -not $seenPaths.Add($path)) {
            throw "Git-tree query returned an invalid or duplicate path for PR #$Number"
        }
        if ($type -ne "tree") {
            $entries.Add($path, [pscustomobject]@{
                sha = $sha.ToLowerInvariant()
                mode = $mode
                type = $type
            })
        }
    }
    return $entries
}

function Get-PullRequestFileInventory {
    param(
        [Parameter(Mandatory)][int]$Number,
        [Parameter(Mandatory)]
        [ValidatePattern('^[0-9a-fA-F]{40}$')]
        [string]$BaseSha,
        [Parameter(Mandatory)]
        [ValidatePattern('^[0-9a-fA-F]{40}$')]
        [string]$HeadSha
    )

    $cacheKey = "${Number}:${BaseSha}:$HeadSha"
    if ($script:fileInventoryByHead.ContainsKey($cacheKey)) {
        return $script:fileInventoryByHead[$cacheKey]
    }

    $baseTreeSha = Get-GitTreeSha -CommitSha $BaseSha -Number $Number
    $headTreeSha = Get-GitTreeSha -CommitSha $HeadSha -Number $Number
    $baseEntries = Get-CompleteGitTree -TreeSha $baseTreeSha -Number $Number
    $headEntries = Get-CompleteGitTree -TreeSha $headTreeSha -Number $Number

    $allPaths = [Collections.Generic.HashSet[string]]::new(
        [StringComparer]::Ordinal
    )
    foreach ($path in $baseEntries.Keys) { [void]$allPaths.Add($path) }
    foreach ($path in $headEntries.Keys) { [void]$allPaths.Add($path) }

    # Compare immutable object identity rather than the mutable PR-files
    # endpoint. Renames intentionally appear as a deletion plus an addition so
    # both paths are checked for review influence.
    $files = @($allPaths | Sort-Object -CaseSensitive | ForEach-Object {
        $path = $_
        $changed = -not $baseEntries.ContainsKey($path) -or
            -not $headEntries.ContainsKey($path)
        if (-not $changed) {
            $baseEntry = $baseEntries[$path]
            $headEntry = $headEntries[$path]
            $changed = (
                $baseEntry.sha -cne $headEntry.sha -or
                $baseEntry.mode -cne $headEntry.mode -or
                $baseEntry.type -cne $headEntry.type
            )
        }
        if ($changed) {
            [pscustomobject]@{ filename = $path }
        }
    })

    $inventory = [pscustomobject]@{
        files = $files
        baseTreeSha = $baseTreeSha
        headTreeSha = $headTreeSha
        baseEntryCount = $baseEntries.Count
        headEntryCount = $headEntries.Count
        source = "immutable-git-trees"
        complete = $true
    }
    $script:fileInventoryByHead[$cacheKey] = $inventory
    return $inventory
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

function Get-PrSnapshot {
    param([int]$number)

    # Same reason as Invoke-PagedQuery: sparse GraphQL fields are read
    # positionally here, and StrictMode is scoped to this function so
    # dot-sourcing cannot change the caller's session.
    Set-StrictMode -Off

    $jsonFields = "number,title,state,isDraft,mergeable,mergeStateStatus,reviewDecision," +
        "headRefOid,baseRefName,baseRefOid,statusCheckRollup,url"
    $viewText = & gh pr view $number --repo $Repo --json $jsonFields
    if ($LASTEXITCODE -ne 0) {
        throw "gh pr view failed for PR #$number"
    }
    $view = $viewText | ConvertFrom-Json
    $fileInventory = Get-PullRequestFileInventory `
        -Number $number -BaseSha $view.baseRefOid -HeadSha $view.headRefOid
    $reviewInfluenceChanges = @($fileInventory.files | ForEach-Object {
        foreach ($candidate in @($_.filename, $_.previous_filename)) {
            if ($candidate -and (Test-CopilotReviewInfluencePath -Path $candidate)) {
                $candidate
            }
        }
    } | Sort-Object -Unique)

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

    # A check that never started cannot fail and cannot be pending, so it leaves
    # no trace in either set above. Test presence separately, by prefix, so one
    # required name covers every leg the pipeline reports.
    $checkNames = @($view.statusCheckRollup |
        ForEach-Object { if ($_.name) { $_.name } else { $_.context } } |
        Where-Object { $_ })
    $missingRequiredChecks = @($RequiredCheck | Where-Object {
        $required = $_
        -not ($checkNames | Where-Object {
            $_.StartsWith($required, [StringComparison]::OrdinalIgnoreCase)
        })
    })

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
    $latestAutomatedForHead = if ($reviewsForHead) {
        $reviewsForHead |
            Sort-Object { [datetime]$_.submittedAt } |
            Select-Object -Last 1
    } else {
        $null
    }
    $latestAutomated = if ($latestAutomatedForHead) {
        $latestAutomatedForHead
    } else {
        $automatedReviews |
            Where-Object { $_.submittedAt } |
            Sort-Object { [datetime]$_.submittedAt } |
            Select-Object -Last 1
    }
    $automatedReviewCoversHead = [bool]$reviewsForHead
    $suppressedForHead = @($suppressed |
        Where-Object { $_.commit -eq $view.headRefOid })

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

    $currentRefs = Get-CurrentPullRequestRefs -Number $number
    $snapshotStillCurrent = (
        $currentRefs.baseSha -eq $view.baseRefOid -and
        $currentRefs.headSha -eq $view.headRefOid
    )
    $privilegedPipelineReviewEvidenceComplete = (
        @($truncatedThreadComments).Count -eq 0 -and
        $fileInventory.complete -and
        @($reviewInfluenceChanges).Count -eq 0 -and
        $automatedReviewCoversHead -and
        @($unresolved).Count -eq 0 -and
        @($suppressedForHead).Count -eq 0 -and
        $snapshotStillCurrent
    )

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
        baseSha = $view.baseRefOid
        headSha = $view.headRefOid
        targetStatus = $compare.status
        aheadBy = $compare.ahead_by
        behindBy = $compare.behind_by
        failedChecks = $failedChecks
        pendingChecks = $pendingChecks
        missingRequiredChecks = $missingRequiredChecks
        unresolvedThreads = $unresolved
        suppressedReviewBodies = $suppressed
        suppressedReviewBodiesForHead = $suppressedForHead
        reviewInfluenceChanges = $reviewInfluenceChanges
        changedFileCount = $fileInventory.files.Count
        changedFileInventoryComplete = $fileInventory.complete
        changedFileInventorySource = $fileInventory.source
        baseTreeSha = $fileInventory.baseTreeSha
        headTreeSha = $fileInventory.headTreeSha
        snapshotStillCurrent = $snapshotStillCurrent
        privilegedPipelineReviewEvidenceComplete = $privilegedPipelineReviewEvidenceComplete
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
            # Everything verifiable must be clear. This previously tested only
            # $truncatedThreadComments, which counts comment-pagination truncation
            # rather than unresolved review threads, so it reported complete=true
            # with review feedback still outstanding. It also read a head whose
            # required build had never queued as clean, because an absent check
            # is neither failed nor pending.
            complete = (
                @($truncatedThreadComments).Count -eq 0 -and
                $fileInventory.complete -and
                @($reviewInfluenceChanges).Count -eq 0 -and
                $automatedReviewCoversHead -and
                @($unresolved).Count -eq 0 -and
                @($suppressedForHead).Count -eq 0 -and
                $snapshotStillCurrent -and
                @($missingRequiredChecks).Count -eq 0 -and
                @($failedChecks).Count -eq 0 -and
                @($pendingChecks).Count -eq 0
            )
        }
    }
}

$results = @(foreach ($number in $PullRequest) {
    $snapshot = Get-PrSnapshot -number $number

    # Automated review lands some time after a push, so a single snapshot taken
    # straight after one reports zero findings for code that has not been looked
    # at yet. Waiting deliberately stops at review evidence. This read-only
    # process must not auto-authorize credential-bearing CI when an AI review
    # appears.
    if ($WaitForReview) {
        $deadline = (Get-Date).AddMinutes($TimeoutMinutes)
        while (-not $snapshot.completeness.automatedReviewCoversHead -and
            $snapshot.changedFileInventoryComplete -and
            @($snapshot.reviewInfluenceChanges).Count -eq 0 -and
            (Get-Date) -lt $deadline) {
            Write-Verbose ("PR #{0}: waiting for automated review of {1}" -f
                $number, $snapshot.headSha)
            Start-Sleep -Seconds $PollSeconds
            $snapshot = Get-PrSnapshot -number $number
        }
        if (-not $snapshot.completeness.automatedReviewCoversHead) {
            Write-Warning ("PR #{0}: no automated review covered {1} within {2} minute(s). " +
                "Findings for this head may still be pending; do not read this as clean." -f
                $number, $snapshot.headSha, $TimeoutMinutes)
        }
        if (-not $snapshot.changedFileInventoryComplete) {
            Write-Warning ("PR #{0}: immutable changed-file inventory is incomplete; " +
                "do not treat this snapshot as review evidence." -f $number)
        }
        if (-not $snapshot.snapshotStillCurrent) {
            Write-Warning ("PR #{0}: base or head changed while evidence was collected; " +
                "rerun readiness for the current commits." -f $number)
        }
        if (@($snapshot.reviewInfluenceChanges).Count -gt 0) {
            Write-Warning ("PR #{0}: head-controlled review input(s) changed: {1}. " +
                "Require an independent maintainer security review and manual trigger." -f
                $number, ($snapshot.reviewInfluenceChanges -join ", "))
        }
        if (@($snapshot.missingRequiredChecks).Count -gt 0) {
            Write-Warning ("PR #{0}: required check(s) '{1}' are absent on {2}. " +
                "This helper is read-only and will not post '/azp run'; that command " +
                "cannot be atomically bound to a commit SHA." -f
                $number, ($snapshot.missingRequiredChecks -join ", "),
                $snapshot.headSha)
        }
    }

    $snapshot
})

ConvertTo-Json -InputObject $results -Depth 12
