"""
GitHub Commit Information API
Retrieves commit, associated PR, linked issues, and CI runs for a commit.
Authenticates using GitHub Personal Access Token from .env file.
"""

import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
from github import Github, GithubException


class GitHubCommitAPI:
    def __init__(self):
        # Load environment variables from .env file
        load_dotenv()
        
        # Get configuration from environment
        self.owner = os.getenv("GITHUB_OWNER")
        self.repo_name = os.getenv("GITHUB_REPO")
        token = os.getenv("GITHUB_TOKEN")
        
        if not all([self.owner, self.repo_name, token]):
            raise ValueError(
                "Missing required environment variables. "
                "Please ensure GITHUB_OWNER, GITHUB_REPO, and GITHUB_TOKEN are set in .env file."
            )
        
        # Initialize GitHub client
        self.github = Github(token)
        self.repo = self.github.get_repo(f"{self.owner}/{self.repo_name}")
    
    def get_commit_info(self, commit_sha: str) -> Dict[str, Any]:
        """
        Retrieve comprehensive information about a commit including:
        - Commit details (author, message, changes)
        - Associated pull request
        - Linked issues
        - CI/CD workflow runs
        """
        try:
            commit = self.repo.get_commit(commit_sha)
        except GithubException as e:
            return {
                "commit": None,
                "pull_request": None,
                "issues": [],
                "workflow_runs": [],
                "error": f"Failed to fetch commit: {str(e)}",
            }
        
        # ---------- Commit ----------
        result: Dict[str, Any] = {
            "commit": {
                "sha": commit.sha,
                "author": {
                    "name": commit.commit.author.name,
                    "email": commit.commit.author.email,
                    "date": commit.commit.author.date.isoformat() if commit.commit.author.date else None,
                },
                "committer": {
                    "name": commit.commit.committer.name,
                    "email": commit.commit.committer.email,
                    "date": commit.commit.committer.date.isoformat() if commit.commit.committer.date else None,
                },
                "message": commit.commit.message,
                "comment_count": commit.commit.comment_count,
                "stats": {
                    "additions": commit.stats.additions,
                    "deletions": commit.stats.deletions,
                    "total": commit.stats.total,
                },
                "url": commit.url,
                "html_url": commit.html_url,
                "parents": [{"sha": p.sha, "url": p.url} for p in commit.parents],
                "tree_sha": commit.commit.tree.sha,
            },
            "pull_request": None,
            "issues": [],
            "workflow_runs": [],
            "error": None,
        }
        
        # ---------- Files / changes ----------
        files: List[Dict[str, Any]] = []
        for file in commit.files:
            files.append({
                "filename": file.filename,
                "status": file.status,  # added, removed, modified, renamed, etc.
                "additions": file.additions,
                "deletions": file.deletions,
                "changes": file.changes,
                "patch": file.patch if hasattr(file, 'patch') else None,
                "blob_url": file.blob_url,
                "raw_url": file.raw_url,
                "previous_filename": file.previous_filename if file.status == "renamed" else None,
            })
        
        result["commit"]["files"] = files
        
        # Compute change counts by status
        change_counts = {"added": 0, "modified": 0, "removed": 0, "renamed": 0}
        for file in commit.files:
            if file.status in change_counts:
                change_counts[file.status] += 1
        result["commit"]["change_counts"] = change_counts
        
        # ---------- Pull Request ----------
        pr = self._find_pr_for_commit(commit_sha, commit.commit.message)
        
        if pr is not None:
            result["pull_request"] = {
                "number": pr.number,
                "title": pr.title,
                "body": pr.body,
                "state": pr.state,
                "created_by": {
                    "login": pr.user.login,
                    "name": pr.user.name,
                    "email": pr.user.email,
                    "id": pr.user.id,
                },
                "created_at": pr.created_at.isoformat() if pr.created_at else None,
                "updated_at": pr.updated_at.isoformat() if pr.updated_at else None,
                "closed_at": pr.closed_at.isoformat() if pr.closed_at else None,
                "merged_at": pr.merged_at.isoformat() if pr.merged_at else None,
                "merge_commit_sha": pr.merge_commit_sha,
                "head": {
                    "ref": pr.head.ref,
                    "sha": pr.head.sha,
                    "label": pr.head.label,
                },
                "base": {
                    "ref": pr.base.ref,
                    "sha": pr.base.sha,
                    "label": pr.base.label,
                },
                "merged": pr.merged,
                "mergeable": pr.mergeable,
                "merged_by": {
                    "login": pr.merged_by.login,
                    "name": pr.merged_by.name,
                } if pr.merged_by else None,
                "url": pr.url,
                "html_url": pr.html_url,
                "labels": [{"name": label.name, "color": label.color} for label in pr.labels],
                "reviewers": [
                    {
                        "login": reviewer.login,
                        "name": reviewer.name,
                    }
                    for reviewer in (pr.requested_reviewers or [])
                ],
                "reviews": self._get_pr_reviews(pr),
                "comments": pr.comments,
                "review_comments": pr.review_comments,
                "commits": pr.commits,
                "additions": pr.additions,
                "deletions": pr.deletions,
                "changed_files": pr.changed_files,
            }
            
            # Get linked issues from PR
            result["issues"] = self._get_pr_linked_issues(pr)
        
        # ---------- Workflow Runs (CI/CD) ----------
        result["workflow_runs"] = self._get_workflow_runs_for_commit(commit_sha)
        
        return result
    
    # -------- Helper Methods --------
    
    def _find_pr_for_commit(self, commit_sha: str, commit_message: str) -> Optional[Any]:
        """
        Find the pull request associated with a commit.
        First tries to find via API, then falls back to parsing commit message.
        """
        try:
            # Method 1: Search for PRs that contain this commit
            prs = self.repo.get_pulls(state="all", sort="updated", direction="desc")
            for pr in prs:
                try:
                    commits = pr.get_commits()
                    for c in commits:
                        if c.sha == commit_sha:
                            return pr
                except GithubException:
                    continue
                
                # Limit search to avoid rate limiting (check most recent 50 PRs)
                if pr.number < (self.repo.get_pulls(state="all")[0].number - 50):
                    break
            
            # Method 2: Parse PR number from commit message
            pr_number = self._parse_pr_number_from_message(commit_message)
            if pr_number is not None:
                try:
                    return self.repo.get_pull(pr_number)
                except GithubException:
                    pass
        
        except GithubException:
            pass
        
        return None
    
    @staticmethod
    def _parse_pr_number_from_message(message: str) -> Optional[int]:
        """
        Parse PR number from commit message.
        Examples:
        - "Merge pull request #1234 from branch"
        - "Title (#1234)"
        - "Fixes #1234"
        """
        # Look for #number pattern (most common in merge commits)
        patterns = [
            r"[Mm]erge pull request #(\d+)",
            r"\(#(\d+)\)",
            r"#(\d+)",
        ]
        
        for pattern in patterns:
            match = re.search(pattern, message)
            if match:
                return int(match.group(1))
        
        return None
    
    def _get_pr_reviews(self, pr) -> List[Dict[str, Any]]:
        """Get reviews for a pull request."""
        reviews = []
        try:
            for review in pr.get_reviews():
                reviews.append({
                    "id": review.id,
                    "user": {
                        "login": review.user.login,
                        "name": review.user.name,
                    },
                    "state": review.state,  # APPROVED, CHANGES_REQUESTED, COMMENTED, etc.
                    "submitted_at": review.submitted_at.isoformat() if review.submitted_at else None,
                    "body": review.body,
                })
        except GithubException:
            pass
        
        return reviews
    
    def _get_pr_linked_issues(self, pr) -> List[Dict[str, Any]]:
        """
        Get issues linked to a pull request.
        Includes issues mentioned in PR body and linked via GitHub's issue linking.
        """
        issues = []
        
        # Parse issue references from PR body
        if pr.body:
            # Look for patterns like "Fixes #123", "Closes #456", etc.
            issue_patterns = [
                r"(?:close[sd]?|fix(?:e[sd])?|resolve[sd]?)\s+#(\d+)",
                r"#(\d+)",
            ]
            
            seen_numbers = set()
            for pattern in issue_patterns:
                matches = re.finditer(pattern, pr.body, re.IGNORECASE)
                for match in matches:
                    issue_number = int(match.group(1))
                    if issue_number not in seen_numbers:
                        seen_numbers.add(issue_number)
                        try:
                            issue = self.repo.get_issue(issue_number)
                            issues.append({
                                "number": issue.number,
                                "title": issue.title,
                                "state": issue.state,
                                "url": issue.url,
                                "html_url": issue.html_url,
                                "labels": [label.name for label in issue.labels],
                            })
                        except GithubException:
                            # Issue might not exist or might be from another repo
                            pass
        
        return issues
    
    def _get_workflow_runs_for_commit(self, commit_sha: str) -> List[Dict[str, Any]]:
        """
        Get GitHub Actions workflow runs for a specific commit.
        """
        runs = []
        try:
            workflow_runs = self.repo.get_commit(commit_sha).get_check_runs()
            
            # Group by workflow run
            seen_run_ids = set()
            for check_run in workflow_runs:
                # Get the workflow run if available
                try:
                    # Check runs may not always have a direct workflow run link
                    # We'll collect the check run data directly
                    if check_run.id not in seen_run_ids:
                        seen_run_ids.add(check_run.id)
                        
                        runs.append({
                            "id": check_run.id,
                            "name": check_run.name,
                            "status": check_run.status,  # queued, in_progress, completed
                            "conclusion": check_run.conclusion,  # success, failure, neutral, cancelled, etc.
                            "started_at": check_run.started_at.isoformat() if check_run.started_at else None,
                            "completed_at": check_run.completed_at.isoformat() if check_run.completed_at else None,
                            "html_url": check_run.html_url,
                            "details_url": check_run.details_url,
                        })
                except (GithubException, AttributeError):
                    continue
            
            # Also try to get workflow runs directly
            try:
                for workflow_run in self.repo.get_workflow_runs(head_sha=commit_sha):
                    if workflow_run.id not in seen_run_ids:
                        seen_run_ids.add(workflow_run.id)
                        runs.append({
                            "id": workflow_run.id,
                            "name": workflow_run.name,
                            "status": workflow_run.status,
                            "conclusion": workflow_run.conclusion,
                            "created_at": workflow_run.created_at.isoformat() if workflow_run.created_at else None,
                            "updated_at": workflow_run.updated_at.isoformat() if workflow_run.updated_at else None,
                            "run_number": workflow_run.run_number,
                            "event": workflow_run.event,
                            "html_url": workflow_run.html_url,
                            "workflow_name": workflow_run.name,
                        })
            except (GithubException, AttributeError):
                pass
        
        except GithubException:
            pass
        
        return runs

    # -------- Range helpers --------

    def _list_commit_shas_between_inclusive(
        self,
        base_ref: str,
        head_ref: str,
    ) -> List[str]:
        comparison = self.repo.compare(base_ref, head_ref)
        shas = [commit.sha for commit in comparison.commits]

        if not shas or shas[0] != base_ref:
            shas = [base_ref] + [sha for sha in shas if sha != base_ref]
        if not shas or shas[-1] != head_ref:
            shas = [sha for sha in shas if sha != head_ref] + [head_ref]

        return shas

    def get_commits_info_between(
        self,
        base_ref: str,
        head_ref: str,
        *,
        max_parallel: int = 8,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        commit_shas = self._list_commit_shas_between_inclusive(base_ref, head_ref)

        if limit is not None and limit >= 0:
            commit_shas = commit_shas[:limit]

        results: List[Tuple[int, Dict[str, Any]]] = []
        if not commit_shas:
            return []

        max_workers = max(1, max_parallel or len(commit_shas))
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            future_map = {
                pool.submit(self.get_commit_info, sha): index
                for index, sha in enumerate(commit_shas)
            }
            for future in as_completed(future_map):
                index = future_map[future]
                try:
                    data = future.result()
                except Exception as exc:
                    data = {
                        "commit": {
                            "sha": commit_shas[index],
                        },
                        "error": f"{type(exc).__name__}: {exc}",
                        "pull_request": None,
                        "issues": [],
                        "workflow_runs": [],
                    }
                results.append((index, data))

        results.sort(key=lambda pair: pair[0])
        return [payload for _, payload in results]


# Convenience helpers
def get_commit_and_pr_info(commit_sha: str) -> Dict[str, Any]:
    api = GitHubCommitAPI()
    return api.get_commit_info(commit_sha)


def get_commit_info(
    begin_ref: str,
    end_ref: Optional[str] = None,
    *,
    max_parallel: int = 8,
    limit: Optional[int] = None,
):
    """Fetch GitHub commit metadata for a single commit or an inclusive range.

    When ``end_ref`` is omitted, returns a single commit payload (dict), matching
    the legacy ``get_commit_and_pr_info`` behavior. When both ``begin_ref`` and
    ``end_ref`` are provided, returns a list of commit payloads ordered from the
    base commit through the head commit.
    """

    api = GitHubCommitAPI()
    if end_ref is None:
        return api.get_commit_info(begin_ref)

    return api.get_commits_info_between(
        begin_ref,
        end_ref,
        max_parallel=max_parallel,
        limit=limit,
    )


# Example usage
if __name__ == "__main__":
    # Example: Get info for a specific commit
    commit_sha = "abc123def456"  # Replace with actual commit SHA
    
    try:
        info = get_commit_and_pr_info(commit_sha)
        
        print(f"Commit: {info['commit']['sha']}")
        print(f"Author: {info['commit']['author']['name']}")
        print(f"Message: {info['commit']['message'][:50]}...")
        
        if info['pull_request']:
            print(f"\nPull Request #{info['pull_request']['number']}: {info['pull_request']['title']}")
            print(f"State: {info['pull_request']['state']}")
        
        if info['issues']:
            print(f"\nLinked Issues: {len(info['issues'])}")
            for issue in info['issues']:
                print(f"  - Issue #{issue['number']}: {issue['title']}")
        
        if info['workflow_runs']:
            print(f"\nWorkflow Runs: {len(info['workflow_runs'])}")
            for run in info['workflow_runs']:
                print(f"  - {run['name']}: {run['conclusion']}")
        
    except Exception as e:
        print(f"Error: {e}")