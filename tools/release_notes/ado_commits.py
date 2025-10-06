"""
Azure DevOps Commit Information API
Retrieves commit, associated PR, linked work items, and CI builds for a commit.
Also supports running across a commit range: [start_commit, end_commit] inclusive.
Authenticates using Azure CLI.
"""

from typing import Any, Dict, Optional, Union, List, Iterable, Tuple
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

from azure.devops.connection import Connection
from azure.identity import AzureCliCredential
from msrest.authentication import BasicAuthentication

# Optional model imports (SDK version varies by env). We import defensively.
_GitQueryCommitsCriteria = None
for _ver in ("v7_0", "v6_0", "v5_1"):
    try:
        _mod = __import__(f"azure.devops.{_ver}.git.models", fromlist=["GitQueryCommitsCriteria"])
        _GitQueryCommitsCriteria = getattr(_mod, "GitQueryCommitsCriteria")
        break
    except Exception:
        continue


class AzureDevOpsCommitAPI:
    def __init__(self):
        # Org / project / repo
        self.organization_url = "https://dev.azure.com/msdata"
        self.project = "a365"
        self.repo_name = "synapseml-internal"

        # Azure CLI → ADO PAT-equivalent token
        self.credential = AzureCliCredential()
        token = self.credential.get_token("499b84ac-1321-427f-aa17-267ca6975798/.default")
        creds = BasicAuthentication(username="", password=token.token)

        # Clients
        self.connection = Connection(base_url=self.organization_url, creds=creds)
        self.git = self.connection.clients.get_git_client()
        self.builds = self.connection.clients.get_build_client()

    # ------------------ NEW: range helpers ------------------

    def _mk_commit_query(self, older_commit: str, newer_commit: str):
        """
        Build a GitQueryCommitsCriteria (or a dict fallback) to ask ADO for commits between two SHAs.
        We request commits from newer back to older (ADO typically returns newest→oldest).
        """
        if _GitQueryCommitsCriteria is not None:
            return _GitQueryCommitsCriteria(
                from_commit_id=older_commit,  # lower/older boundary
                to_commit_id=newer_commit,    # upper/newer boundary
                include_links=False,
            )
        # Fallback (msrest usually expects model objects, but this keeps code resilient)
        return {
            "from_commit_id": older_commit,
            "to_commit_id": newer_commit,
            "include_links": False,
        }

    def _list_commit_ids_between_commits_inclusive(
        self,
        older_commit: str,
        newer_commit: str,
    ) -> List[str]:
        """
        Return commit SHAs between two commit IDs [older_commit, newer_commit] inclusive.
        Order: oldest → newest (useful for linear release notes).
        """
        criteria = self._mk_commit_query(older_commit, newer_commit)
        # get_commits supports: repository_id, search_criteria, project, skip, top
        commits = self.git.get_commits(
            repository_id=self.repo_name,
            search_criteria=criteria,
            project=self.project,
        ) or []

        # ADO returns newest → oldest; reverse to oldest → newest
        shas = [c.commit_id for c in commits][::-1]

        # Ensure inclusive boundaries (some servers omit the endpoints)
        if not shas or shas[0] != older_commit:
            shas = [older_commit] + [s for s in shas if s != older_commit]
        if not shas or shas[-1] != newer_commit:
            shas = [s for s in shas if s != newer_commit] + [newer_commit]

        return shas

    def get_commits_info_between(
        self,
        begin_commit: str,
        end_commit: str,
        *,
        max_parallel: int = 8,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Compute get_commit_info() for all commits between [begin_commit, end_commit], inclusive,
        running up to max_parallel at once. Returns results ordered oldest → newest.
        """
        commit_ids = self._list_commit_ids_between_commits_inclusive(begin_commit, end_commit)

        if limit is not None:
            if limit <= 0:
                return []
            commit_ids = commit_ids[:limit]

        return self.get_commits_info_for_ids(commit_ids, max_parallel=max_parallel)

    def get_commits_info_for_ids(
        self,
        commit_ids: Iterable[str],
        *,
        max_parallel: int = 8,
    ) -> List[Dict[str, Any]]:
        """
        Fetch commit metadata for the provided commit IDs in the order given.
        """
        commit_list = [cid for cid in commit_ids if cid]
        if not commit_list:
            return []

        # Preserve caller-provided order while avoiding duplicate requests.
        seen: Dict[str, int] = {}
        ordered_ids: List[str] = []
        for idx, cid in enumerate(commit_list):
            if cid not in seen:
                seen[cid] = idx
                ordered_ids.append(cid)

        results: List[Tuple[int, Dict[str, Any]]] = []
        max_workers = max(1, max_parallel or len(ordered_ids))
        # Use a stable index to restore order after parallel execution
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            future_map = {pool.submit(self.get_commit_info, cid): idx for idx, cid in enumerate(ordered_ids)}
            for fut in as_completed(future_map):
                idx = future_map[fut]
                try:
                    data = fut.result()
                except Exception as e:
                    data = {"error": f"{type(e).__name__}: {e}", "commit": {"commit_id": ordered_ids[idx]}}
                results.append((idx, data))

        # Sort by original sequence (oldest → newest)
        results.sort(key=lambda t: t[0])
        return [r for _, r in results]

    # ------------------ existing single-commit flow ------------------

    def get_commit_info(self, commit_id: str) -> Dict[str, Any]:
        # ---------- Commit ----------
        commit = self.git.get_commit(
            commit_id=commit_id,
            repository_id=self.repo_name,
            project=self.project,
        )

        result: Dict[str, Any] = {
            "commit": {
                "commit_id": commit.commit_id,
                "author": {
                    "name": commit.author.name,
                    "email": commit.author.email,
                    "date": commit.author.date.isoformat() if commit.author.date else None,
                },
                "committer": {
                    "name": commit.committer.name,
                    "email": commit.committer.email,
                    "date": commit.committer.date.isoformat() if commit.committer.date else None,
                },
                "comment": commit.comment,
                "comment_truncated": bool(getattr(commit, "comment_truncated", False)),
                "change_counts": {
                    "sdk_add": (commit.change_counts or {}).get("Add", 0),
                    "sdk_edit": (commit.change_counts or {}).get("Edit", 0),
                    "sdk_delete": (commit.change_counts or {}).get("Delete", 0),
                },
                "url": commit.url,
                "remote_url": getattr(commit, "remote_url", None),
                "parents": list(commit.parents or []),
                "tree_id": getattr(commit, "tree_id", None),
                "push": getattr(commit, "push", None).__dict__ if getattr(commit, "push", None) else None,
            },
            "pull_request": None,
            "work_items": [],
            "ci_builds": [],
            "error": None,
        }

        # ---------- Files / changes ----------
        changes = self.git.get_changes(
            commit_id=commit_id,
            repository_id=self.repo_name,
            project=self.project,
        )
        files: List[Dict[str, Any]] = []
        if changes and getattr(changes, "changes", None):
            add = edit = delete = 0
            for ch in changes.changes:
                change_type = getattr(ch, "change_type", None) or getattr(ch, "changeType", None)
                if change_type is None and isinstance(ch, dict):
                    change_type = ch.get("change_type") or ch.get("changeType")

                item = getattr(ch, "item", None) or (ch.get("item") if isinstance(ch, dict) else None)
                path = url = obj_id = None
                if item is not None:
                    if isinstance(item, dict):
                        path = item.get("path")
                        url = item.get("url")
                        obj_id = item.get("object_id") or item.get("objectId")
                    else:
                        path = getattr(item, "path", None)
                        url = getattr(item, "url", None)
                        obj_id = getattr(item, "object_id", getattr(item, "objectId", None))

                ct_lower = (str(change_type).lower() if change_type else "")
                if ct_lower == "add":
                    add += 1
                elif ct_lower in ("edit", "modify"):
                    edit += 1
                elif ct_lower == "delete":
                    delete += 1

                files.append(
                    {
                        "change_type": change_type,
                        "item": {"path": path, "url": url, "object_id": obj_id},
                    }
                )

            result["commit"]["changes"] = files
            result["commit"]["change_counts_computed"] = {"add": add, "edit": edit, "delete": delete}
        else:
            result["commit"]["changes"] = []

        # ---------- PR (query by commit, then fallback parse from message) ----------
        pr_id = self._find_pr_id_by_query(commit_id)
        if pr_id is None:
            pr_id = self._parse_pr_id_from_message(commit.comment or "")

        if pr_id is not None:
            pr = self.git.get_pull_request(
                pull_request_id=pr_id,
                project=self.project,
                repository_id=self.repo_name,
            )
            result["pull_request"] = {
                "pull_request_id": pr.pull_request_id,
                "title": pr.title,
                "description": pr.description,
                "status": pr.status,
                "created_by": {
                    "name": pr.created_by.display_name,
                    "email": pr.created_by.unique_name,
                    "id": pr.created_by.id,
                },
                "creation_date": pr.creation_date.isoformat() if pr.creation_date else None,
                "closed_date": pr.closed_date.isoformat() if pr.closed_date else None,
                "source_ref_name": pr.source_ref_name,
                "target_ref_name": pr.target_ref_name,
                "merge_status": pr.merge_status,
                "merge_id": getattr(pr, "merge_id", None),
                "url": pr.url,
                "reviewers": [
                    {
                        "name": rv.display_name,
                        "vote": rv.vote,
                        "is_required": getattr(rv, "is_required", False),
                    }
                    for rv in (pr.reviewers or [])
                ],
                "labels": [lb.name for lb in (pr.labels or [])],
                "work_items": [{"id": wi.id, "url": wi.url} for wi in (pr.work_item_refs or [])],
                "last_merge_source_commit": getattr(getattr(pr, "last_merge_source_commit", None), "commit_id", None),
                "last_merge_target_commit": getattr(getattr(pr, "last_merge_target_commit", None), "commit_id", None),
                "last_merge_commit": getattr(getattr(pr, "last_merge_commit", None), "commit_id", None),
            }
            # bubble up work items at top-level too
            result["work_items"] = result["pull_request"]["work_items"]

        # ---------- CI builds for this commit ----------
        try:
            builds = self.builds.get_builds(
                project=self.project,
                top=10,
                query_order="queueTimeDescending",
                **{"sourceVersion": commit_id}
            )
            result["ci_builds"] = [
                {
                    "id": b.id,
                    "build_number": b.build_number,
                    "result": getattr(b, "result", None),
                    "status": getattr(b, "status", None),
                    "queue_time": getattr(b, "queue_time", None).isoformat() if getattr(b, "queue_time", None) else None,
                    "start_time": getattr(b, "start_time", None).isoformat() if getattr(b, "start_time", None) else None,
                    "finish_time": getattr(b, "finish_time", None).isoformat() if getattr(b, "finish_time", None) else None,
                    "definition": getattr(getattr(b, "definition", None), "name", None),
                    "source_branch": getattr(b, "source_branch", None),
                    "source_version": getattr(b, "source_version", None),
                    "url": getattr(b, "url", None),
                    "web_url": getattr(getattr(b, "_links", {}), "web", {}).get("href", None)
                    if isinstance(getattr(b, "_links", {}), dict)
                    else None,
                }
                for b in (builds or [])
                if getattr(b, "source_version", commit_id) == commit_id
            ]
        except Exception:
            result["ci_builds"] = []

        return result

    # -------- helpers --------
    def _find_pr_id_by_query(self, commit_id: str) -> Optional[int]:
        pr_query = self.git.get_pull_request_query(
            queries={"queries": [{"items": [commit_id], "type": "commit"}]},
            repository_id=self.repo_name,
            project=self.project,
        )
        if getattr(pr_query, "results", None):
            first_list = pr_query.results[0] if len(pr_query.results) else None
            if first_list and len(first_list):
                first_pr_ref = first_list[0]
                pr_id = getattr(first_pr_ref, "pull_request_id", None)
                if pr_id is None and isinstance(first_pr_ref, dict):
                    pr_id = first_pr_ref.get("pull_request_id")
                return int(pr_id) if pr_id is not None else None
        return None

    @staticmethod
    def _parse_pr_id_from_message(message: str) -> Optional[int]:
        # e.g., "Merged PR 1813120: Title" or "Merge pull request #1813120"
        m = re.search(r"(?:PR|pull request)\s*#?\s*(\d+)", message, re.IGNORECASE)
        return int(m.group(1)) if m else None

    @staticmethod
    def _safe_attr(obj: Union[Dict[str, Any], Any, None], *keys: str) -> Optional[Any]:
        if obj is None:
            return None
        if isinstance(obj, dict):
            for k in keys:
                if k in obj:
                    return obj[k]
            return None
        for k in keys:
            if hasattr(obj, k):
                return getattr(obj, k)
        return None


# Convenience functions
def get_commit_and_pr_info(commit_id: str) -> Dict[str, Any]:
    api = AzureDevOpsCommitAPI()
    return api.get_commit_info(commit_id)


def get_range_commit_and_pr_info(
    begin_commit: str,
    end_commit: str,
    *,
    max_parallel: int = 8,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    One-shot helper: build client, fetch commit range, run in parallel.
    """
    api = AzureDevOpsCommitAPI()
    return api.get_commits_info_between(begin_commit, end_commit, max_parallel=max_parallel, limit=limit)


def get_commits_info_for_ids(
    commit_ids: Iterable[str],
    *,
    max_parallel: int = 8,
) -> List[Dict[str, Any]]:
    """Convenience wrapper to fetch commit info for an explicit list of commit IDs."""
    api = AzureDevOpsCommitAPI()
    return api.get_commits_info_for_ids(commit_ids, max_parallel=max_parallel)
