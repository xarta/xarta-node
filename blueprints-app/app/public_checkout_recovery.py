"""Bounded public-checkout recovery; caller must hold the provider claim fence."""

import hashlib
import os
import re
import subprocess
from pathlib import Path

BOOTSTRAP_PATHS = (
    "blueprints-app/app/routes_sync.py",
    "blueprints-app/app/public_checkout_recovery.py",
)
PROTECTED = (".xarta", ".lone-wolf")


def git(repo, *args, check=True):
    p = subprocess.run(
        ["git", "-c", "submodule.recurse=false", "-C", str(repo), *args],
        capture_output=True,
        timeout=60,
    )
    if check and p.returncode:
        raise RuntimeError(
            "public_recovery_git_failed:" + args[0] + ":" + p.stderr.decode(errors="replace")[:1000]
        )
    return p.stdout


def text(repo, *args):
    return git(repo, *args).decode().strip()


def validate_request(data):
    required = {
        "node_id",
        "expected_old_head",
        "expected_head",
        "expected_origin",
        "recovery_branch",
        "mode",
        "bootstrap",
    }
    if (
        set(data) != required
        or data["mode"] not in {"recover", "rollback"}
        or type(data["bootstrap"]) is not bool
    ):
        raise ValueError("Invalid recovery request fields")
    if any(
        not isinstance(data[k], str) or re.fullmatch(r"[0-9a-f]{40}", data[k]) is None
        for k in ("expected_old_head", "expected_head")
    ):
        raise ValueError("Recovery requires full commit hashes")
    if (
        not isinstance(data["recovery_branch"], str)
        or re.fullmatch(r"codex/recovery-[a-z0-9-]{1,80}", data["recovery_branch"]) is None
    ):
        raise ValueError("Invalid recovery branch")
    if (
        not isinstance(data["expected_origin"], str)
        or not data["expected_origin"]
        or len(data["expected_origin"]) > 500
        or any(c.isspace() for c in data["expected_origin"])
    ):
        raise ValueError("Invalid recovery origin")
    if (
        not isinstance(data["node_id"], str)
        or re.fullmatch(r"[A-Za-z0-9.-]{1,80}", data["node_id"]) is None
    ):
        raise ValueError("Invalid recovery identity")
    return data


def protected_snapshot(repo):
    result = {}
    for name in (".env", ".nodes.json", ".xarta", ".lone-wolf"):
        p = repo / name
        if p.exists() or p.is_symlink():
            s = p.lstat()
            result[name] = [
                s.st_dev,
                s.st_ino,
                s.st_uid,
                s.st_gid,
                s.st_mode,
                os.readlink(p) if p.is_symlink() else None,
            ]
            if p.is_file() and not p.is_symlink():
                result[name].append(hashlib.sha256(p.read_bytes()).hexdigest())
    return result


def execute(repo, request):
    """Run only inside the restart-owning process's continuous maintenance fence."""
    repo = Path(repo)
    r = validate_request(request)
    old, target, branch = r["expected_old_head"], r["expected_head"], r["recovery_branch"]
    if text(repo, "rev-parse", "HEAD") != old or text(repo, "branch", "--show-current") != "main":
        raise RuntimeError("public_recovery_old_head_or_branch_changed")
    if text(repo, "remote", "get-url", "origin") != r["expected_origin"]:
        raise RuntimeError("public_recovery_origin_changed")
    if (
        text(repo, "rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}")
        != "origin/main"
    ):
        raise RuntimeError("public_recovery_upstream_changed")
    if text(repo, "worktree", "list", "--porcelain").count("worktree ") != 1:
        raise RuntimeError("public_recovery_multiple_worktrees")
    protected = protected_snapshot(repo)
    git(repo, "fetch", "--no-tags", "origin", "main")
    if r["mode"] == "recover" and text(repo, "rev-parse", "origin/main") != target:
        raise RuntimeError("public_recovery_remote_advanced")
    if r["mode"] == "rollback" and text(repo, "rev-parse", "refs/heads/" + branch) != target:
        raise RuntimeError("public_recovery_retained_tip_changed")
    if r["bootstrap"]:
        if r["mode"] != "recover":
            raise RuntimeError("public_recovery_invalid_bootstrap")
        # The old process cannot expose a new API without an initial guarded
        # protocol-only restart. Admit only exact published bootstrap bytes.
        for path in BOOTSTRAP_PATHS:
            if (repo / path).read_bytes() != git(repo, "show", target + ":" + path):
                raise RuntimeError("public_recovery_bootstrap_bytes_changed")
        statuses = git(repo, "status", "--porcelain=v1", "-z").decode().split("\0")
        if sorted(row[3:] for row in statuses if row) != sorted(BOOTSTRAP_PATHS):
            raise RuntimeError("public_recovery_unclassified_dirty_paths")
        git(repo, "restore", "--source=HEAD", "--worktree", "--", BOOTSTRAP_PATHS[0])
        (repo / BOOTSTRAP_PATHS[1]).unlink()
    if git(repo, "status", "--porcelain=v1", "-z"):
        raise RuntimeError("public_recovery_dirty")
    paths = git(repo, "diff", "--name-only", "-z", old, target).decode().split("\0")
    for path in filter(None, paths):
        if (repo / path).exists() or (repo / path).is_symlink():
            if not git(repo, "ls-tree", "--name-only", old, "--", path).strip():
                raise RuntimeError("public_recovery_ignored_or_untracked_collision")
        parts = Path(path).parts
        if any(part in PROTECTED for part in parts):
            raise RuntimeError("public_recovery_protected_path")
        for parent in list((repo / path).parents):
            if parent == repo:
                break
            if parent.is_symlink() or (parent / ".git").exists():
                raise RuntimeError("public_recovery_nested_repository_or_symlink")
    if r["mode"] == "recover":
        exists = (
            subprocess.run(
                ["git", "-C", str(repo), "show-ref", "--verify", "--quiet", "refs/heads/" + branch]
            ).returncode
            == 0
        )
        if exists:
            raise RuntimeError("public_recovery_branch_exists")
        git(repo, "branch", "-m", "main", branch)
        if text(repo, "rev-parse", "refs/heads/" + branch) != old:
            raise RuntimeError("public_recovery_retained_tip_changed")
        git(repo, "switch", "--no-overwrite-ignore", "--create", "main", target)
        git(repo, "branch", "--set-upstream-to=origin/main", "main")
        if text(repo, "rev-list", "--left-right", "--count", "HEAD...origin/main").split() != [
            "0",
            "0",
        ]:
            raise RuntimeError("public_recovery_alignment_failed")
    else:
        git(repo, "switch", "--no-overwrite-ignore", branch)
    if text(repo, "rev-parse", "HEAD") != target or git(repo, "status", "--porcelain=v1", "-z"):
        raise RuntimeError("public_recovery_post_checkout_failed")
    if protected_snapshot(repo) != protected:
        raise RuntimeError("public_recovery_protected_state_changed")
    return {
        "old_head": old,
        "target_head": target,
        "recovery_branch": branch,
        "mode": r["mode"],
        "protected": protected,
        "changed_paths": len(list(filter(None, paths))),
    }
