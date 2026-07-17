from __future__ import annotations

import os
import stat
import sys
import tempfile
from pathlib import Path

import pytest

_TEST_PROCESS_UID = os.getuid()
_APP_ROOT = Path(__file__).resolve().parents[1] / "blueprints-app"

if str(_APP_ROOT) not in sys.path:
    sys.path.insert(0, str(_APP_ROOT))


def _identity_scoped_pytest_temp_root() -> Path:
    root = Path(tempfile.gettempdir()) / f"xarta-node-pytest-uid-{_TEST_PROCESS_UID}"
    root.mkdir(mode=0o700, parents=False, exist_ok=True)
    root_stat = os.lstat(root)
    if stat.S_ISLNK(root_stat.st_mode):
        raise RuntimeError(f"pytest temp root must not be a symlink: {root}")
    if not stat.S_ISDIR(root_stat.st_mode):
        raise RuntimeError(f"pytest temp root must be a directory: {root}")
    if root_stat.st_uid != _TEST_PROCESS_UID:
        raise RuntimeError(
            f"pytest temp root {root} is owned by uid {root_stat.st_uid}, "
            f"not test-process uid {_TEST_PROCESS_UID}"
        )
    if root_stat.st_mode & 0o077:
        raise RuntimeError(f"pytest temp root must be private (0700): {root}")
    return root


def _private_directory_problem(path: Path) -> str | None:
    path_stat = os.lstat(path)
    if stat.S_ISLNK(path_stat.st_mode):
        return "is a symlink"
    if not stat.S_ISDIR(path_stat.st_mode):
        return "is not a directory"
    if path_stat.st_uid != _TEST_PROCESS_UID:
        return f"is owned by uid {path_stat.st_uid}, not {_TEST_PROCESS_UID}"
    if path_stat.st_mode & 0o077:
        return "is not private (0700)"
    return None


_PYTEST_TEMP_ROOT = _identity_scoped_pytest_temp_root()
os.environ["PYTEST_DEBUG_TEMPROOT"] = str(_PYTEST_TEMP_ROOT)


@pytest.fixture(autouse=True)
def _guard_pytest_temp_root_ownership(tmp_path_factory):
    """Name the test that hands either pytest temp-root layer to another uid."""
    pytest_user_root = tmp_path_factory.getbasetemp().parent
    yield
    for path in (_PYTEST_TEMP_ROOT, pytest_user_root):
        if problem := _private_directory_problem(path):
            pytest.fail(f"test changed pytest temp directory {path}: {problem}", pytrace=False)
