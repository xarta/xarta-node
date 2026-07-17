from __future__ import annotations

import os
import stat
import tempfile
from pathlib import Path


def test_pytest_temp_root_is_scoped_to_numeric_identity(tmp_path):
    configured_root = Path(os.environ["PYTEST_DEBUG_TEMPROOT"])

    assert configured_root == Path(tempfile.gettempdir()) / f"xarta-node-pytest-uid-{os.getuid()}"
    root_stat = os.lstat(configured_root)
    assert stat.S_ISDIR(root_stat.st_mode)
    assert not stat.S_ISLNK(root_stat.st_mode)
    assert root_stat.st_uid == os.getuid()
    assert root_stat.st_mode & 0o077 == 0
    assert tmp_path.is_relative_to(configured_root)
    assert tmp_path.parents[2] == configured_root
