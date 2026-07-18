"""Inode-bound ownership and file helpers for node-local repository paths."""

from __future__ import annotations

import logging
import os
import stat
import uuid
from pathlib import Path

log = logging.getLogger(__name__)

NODE_LOCAL_ROOT = Path("/xarta-node/.lone-wolf")
_DIR_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
_FILE_FLAGS = os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC


def _bounded_components(target: Path, root: Path) -> tuple[Path, Path, list[str]]:
    root_abs = Path(os.path.abspath(root))
    target_abs = Path(os.path.abspath(target))
    try:
        relative = target_abs.relative_to(root_abs)
    except ValueError as exc:
        raise ValueError(f"path is outside node-local root {root_abs}: {target_abs}") from exc
    components = list(relative.parts)
    if any(component in {"", ".", ".."} for component in components):
        raise ValueError(f"path contains an unsafe component: {target_abs}")
    return root_abs, target_abs, components


def _open_component(parent_fd: int, name: str, *, directory: bool) -> int:
    flags = _DIR_FLAGS if directory else _FILE_FLAGS
    return os.open(name, flags, dir_fd=parent_fd)


def _open_existing_chain(root: Path, components: list[str]) -> list[int]:
    fds = [os.open(root, _DIR_FLAGS)]
    try:
        for component in components:
            observed = os.stat(component, dir_fd=fds[-1], follow_symlinks=False)
            if stat.S_ISDIR(observed.st_mode):
                child_fd = _open_component(fds[-1], component, directory=True)
            elif stat.S_ISREG(observed.st_mode):
                child_fd = _open_component(fds[-1], component, directory=False)
            else:
                raise OSError(f"refusing symlink or special path component: {component}")
            opened = os.fstat(child_fd)
            if (opened.st_dev, opened.st_ino) != (observed.st_dev, observed.st_ino):
                os.close(child_fd)
                raise OSError(f"path component changed while opening: {component}")
            if stat.S_ISREG(opened.st_mode) and opened.st_nlink != 1:
                os.close(child_fd)
                raise OSError(f"refusing multiply-linked regular file: {component}")
            fds.append(child_fd)
        return fds
    except Exception:
        for fd in reversed(fds):
            os.close(fd)
        raise


def _open_parent(
    target: Path,
    *,
    root: Path,
    create: bool,
) -> tuple[Path, Path, str, list[int], os.stat_result]:
    root_abs, target_abs, components = _bounded_components(target, root)
    if not components:
        raise ValueError("the node-local root itself is not a file target")
    fds = [os.open(root_abs, _DIR_FLAGS)]
    owner = os.fstat(fds[0])
    try:
        for component in components[:-1]:
            try:
                child_fd = _open_component(fds[-1], component, directory=True)
            except FileNotFoundError:
                if not create:
                    raise
                os.mkdir(component, mode=0o755, dir_fd=fds[-1])
                child_fd = _open_component(fds[-1], component, directory=True)
                os.fchown(child_fd, owner.st_uid, owner.st_gid)
            fds.append(child_fd)
        return root_abs, target_abs, components[-1], fds, owner
    except Exception:
        for fd in reversed(fds):
            os.close(fd)
        raise


def normalize_node_local_ownership(target: Path, *, root: Path | None = None) -> bool:
    """Hand back an existing regular file/directory chain using bound file descriptors."""
    root_path = Path(root or NODE_LOCAL_ROOT)
    try:
        root_abs, _target_abs, components = _bounded_components(Path(target), root_path)
        fds = _open_existing_chain(root_abs, components)
    except (OSError, ValueError) as exc:
        log.warning("node-local ownership: refusing %s: %s", target, exc)
        return False
    try:
        owner = os.fstat(fds[0])
        for fd in reversed(fds):
            os.fchown(fd, owner.st_uid, owner.st_gid)
        return True
    except OSError as exc:
        log.warning("node-local ownership: could not normalize %s: %s", target, exc)
        return False
    finally:
        for fd in reversed(fds):
            os.close(fd)


def write_node_local_text_atomic(
    target: Path,
    text: str,
    *,
    root: Path | None = None,
    mode: int = 0o644,
) -> Path:
    """Atomically replace one regular file without following target or parent symlinks."""
    root_path = Path(root or NODE_LOCAL_ROOT)
    _root_abs, target_abs, name, fds, owner = _open_parent(
        Path(target), root=root_path, create=True
    )
    parent_fd = fds[-1]
    temp_name = f".{name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    temp_fd = -1
    try:
        temp_fd = os.open(
            temp_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
            0o600,
            dir_fd=parent_fd,
        )
        payload = text.encode("utf-8")
        written = 0
        while written < len(payload):
            written += os.write(temp_fd, payload[written:])
        os.fchmod(temp_fd, mode)
        os.fchown(temp_fd, owner.st_uid, owner.st_gid)
        os.fsync(temp_fd)
        os.replace(temp_name, name, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
        os.fsync(parent_fd)
        return target_abs
    finally:
        if temp_fd >= 0:
            os.close(temp_fd)
        try:
            os.unlink(temp_name, dir_fd=parent_fd)
        except FileNotFoundError:
            pass
        for fd in reversed(fds):
            os.close(fd)


def read_node_local_text(
    target: Path,
    *,
    root: Path | None = None,
    max_bytes: int = 8 * 1024 * 1024,
) -> str:
    """Read one bounded regular file without following target or parent symlinks."""
    root_path = Path(root or NODE_LOCAL_ROOT)
    _root_abs, _target_abs, name, fds, _owner = _open_parent(
        Path(target), root=root_path, create=False
    )
    file_fd = -1
    try:
        file_fd = _open_component(fds[-1], name, directory=False)
        observed = os.fstat(file_fd)
        if not stat.S_ISREG(observed.st_mode):
            raise OSError("node-local read target is not a regular file")
        if observed.st_size > max_bytes:
            raise OSError(f"node-local read target exceeds {max_bytes} bytes")
        chunks: list[bytes] = []
        remaining = max_bytes + 1
        while remaining > 0:
            chunk = os.read(file_fd, min(remaining, 64 * 1024))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        payload = b"".join(chunks)
        if len(payload) > max_bytes:
            raise OSError(f"node-local read target exceeds {max_bytes} bytes")
        return payload.decode("utf-8")
    finally:
        if file_fd >= 0:
            os.close(file_fd)
        for fd in reversed(fds):
            os.close(fd)


def unlink_node_local_file(target: Path, *, root: Path | None = None) -> bool:
    """Unlink one regular file or symlink through a bound parent directory."""
    root_path = Path(root or NODE_LOCAL_ROOT)
    _root_abs, _target_abs, name, fds, _owner = _open_parent(
        Path(target), root=root_path, create=False
    )
    try:
        try:
            observed = os.stat(name, dir_fd=fds[-1], follow_symlinks=False)
        except FileNotFoundError:
            return False
        if not (stat.S_ISREG(observed.st_mode) or stat.S_ISLNK(observed.st_mode)):
            raise OSError("node-local unlink target is not a regular file or symlink")
        os.unlink(name, dir_fd=fds[-1])
        return True
    finally:
        for fd in reversed(fds):
            os.close(fd)
